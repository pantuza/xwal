package awss3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsTypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"github.com/pantuza/xwal/pkg/types"
	"github.com/pantuza/xwal/protobuf/xwalpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protodelim"
)

const (
	// Segments Object name format of the WAL (decimal index, no fixed width).
	S3WALSegmentObjectFormat = "wal_%d"
	S3GarbageObjectExtension = ".garbage"
	S3CheckpointObjectSuffix = ".checkpoint"
)

type AWSS3WALBackend struct {
	cfg *AWSS3Config

	ctx               context.Context
	cancel            context.CancelFunc
	waitGroup         sync.WaitGroup
	cleanLogsInterval *time.Ticker

	firstSegmentIndex uint64
	lastSegmentIndex  uint64

	currentSegmentObjectName string
	lastLSN                  uint64
	logger                   *zap.Logger

	// AWS S3 client reference
	s3Client *s3.Client
}

func NewAWSS3WALBackend(cfg *AWSS3Config) *AWSS3WALBackend {
	if cfg.BucketName == "" {
		cfg.BucketName = DefaultBucketName
	}
	if cfg.Region == "" {
		cfg.Region = DefaultRegion
	}
	if cfg.SegmentsObjectSizeMB == 0 {
		cfg.SegmentsObjectSizeMB = DefaultSegmentsObjectSizeMB
	}
	if cfg.SegmentsBucketSizeGB == 0 {
		cfg.SegmentsBucketSizeGB = DefaultSegmentsBucketSizeGB
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.CleanLogsInterval == 0 {
		cfg.CleanLogsInterval = 1 * time.Minute // Default interval to clean garbage logs
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &AWSS3WALBackend{
		cfg:               cfg,
		ctx:               ctx,
		cancel:            cancel,
		waitGroup:         sync.WaitGroup{},
		cleanLogsInterval: time.NewTicker(cfg.CleanLogsInterval),
		firstSegmentIndex: 0,
		lastSegmentIndex:  0,
		lastLSN:           0,
		logger:            cfg.Logger,
	}
}

func (s *AWSS3WALBackend) Type() types.WALBackendType {
	return types.AWSS3WALBackend
}

func (wal *AWSS3WALBackend) Open() error {
	if err := wal.cfg.Validate(); err != nil {
		return err
	}
	if err := awsAuthenticate(wal.cfg); err != nil {
		return fmt.Errorf("failed to authenticate with AWS: %w", err)
	}

	wal.s3Client = s3.NewFromConfig(*wal.cfg.AWSConfig)

	if err := wal.createWalBucket(); err != nil {
		return fmt.Errorf("failed to open WAL bucket: %w", err)
	}

	if err := wal.extractSegmentsIndexesFromObjects(); err != nil {
		return err
	}
	wal.currentSegmentObjectName = fmt.Sprintf(S3WALSegmentObjectFormat, wal.lastSegmentIndex)
	if err := wal.getLastLogSequencyNumber(); err != nil {
		return err
	}

	return nil
}

// createWalBucket creates the bucket if it does not exist.
func (wal *AWSS3WALBackend) createWalBucket() error {
	// Create the bucket if it does not exists
	if _, err := wal.s3Client.HeadBucket(wal.ctx, &s3.HeadBucketInput{Bucket: &wal.cfg.BucketName}); err != nil {

		wal.logger.Info("Bucket does not exists. Creating it.", zap.String("bucket", wal.cfg.BucketName))
		_, err := wal.s3Client.CreateBucket(wal.ctx, &s3.CreateBucketInput{
			Bucket: &wal.cfg.BucketName,
			CreateBucketConfiguration: &awsTypes.CreateBucketConfiguration{
				LocationConstraint: awsTypes.BucketLocationConstraint(wal.cfg.Region),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		// Otherwise, read the bucket
	} else {
		wal.logger.Info("Bucket already exists", zap.String("bucket", wal.cfg.BucketName))
	}

	return nil
}

func (wal *AWSS3WALBackend) Write(entries []*xwalpb.WALEntry) error {
	// Initialize a bytes buffer to accumulate the serialized data
	var buffer bytes.Buffer

	for _, entry := range entries {
		if _, err := protodelim.MarshalTo(&buffer, entry); err != nil {
			return fmt.Errorf("marshaling entry to segment buffer for s3: %w", err)
		}
	}

	if err := wal.rotateSegmentsIfNeeded(); err != nil {
		return err
	}

	if err := wal.appendObject(wal.ctx, &buffer); err != nil {
		return err
	}

	return nil
}

// rotateSegmentsIfNeeded checks if the current segment object size is greater than the configured size
// and rotates to the next segment if needed. It also updates the currentSegmentObjectName with the new segment name.
func (wal *AWSS3WALBackend) rotateSegmentsIfNeeded() error {
	// TODO: Evaluate if we can prevent calling s3 api for every call to this method
	currentSegmentSize, err := wal.getCurrentObjectSize(wal.ctx)
	if err != nil {
		return err
	}

	if currentSegmentSize >= int64(wal.cfg.SegmentsObjectSizeMB)*1024*1024 {
		next, err := nextSegmentIndex(wal.lastSegmentIndex)
		if err != nil {
			return err
		}
		wal.lastSegmentIndex = next
		wal.currentSegmentObjectName = fmt.Sprintf(S3WALSegmentObjectFormat, wal.lastSegmentIndex)
	}

	return nil
}

func (wal *AWSS3WALBackend) getCurrentObjectSize(ctx context.Context) (int64, error) {
	input := &s3.HeadObjectInput{
		Bucket: &wal.cfg.BucketName,
		Key:    &wal.currentSegmentObjectName,
	}

	// TODO: Reduce the timeout for these calls. Maybe make ctx take this control.
	// The issue is: if for some reason xWAL isn't able to communicate with s3, it should
	// fail fast instead of hang for so long.
	resp, err := wal.s3Client.HeadObject(ctx, input)
	if err != nil {
		if isS3NotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("get object size for %q in bucket %q: %w", wal.currentSegmentObjectName, wal.cfg.BucketName, err)
	}

	return aws.ToInt64(resp.ContentLength), nil
}

func (wal *AWSS3WALBackend) appendObject(ctx context.Context, data *bytes.Buffer) error {
	current, err := wal.readObject(ctx, wal.currentSegmentObjectName)
	if err != nil {
		return err
	}
	current = append(current, data.Bytes()...)

	input := &s3.PutObjectInput{
		Bucket: &wal.cfg.BucketName,
		Key:    &wal.currentSegmentObjectName,
		Body:   bytes.NewReader(current),
	}

	if _, err := wal.s3Client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("put object %q in bucket %q: %w", wal.currentSegmentObjectName, wal.cfg.BucketName, err)
	}

	return nil
}

func (wal *AWSS3WALBackend) Replay(channel chan *xwalpb.WALEntry, backwards bool) error {
	segments, err := wal.getSegmentsObjectNamesFromRange(wal.firstSegmentIndex, wal.lastSegmentIndex)
	if err != nil {
		return fmt.Errorf("get segment objects from range: %w", err)
	}
	if backwards {
		slices.Reverse(segments)
	}
	return wal.replaySegments(segments, channel, backwards)
}

func (wal *AWSS3WALBackend) ReplayFromRange(channel chan *xwalpb.WALEntry, backwards bool, start, end uint64) error {
	if start > end || start < wal.firstSegmentIndex || end > wal.lastSegmentIndex {
		return fmt.Errorf("invalid range: start=%d end=%d (first segment index=%d last=%d)", start, end, wal.firstSegmentIndex, wal.lastSegmentIndex)
	}

	segments, err := wal.getSegmentsObjectNamesFromRange(start, end)
	if err != nil {
		return fmt.Errorf("get segment objects from range: %w", err)
	}
	if backwards {
		slices.Reverse(segments)
	}
	return wal.replaySegments(segments, channel, backwards)
}

func (wal *AWSS3WALBackend) CreateCheckpoint() (uint64, error) {
	checkpointIndex := wal.lastSegmentIndex
	checkpointObjectName := fmt.Sprintf(S3WALSegmentObjectFormat, checkpointIndex) + S3CheckpointObjectSuffix
	if err := wal.renameObject(fmt.Sprintf(S3WALSegmentObjectFormat, checkpointIndex), checkpointObjectName); err != nil {
		return 0, fmt.Errorf("rename segment object to checkpoint: %w", err)
	}

	next, err := nextSegmentIndex(wal.lastSegmentIndex)
	if err != nil {
		return 0, err
	}
	wal.lastSegmentIndex = next
	wal.currentSegmentObjectName = fmt.Sprintf(S3WALSegmentObjectFormat, wal.lastSegmentIndex)
	return checkpointIndex, nil
}

func (wal *AWSS3WALBackend) ReplayFromCheckpoint(channel chan *xwalpb.WALEntry, checkpoint uint64, backwards bool) error {
	if checkpoint > wal.lastSegmentIndex {
		return fmt.Errorf("invalid checkpoint %d (last segment index=%d)", checkpoint, wal.lastSegmentIndex)
	}
	segments, err := wal.getSegmentsObjectNamesFromOrToCheckpoint(checkpoint, wal.lastSegmentIndex, checkpoint)
	if err != nil {
		return fmt.Errorf("get segment objects from checkpoint: %w", err)
	}
	if backwards {
		slices.Reverse(segments)
	}
	return wal.replaySegments(segments, channel, backwards)
}

func (wal *AWSS3WALBackend) ReplayToCheckpoint(channel chan *xwalpb.WALEntry, checkpoint uint64, backwards bool) error {
	if checkpoint > wal.lastSegmentIndex {
		return fmt.Errorf("invalid checkpoint %d (last segment index=%d)", checkpoint, wal.lastSegmentIndex)
	}
	segments, err := wal.getSegmentsObjectNamesFromOrToCheckpoint(wal.firstSegmentIndex, checkpoint, checkpoint)
	if err != nil {
		return fmt.Errorf("get segment objects to checkpoint: %w", err)
	}
	if backwards {
		slices.Reverse(segments)
	}
	return wal.replaySegments(segments, channel, backwards)
}

func (wal *AWSS3WALBackend) Close() error {
	wal.cancel()
	wal.cleanLogsInterval.Stop()
	return nil
}

func (wal *AWSS3WALBackend) LastIndex() uint64 {
	return wal.lastLSN
}

func (wal *AWSS3WALBackend) IncLastIndex() {
	wal.lastLSN++
}

// CurrentSegmentIndex implements types.WALSegmentIndexer for observability.
func (wal *AWSS3WALBackend) CurrentSegmentIndex() uint64 {
	return wal.lastSegmentIndex
}

func (wal *AWSS3WALBackend) extractSegmentsIndexesFromObjects() error {
	objects, err := wal.listWALObjects()
	if err != nil {
		return err
	}

	wal.firstSegmentIndex = math.MaxUint64
	wal.lastSegmentIndex = 0
	for _, obj := range objects {
		if obj.Key == nil {
			continue
		}
		idx, err := wal.extractSegmentIndex(*obj.Key)
		if err == ErrInvalidSegmentIndex {
			if idx == math.MaxUint64 {
				return fmt.Errorf("garbage segment index overflow for object %q", *obj.Key)
			}
			wal.firstSegmentIndex = idx + 1
			continue
		}
		if idx < wal.firstSegmentIndex {
			wal.firstSegmentIndex = idx
		}
		if idx > wal.lastSegmentIndex {
			wal.lastSegmentIndex = idx
		}
	}
	if wal.firstSegmentIndex == math.MaxUint64 {
		wal.firstSegmentIndex = 0
		wal.lastSegmentIndex = 0
	}
	return nil
}

// ErrSegmentIndexExhausted is returned when the segment counter would exceed the representable range.
var ErrSegmentIndexExhausted = errors.New("wal segment index exhausted")

var ErrInvalidSegmentIndex = errors.New("invalid segment index: object name indicates garbage segment")

func (wal *AWSS3WALBackend) extractSegmentIndex(objectName string) (uint64, error) {
	var index uint64
	if _, err := fmt.Sscanf(objectName, S3WALSegmentObjectFormat, &index); err != nil {
		return index, ErrInvalidSegmentIndex
	}
	if len(objectName) >= len(S3GarbageObjectExtension) && objectName[len(objectName)-len(S3GarbageObjectExtension):] == S3GarbageObjectExtension {
		return index, ErrInvalidSegmentIndex
	}
	return index, nil
}

func (wal *AWSS3WALBackend) listWALObjects() ([]awsTypes.Object, error) {
	objects := make([]awsTypes.Object, 0)
	var token *string
	for {
		out, err := wal.s3Client.ListObjectsV2(wal.ctx, &s3.ListObjectsV2Input{
			Bucket:            &wal.cfg.BucketName,
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list wal objects in bucket %q: %w", wal.cfg.BucketName, err)
		}
		objects = append(objects, out.Contents...)
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}
	return objects, nil
}

func (wal *AWSS3WALBackend) readObject(ctx context.Context, objectName string) ([]byte, error) {
	out, err := wal.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &wal.cfg.BucketName,
		Key:    &objectName,
	})
	if err != nil {
		if isS3NotFound(err) {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("read object %q from bucket %q: %w", objectName, wal.cfg.BucketName, err)
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read object body %q from bucket %q: %w", objectName, wal.cfg.BucketName, err)
	}
	return data, nil
}

func isS3NotFound(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NotFound" || code == "NoSuchKey" || code == "404"
	}
	return false
}

func (wal *AWSS3WALBackend) getSegmentsObjectNamesFromRange(start, end uint64) ([]string, error) {
	objects, err := wal.listWALObjects()
	if err != nil {
		return nil, err
	}
	indexToObject := make(map[uint64]string, len(objects))
	for _, obj := range objects {
		if obj.Key == nil || *obj.Key == "" {
			continue
		}
		name := *obj.Key
		if len(name) >= len(S3GarbageObjectExtension) && name[len(name)-len(S3GarbageObjectExtension):] == S3GarbageObjectExtension {
			continue
		}
		var idx uint64
		if _, err := fmt.Sscanf(name, S3WALSegmentObjectFormat, &idx); err == nil {
			indexToObject[idx] = name
		}
	}

	files := make([]string, 0, end-start+1)
	for i := start; i <= end; i++ {
		objectName, ok := indexToObject[i]
		if !ok {
			return nil, fmt.Errorf("no segment object found for index %d", i)
		}
		files = append(files, objectName)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no segment objects found in range %d to %d", start, end)
	}
	return files, nil
}

func (wal *AWSS3WALBackend) getSegmentsObjectNamesFromOrToCheckpoint(start, end, checkpoint uint64) ([]string, error) {
	segments, err := wal.getSegmentsObjectNamesFromRange(start, end)
	if err != nil {
		return nil, err
	}
	expectedCheckpointName := fmt.Sprintf(S3WALSegmentObjectFormat, checkpoint) + S3CheckpointObjectSuffix
	for i, seg := range segments {
		var idx uint64
		if _, err := fmt.Sscanf(seg, S3WALSegmentObjectFormat, &idx); err == nil && idx == checkpoint {
			segments[i] = expectedCheckpointName
		}
	}
	return segments, nil
}

func (wal *AWSS3WALBackend) replaySegments(segments []string, channel chan *xwalpb.WALEntry, backwards bool) error {
	for _, segment := range segments {
		data, err := wal.readObject(wal.ctx, segment)
		if err != nil {
			return fmt.Errorf("read segment object %q for replay: %w", segment, err)
		}
		readEntries, err := wal.readEntriesFromBytes(data)
		if err != nil {
			return fmt.Errorf("read entries from segment object %q for replay: %w", segment, err)
		}
		if backwards {
			slices.Reverse(readEntries)
		}
		for _, entry := range readEntries {
			if entry == nil {
				continue
			}
			chksum, err := entry.Checksum()
			if err != nil {
				wal.logger.Error("Fail to validate entry checksum. Skipping replay entry", zap.String("segmentObject", segment), zap.Uint64("LSN", entry.LSN))
			}
			if entry.CRC != chksum {
				wal.logger.Warn("Entry checksum does not match. Skipping replay entry", zap.String("segmentObject", segment), zap.Uint64("LSN", entry.LSN), zap.Uint32("EntryCRC", entry.CRC), zap.Uint32("CalculatedCRC", chksum))
				continue
			}
			channel <- entry
		}
		if err := wal.setSegmentObjectAsGarbage(segment); err != nil {
			return err
		}
	}
	next, err := nextSegmentIndex(wal.lastSegmentIndex)
	if err != nil {
		return err
	}
	wal.lastSegmentIndex = next
	wal.currentSegmentObjectName = fmt.Sprintf(S3WALSegmentObjectFormat, wal.lastSegmentIndex)
	wal.firstSegmentIndex = wal.lastSegmentIndex
	return nil
}

func (wal *AWSS3WALBackend) readEntriesFromBytes(data []byte) ([]*xwalpb.WALEntry, error) {
	entries := make([]*xwalpb.WALEntry, 0)
	reader := bytes.NewReader(data)
	for reader.Len() > 0 {
		entry := &xwalpb.WALEntry{}
		if err := protodelim.UnmarshalFrom(reader, entry); err != nil {
			return nil, fmt.Errorf("unmarshal entry from segment object: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (wal *AWSS3WALBackend) setSegmentObjectAsGarbage(objectName string) error {
	return wal.renameObject(objectName, objectName+S3GarbageObjectExtension)
}

func (wal *AWSS3WALBackend) renameObject(from, to string) error {
	copySource := wal.cfg.BucketName + "/" + from
	if _, err := wal.s3Client.CopyObject(wal.ctx, &s3.CopyObjectInput{
		Bucket:     &wal.cfg.BucketName,
		CopySource: &copySource,
		Key:        &to,
	}); err != nil {
		return fmt.Errorf("copy segment object from %q to %q: %w", from, to, err)
	}
	if _, err := wal.s3Client.DeleteObject(wal.ctx, &s3.DeleteObjectInput{
		Bucket: &wal.cfg.BucketName,
		Key:    &from,
	}); err != nil {
		return fmt.Errorf("delete source segment object %q after rename: %w", from, err)
	}
	return nil
}

func (wal *AWSS3WALBackend) getLastLogSequencyNumber() error {
	data, err := wal.readObject(wal.ctx, wal.currentSegmentObjectName)
	if err != nil {
		return err
	}
	entries, err := wal.readEntriesFromBytes(data)
	if err != nil {
		return err
	}
	wal.lastLSN = 0
	if len(entries) > 0 {
		wal.lastLSN = entries[len(entries)-1].GetLSN()
	}
	return nil
}

func nextSegmentIndex(cur uint64) (uint64, error) {
	if cur == math.MaxUint64 {
		return 0, ErrSegmentIndexExhausted
	}
	return cur + 1, nil
}
