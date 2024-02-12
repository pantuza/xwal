package xwal

import (
	"context"
	"sync"
	"time"
)

type XWAL struct {
	cfg    XWALConfig
	lock   sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	timer  time.Ticker
}

func NewXWAL(cfg XWALConfig) (*XWAL, error) {
	var xwal XWAL
	xwal.cfg = cfg
	return &xwal, nil
}

func (wal *XWAL) configure() error {
	return nil
}
