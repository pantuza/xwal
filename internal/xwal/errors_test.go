package xwal

import "testing"

func TestXWALErrorRespectsGolangErrorInterface(t *testing.T) {
	var _ error = NewXWALError(XWALBadConfiguration, "Fake error", map[string]string{})
}
