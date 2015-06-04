package utils

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/endophage/gotuf/data"
)

func Download(url url.URL) (*http.Response, error) {
	return http.Get(url.String())
}

func Upload(url string, body io.Reader) (*http.Response, error) {
	return http.Post(url, "application/json", body)
}

func ValidateTarget(r io.Reader, m *data.FileMeta) error {
	h := sha256.New()
	length, err := io.Copy(h, r)
	if err != nil {
		return err
	}
	if length != m.Length {
		return fmt.Errorf("Size of downloaded target did not match targets entry.\nExpected: %s\nReceived: %s\n", m.Length, length)
	}
	hashDigest := h.Sum(nil)
	if bytes.Compare(m.Hashes["sha256"], hashDigest[:]) != 0 {
		return fmt.Errorf("Hash of downloaded target did not match targets entry.\nExpected: %x\nReceived: %x\n", m.Hashes["sha256"], hashDigest)
	}
	return nil
}