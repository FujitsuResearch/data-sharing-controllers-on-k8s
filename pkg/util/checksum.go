// Copyright (c) 2022 Fujitsu Limited

package util

import (
	"crypto/sha256"
	"io"
	"os"
)

func CalculateChecksum(filePath string) ([]byte, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := sha256.New()

	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}
