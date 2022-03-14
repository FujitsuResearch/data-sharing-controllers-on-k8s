// Copyright (c) 2022 Fujitsu Limited

package util

import (
	"fmt"
	"strings"
)

const (
	keyDelimiter = "/"
)

func ConcatenateNamespaceAndName(namespace string, name string) string {
	return fmt.Sprintf("%s%s%s", namespace, keyDelimiter, name)
}

func SplitIntoNamespaceAndName(
	namespaceAndName string) (string, string, error) {
	keys := strings.Split(namespaceAndName, keyDelimiter)
	if len(keys) != 2 {
		return "", "", fmt.Errorf(
			"Could not correctly split a string %q into namespace and name",
			namespaceAndName)
	}

	return keys[0], keys[1], nil
}
