// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package util

import "hash/fnv"

// GetPartitionIndex returns the index of the partition for the given key
func GetPartitionIndex(key []byte, partitions int) (int, error) {
	h := fnv.New32a()
	if _, err := h.Write(key); err != nil {
		return 0, err
	}
	return int(h.Sum32() % uint32(partitions)), nil
}
