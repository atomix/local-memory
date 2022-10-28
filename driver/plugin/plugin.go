// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/atomix/pod-memory-storage/driver"
	"github.com/atomix/runtime/sdk/pkg/network"
)

var Plugin = driver.New(network.NewLocalNetwork())
