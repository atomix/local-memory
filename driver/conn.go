// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"fmt"
	counterv1api "github.com/atomix/runtime/api/atomix/runtime/counter/v1"
	countermapv1api "github.com/atomix/runtime/api/atomix/runtime/countermap/v1"
	electionv1api "github.com/atomix/runtime/api/atomix/runtime/election/v1"
	indexedmapv1api "github.com/atomix/runtime/api/atomix/runtime/indexedmap/v1"
	lockv1api "github.com/atomix/runtime/api/atomix/runtime/lock/v1"
	mapv1api "github.com/atomix/runtime/api/atomix/runtime/map/v1"
	multimapv1api "github.com/atomix/runtime/api/atomix/runtime/multimap/v1"
	setv1api "github.com/atomix/runtime/api/atomix/runtime/set/v1"
	valuev1api "github.com/atomix/runtime/api/atomix/runtime/value/v1"
	counterv1 "github.com/atomix/runtime/primitives/pkg/counter/v1"
	countermapv1 "github.com/atomix/runtime/primitives/pkg/countermap/v1"
	electionv1 "github.com/atomix/runtime/primitives/pkg/election/v1"
	indexedmapv1 "github.com/atomix/runtime/primitives/pkg/indexedmap/v1"
	lockv1 "github.com/atomix/runtime/primitives/pkg/lock/v1"
	mapv1 "github.com/atomix/runtime/primitives/pkg/map/v1"
	multimapv1 "github.com/atomix/runtime/primitives/pkg/multimap/v1"
	setv1 "github.com/atomix/runtime/primitives/pkg/set/v1"
	valuev1 "github.com/atomix/runtime/primitives/pkg/value/v1"
	"github.com/atomix/runtime/sdk/pkg/network"
	"github.com/atomix/runtime/sdk/pkg/protocol"
	"github.com/atomix/runtime/sdk/pkg/protocol/client"
	"github.com/atomix/runtime/sdk/pkg/protocol/node"
	"github.com/atomix/runtime/sdk/pkg/runtime"
	"sync"
)

func newConn(network network.Network) *podMemoryConn {
	return &podMemoryConn{
		ProtocolClient: client.NewClient(network),
		network:        network,
	}
}

type podMemoryConn struct {
	*client.ProtocolClient
	network network.Network
	node    *node.Node
	mu      sync.Mutex
}

func (c *podMemoryConn) Connect(ctx context.Context, spec runtime.ConnSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.node != nil {
		return nil
	}

	c.node = newNode(c.network,
		node.WithHost(fmt.Sprintf("%s.%s", spec.Namespace, spec.Name)),
		node.WithPort(8080))
	if err := c.node.Start(); err != nil {
		return err
	}

	config := protocol.ProtocolConfig{
		Partitions: []protocol.PartitionConfig{
			{
				PartitionID: 1,
				Leader:      fmt.Sprintf("%s.%s:8080", spec.Namespace, spec.Name),
			},
		},
	}
	return c.ProtocolClient.Connect(ctx, config)
}

func (c *podMemoryConn) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.node == nil {
		return nil
	}
	if err := c.ProtocolClient.Close(ctx); err != nil {
		return err
	}
	if err := c.node.Stop(); err != nil {
		return err
	}
	return nil
}

func (c *podMemoryConn) NewCounter(spec runtime.PrimitiveSpec) (counterv1api.CounterServer, error) {
	return counterv1.NewCounterProxy(c.Protocol, spec)
}

func (c *podMemoryConn) NewCounterMap(spec runtime.PrimitiveSpec) (countermapv1api.CounterMapServer, error) {
	return countermapv1.NewCounterMapProxy(c.Protocol, spec)
}

func (c *podMemoryConn) NewLeaderElection(spec runtime.PrimitiveSpec) (electionv1api.LeaderElectionServer, error) {
	return electionv1.NewLeaderElectionProxy(c.Protocol, spec)
}

func (c *podMemoryConn) NewIndexedMap(spec runtime.PrimitiveSpec) (indexedmapv1api.IndexedMapServer, error) {
	return indexedmapv1.NewIndexedMapProxy(c.Protocol, spec)
}

func (c *podMemoryConn) NewLock(spec runtime.PrimitiveSpec) (lockv1api.LockServer, error) {
	return lockv1.NewLockProxy(c.Protocol, spec)
}

func (c *podMemoryConn) NewMap(spec runtime.PrimitiveSpec) (mapv1api.MapServer, error) {
	return mapv1.NewMapProxy(c.Protocol, spec)
}

func (c *podMemoryConn) NewMultiMap(spec runtime.PrimitiveSpec) (multimapv1api.MultiMapServer, error) {
	return multimapv1.NewMultiMapProxy(c.Protocol, spec)
}

func (c *podMemoryConn) NewSet(spec runtime.PrimitiveSpec) (setv1api.SetServer, error) {
	return setv1.NewSetProxy(c.Protocol, spec)
}

func (c *podMemoryConn) NewValue(spec runtime.PrimitiveSpec) (valuev1api.ValueServer, error) {
	return valuev1.NewValueProxy(c.Protocol, spec)
}
