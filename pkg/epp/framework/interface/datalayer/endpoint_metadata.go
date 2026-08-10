/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datalayer

import (
	"fmt"
	"maps"

	"k8s.io/apimachinery/pkg/types"
)

// ID uniquely identifies an endpoint. It aliases types.NamespacedName so the identity
// can later move off the Kubernetes-specific type without churning datastore keys.
// Every discovery source must set it uniquely.
type ID = types.NamespacedName

// DataParallelTarget identifies a scheduler behind a shared inference frontend.
type DataParallelTarget struct {
	// GlobalRank identifies the scheduler across the distributed engine.
	GlobalRank int
	// Selector is the value understood by the shared HTTP frontend.
	Selector int
}

// String returns a readable data parallel target.
func (target *DataParallelTarget) String() string {
	if target == nil {
		return "<nil>"
	}
	return fmt.Sprintf("{GlobalRank:%d Selector:%d}", target.GlobalRank, target.Selector)
}

// EndpointMetadata describes an inference endpoint.
type EndpointMetadata struct {
	// ID is the endpoint's unique identity, and the datastore key.
	ID      ID
	Name    string
	Address string
	// KVEventHost is the host publishing this endpoint's KV events. It defaults
	// to Address for physical endpoints.
	KVEventHost string
	// NodeAddress is the node IP hosting this pod (pod.Status.HostIP).
	// Empty for non-Kubernetes discovery sources (e.g. file discovery).
	NodeAddress string
	Port        string
	MetricsHost string
	Labels      map[string]string
	// RankIndex is this endpoint's position in the pool's TargetPorts,
	// identifying the pod-local rank in multi-port deployments.
	RankIndex int
	// DataParallelTarget is set when the endpoint represents a scheduler behind
	// a shared inference frontend.
	DataParallelTarget *DataParallelTarget
}

// String returns a string representation of the endpoint.
func (epm *EndpointMetadata) String() string {
	if epm == nil {
		return ""
	}
	return fmt.Sprintf("%+v", *epm)
}

// Clone returns a full copy of the object.
func (epm *EndpointMetadata) Clone() *EndpointMetadata {
	if epm == nil {
		return nil
	}

	clonedLabels := make(map[string]string, len(epm.Labels))
	maps.Copy(clonedLabels, epm.Labels)
	var dataParallelTarget *DataParallelTarget
	if epm.DataParallelTarget != nil {
		dataParallelTarget = &DataParallelTarget{
			GlobalRank: epm.DataParallelTarget.GlobalRank,
			Selector:   epm.DataParallelTarget.Selector,
		}
	}
	return &EndpointMetadata{
		ID: types.NamespacedName{
			Name:      epm.ID.Name,
			Namespace: epm.ID.Namespace,
		},
		Name:               epm.Name,
		Address:            epm.Address,
		KVEventHost:        epm.KVEventHost,
		NodeAddress:        epm.NodeAddress,
		Port:               epm.Port,
		MetricsHost:        epm.MetricsHost,
		Labels:             clonedLabels,
		RankIndex:          epm.RankIndex,
		DataParallelTarget: dataParallelTarget,
	}
}

// Equal reports whether two EndpointMetadata values describe the same endpoint
// metadata. A nil Labels map and an empty Labels map are treated as equal.
func (epm *EndpointMetadata) Equal(other *EndpointMetadata) bool {
	if epm == nil || other == nil {
		return epm == other
	}
	return epm.ID == other.ID &&
		epm.Name == other.Name &&
		epm.Address == other.Address &&
		epm.KVEventHost == other.KVEventHost &&
		epm.NodeAddress == other.NodeAddress &&
		epm.Port == other.Port &&
		epm.MetricsHost == other.MetricsHost &&
		epm.RankIndex == other.RankIndex &&
		dataParallelTargetsEqual(epm.DataParallelTarget, other.DataParallelTarget) &&
		maps.Equal(epm.Labels, other.Labels)
}

func dataParallelTargetsEqual(a, b *DataParallelTarget) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// GetRankIndex returns the rank index of this endpoint within the pool's
// TargetPorts list.
func (epm *EndpointMetadata) GetRankIndex() int {
	if epm == nil {
		return 0
	}
	return epm.RankIndex
}

// GetID returns the endpoint's unique identity.
func (epm *EndpointMetadata) GetID() ID {
	return epm.ID
}

// GetNamespacedName returns the endpoint's unique identity as a types.NamespacedName.
func (epm *EndpointMetadata) GetNamespacedName() types.NamespacedName {
	if epm == nil {
		return types.NamespacedName{}
	}
	return epm.ID
}

// GetIPAddress returns the Endpoint's IP address.
func (epm *EndpointMetadata) GetIPAddress() string {
	return epm.Address
}

// GetNodeAddress returns the IP of the node hosting this endpoint.
func (epm *EndpointMetadata) GetNodeAddress() string {
	if epm == nil {
		return ""
	}
	return epm.NodeAddress
}

// GetPort returns the Endpoint's inference port.
func (epm *EndpointMetadata) GetPort() string {
	return epm.Port
}

// GetMetricsHost returns the Endpoint's metrics host (ip:port)
func (epm *EndpointMetadata) GetMetricsHost() string {
	return epm.MetricsHost
}

// GetKVEventHost returns the host publishing KV events for this endpoint.
func (epm *EndpointMetadata) GetKVEventHost() string {
	if epm == nil {
		return ""
	}
	if epm.KVEventHost != "" {
		return epm.KVEventHost
	}
	return epm.Address
}

// GetKVEventPortOffset returns the publisher port offset for this endpoint.
func (epm *EndpointMetadata) GetKVEventPortOffset() int {
	if epm != nil && epm.DataParallelTarget != nil {
		return epm.DataParallelTarget.GlobalRank
	}
	return epm.GetRankIndex()
}

// GetCacheIdentity returns the endpoint identity used by rank-local cache data.
func (epm *EndpointMetadata) GetCacheIdentity() string {
	if epm == nil {
		return ""
	}
	if epm.DataParallelTarget != nil {
		return epm.ID.String()
	}
	if epm.Address == "" {
		return ""
	}
	return epm.Address + ":" + epm.Port
}

// GetObservabilityName returns a rank-specific name for logical endpoints.
func (epm *EndpointMetadata) GetObservabilityName() string {
	if epm == nil {
		return ""
	}
	if epm.DataParallelTarget != nil {
		return epm.ID.Name
	}
	return epm.Name
}
