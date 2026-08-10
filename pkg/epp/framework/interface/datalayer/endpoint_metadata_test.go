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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	name      = "test-pod"
	namespace = "default"
	podip     = "192.168.1.123"
)

var (
	labels = map[string]string{
		"app":  "inference-server",
		"env":  "prod",
		"team": "ml",
	}
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Status: corev1.PodStatus{
			PodIP: podip,
		},
	}
	expected = &EndpointMetadata{
		ID:      types.NamespacedName{Name: name, Namespace: namespace},
		Address: podip,
		Labels:  labels,
	}
)

func TestEndpointMetadataClone(t *testing.T) {
	clone := expected.Clone()
	assert.NotSame(t, expected, clone)
	if diff := cmp.Diff(expected, clone); diff != "" {
		t.Errorf("Unexpected output (-want +got): %v", diff)
	}

	clone.Labels["env"] = "staging"
	assert.Equal(t, "prod", expected.Labels["env"], "mutating clone should not affect original")
}

func TestEndpointMetadataEqual(t *testing.T) {
	base := &EndpointMetadata{
		ID:                 types.NamespacedName{Name: "pod-a-rank-0", Namespace: "default"},
		Name:               "pod-a",
		Address:            "10.0.0.1",
		KVEventHost:        "10.0.0.2",
		Port:               "8000",
		MetricsHost:        "10.0.0.1:9000",
		Labels:             map[string]string{"app": "vllm"},
		RankIndex:          1,
		DataParallelTarget: &DataParallelTarget{GlobalRank: 4, Selector: 0},
	}

	assert.True(t, base.Equal(base.Clone()))

	var nilMetadata *EndpointMetadata
	assert.True(t, nilMetadata.Equal(nil))
	assert.False(t, nilMetadata.Equal(base))
	assert.False(t, base.Equal(nil))

	assert.True(t,
		(&EndpointMetadata{Labels: nil}).Equal(&EndpointMetadata{Labels: map[string]string{}}),
		"nil and empty labels should be treated as equivalent")

	tests := []struct {
		name   string
		mutate func(*EndpointMetadata)
	}{
		{
			name: "namespaced name",
			mutate: func(meta *EndpointMetadata) {
				meta.ID.Name = "pod-b-rank-0"
			},
		},
		{
			name: "endpoint name",
			mutate: func(meta *EndpointMetadata) {
				meta.Name = "pod-b"
			},
		},
		{
			name: "address",
			mutate: func(meta *EndpointMetadata) {
				meta.Address = "10.0.0.2"
			},
		},
		{
			name: "KV event host",
			mutate: func(meta *EndpointMetadata) {
				meta.KVEventHost = "10.0.0.3"
			},
		},
		{
			name: "port",
			mutate: func(meta *EndpointMetadata) {
				meta.Port = "8001"
			},
		},
		{
			name: "metrics host",
			mutate: func(meta *EndpointMetadata) {
				meta.MetricsHost = "10.0.0.1:9001"
			},
		},
		{
			name: "labels",
			mutate: func(meta *EndpointMetadata) {
				meta.Labels["app"] = "sglang"
			},
		},
		{
			name: "rank index",
			mutate: func(meta *EndpointMetadata) {
				meta.RankIndex = 2
			},
		},
		{
			name: "data parallel global rank",
			mutate: func(meta *EndpointMetadata) {
				meta.DataParallelTarget.GlobalRank = 5
			},
		},
		{
			name: "data parallel selector",
			mutate: func(meta *EndpointMetadata) {
				meta.DataParallelTarget.Selector = 1
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := base.Clone()
			tt.mutate(changed)
			assert.False(t, base.Equal(changed))
		})
	}
}

func TestEndpointMetadataRoutingIdentities(t *testing.T) {
	physical := &EndpointMetadata{
		ID:        types.NamespacedName{Name: "pod-a-rank-1", Namespace: "default"},
		Name:      "pod-a",
		Address:   "10.0.0.1",
		Port:      "8001",
		RankIndex: 1,
	}
	assert.Equal(t, "10.0.0.1", physical.GetKVEventHost())
	assert.Equal(t, 1, physical.GetKVEventPortOffset())
	assert.Equal(t, "10.0.0.1:8001", physical.GetCacheIdentity())
	assert.Equal(t, "pod-a", physical.GetObservabilityName())
	assert.Empty(t, (&EndpointMetadata{Port: "8001"}).GetCacheIdentity())
	assert.Equal(t, "2001:db8::1:8001", (&EndpointMetadata{Address: "2001:db8::1", Port: "8001"}).GetCacheIdentity())

	logical := &EndpointMetadata{
		ID:                 types.NamespacedName{Name: "pod-b-dp-4", Namespace: "default"},
		Name:               "pod-b",
		Address:            "10.0.0.1",
		KVEventHost:        "10.0.0.2",
		Port:               "8000",
		DataParallelTarget: &DataParallelTarget{GlobalRank: 4, Selector: 4},
	}
	assert.Equal(t, "10.0.0.2", logical.GetKVEventHost())
	assert.Equal(t, 4, logical.GetKVEventPortOffset())
	assert.Equal(t, "default/pod-b-dp-4", logical.GetCacheIdentity())
	assert.Equal(t, "pod-b-dp-4", logical.GetObservabilityName())
}

func TestEndpointMetadataString(t *testing.T) {
	endpointMetadata := EndpointMetadata{
		ID: types.NamespacedName{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		Name:        pod.Name,
		Address:     pod.Status.PodIP,
		Port:        "8000",
		MetricsHost: "127.0.0.1:8000",
		Labels:      labels,
	}

	s := endpointMetadata.String()
	assert.Contains(t, s, name)
	assert.Contains(t, s, namespace)
	assert.Contains(t, s, podip)
}

func TestLogicalEndpointMetadataStringIncludesRank(t *testing.T) {
	metadata := &EndpointMetadata{
		ID:                 types.NamespacedName{Name: "member-dp-4", Namespace: "default"},
		DataParallelTarget: &DataParallelTarget{GlobalRank: 4, Selector: 4},
	}

	assert.Contains(t, metadata.String(), "GlobalRank:4")
	assert.Contains(t, metadata.String(), "Selector:4")
}
