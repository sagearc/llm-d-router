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

package datastore

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

func TestLogicalDataParallelLWSGroup(t *testing.T) {
	ctx := context.Background()
	ds := NewDatastore(ctx, &mockEndpointFactory{}).WithEndpointPool(&datalayer.EndpointPool{
		Namespace:   "default",
		Selector:    labels.Everything(),
		TargetPorts: []int{8000},
	})
	leader := logicalDataParallelPod("wideep-0", "10.0.0.1", "192.168.1.1", "0", "")
	member := logicalDataParallelPod("wideep-0-1", "10.0.0.2", "192.168.1.2", "1", leader.Name)
	member.Annotations[activePortsAnnotation] = ""

	require.Error(t, ds.PodUpdateOrAddIfNotExist(ctx, leader))
	require.Empty(t, ds.PodList(AllPodsPredicate))
	require.NoError(t, ds.PodUpdateOrAddIfNotExist(ctx, member))

	got := endpointMetadata(ds.PodList(AllPodsPredicate))
	want := []*fwkdl.EndpointMetadata{
		logicalDataParallelEndpoint(member, leader, 2),
		logicalDataParallelEndpoint(member, leader, 3),
		logicalDataParallelEndpoint(leader, leader, 0),
		logicalDataParallelEndpoint(leader, leader, 1),
	}
	if diff := cmp.Diff(want, got, cmpopts.SortSlices(func(a, b *fwkdl.EndpointMetadata) bool {
		return a.ID.Name < b.ID.Name
	})); diff != "" {
		t.Fatalf("logical endpoints differ (-want +got):\n%s", diff)
	}
	invalidMember := member.DeepCopy()
	invalidMember.Labels[dataParallelRankStartLabel] = "1"
	require.Error(t, ds.PodUpdateOrAddIfNotExist(ctx, invalidMember))
	require.Empty(t, ds.PodList(AllPodsPredicate), "an invalid update must withdraw the complete group")
	require.NoError(t, ds.PodUpdateOrAddIfNotExist(ctx, member))
	require.Len(t, ds.PodList(AllPodsPredicate), 4)

	ds.PodDelete(member.Name)
	require.Empty(t, ds.PodList(AllPodsPredicate), "an incomplete group must be withdrawn")
}

func TestLogicalDataParallelRankStartOverride(t *testing.T) {
	ctx := context.Background()
	ds := NewDatastore(ctx, &mockEndpointFactory{}).WithEndpointPool(&datalayer.EndpointPool{
		Namespace:   "default",
		Selector:    labels.Everything(),
		TargetPorts: []int{8000},
	})
	leader := logicalDataParallelPod("wideep-0", "10.0.0.1", "192.168.1.1", "0", "")
	member := logicalDataParallelPod("wideep-0-1", "10.0.0.2", "192.168.1.2", "1", leader.Name)
	leader.Labels[dataParallelRanksPerMemberLabel] = "1"
	member.Labels[dataParallelRankStartLabel] = "1"

	require.Error(t, ds.PodUpdateOrAddIfNotExist(ctx, leader))
	require.NoError(t, ds.PodUpdateOrAddIfNotExist(ctx, member))
	got := endpointMetadata(ds.PodList(AllPodsPredicate))
	require.Len(t, got, 3)
	ranks := make([]int, 0, len(got))
	for _, endpoint := range got {
		ranks = append(ranks, endpoint.DataParallelTarget.GlobalRank)
	}
	require.ElementsMatch(t, []int{0, 1, 2}, ranks)
}

func TestLogicalDataParallelPodLabelTransitions(t *testing.T) {
	ctx := context.Background()
	ds := NewDatastore(ctx, &mockEndpointFactory{}).WithEndpointPool(&datalayer.EndpointPool{
		Namespace:   "default",
		Selector:    labels.Everything(),
		TargetPorts: []int{8000},
	})
	pod := logicalDataParallelPod("wideep", "10.0.0.1", "192.168.1.1", "", "")
	delete(pod.Labels, lwsWorkerIndexLabel)
	delete(pod.Annotations, lwsGroupSizeAnnotation)

	physical := pod.DeepCopy()
	delete(physical.Labels, dataParallelRanksPerMemberLabel)
	require.NoError(t, ds.PodUpdateOrAddIfNotExist(ctx, physical))
	physicalEndpoints := endpointMetadata(ds.PodList(AllPodsPredicate))
	require.Len(t, physicalEndpoints, 1)
	require.Nil(t, physicalEndpoints[0].DataParallelTarget)
	require.Equal(t, "wideep-rank-0", physicalEndpoints[0].ID.Name)

	require.NoError(t, ds.PodUpdateOrAddIfNotExist(ctx, pod))
	logicalEndpoints := endpointMetadata(ds.PodList(AllPodsPredicate))
	require.Len(t, logicalEndpoints, 2)
	for _, endpoint := range logicalEndpoints {
		require.NotNil(t, endpoint.DataParallelTarget)
	}

	require.NoError(t, ds.PodUpdateOrAddIfNotExist(ctx, physical))
	physicalEndpoints = endpointMetadata(ds.PodList(AllPodsPredicate))
	require.Len(t, physicalEndpoints, 1)
	require.Nil(t, physicalEndpoints[0].DataParallelTarget)
	require.Equal(t, "wideep-rank-0", physicalEndpoints[0].ID.Name)
}

func TestLogicalDataParallelSinglePod(t *testing.T) {
	ctx := context.Background()
	ds := NewDatastore(ctx, &mockEndpointFactory{}).WithEndpointPool(&datalayer.EndpointPool{
		Namespace:   "default",
		Selector:    labels.Everything(),
		TargetPorts: []int{8000},
	})
	pod := logicalDataParallelPod("wideep", "10.0.0.1", "192.168.1.1", "", "")
	delete(pod.Labels, lwsWorkerIndexLabel)
	delete(pod.Annotations, lwsGroupSizeAnnotation)

	require.NoError(t, ds.PodUpdateOrAddIfNotExist(ctx, pod))
	got := endpointMetadata(ds.PodList(AllPodsPredicate))
	require.Len(t, got, 2)
	require.ElementsMatch(t, []string{"wideep-dp-0", "wideep-dp-1"}, []string{got[0].ID.Name, got[1].ID.Name})
	require.Equal(t, "10.0.0.1", got[0].Address)
	require.Equal(t, "10.0.0.1", got[0].KVEventHost)
}

func TestLogicalDataParallelInvalidGroupsAreWithdrawn(t *testing.T) {
	tests := []struct {
		name        string
		targetPorts []int
		pods        func() []*corev1.Pod
	}{
		{
			name:        "malformed rank count",
			targetPorts: []int{8000},
			pods: func() []*corev1.Pod {
				pod := logicalDataParallelPod("wideep", "10.0.0.1", "192.168.1.1", "", "")
				delete(pod.Labels, lwsWorkerIndexLabel)
				delete(pod.Annotations, lwsGroupSizeAnnotation)
				pod.Labels[dataParallelRanksPerMemberLabel] = "bad"
				return []*corev1.Pod{pod}
			},
		},
		{
			name:        "unready member",
			targetPorts: []int{8000},
			pods: func() []*corev1.Pod {
				pod := logicalDataParallelPod("wideep", "10.0.0.1", "192.168.1.1", "", "")
				delete(pod.Labels, lwsWorkerIndexLabel)
				delete(pod.Annotations, lwsGroupSizeAnnotation)
				pod.Status.Conditions[0].Status = corev1.ConditionFalse
				return []*corev1.Pod{pod}
			},
		},
		{
			name:        "overlapping rank ranges",
			targetPorts: []int{8000},
			pods: func() []*corev1.Pod {
				leader := logicalDataParallelPod("wideep-0", "10.0.0.1", "192.168.1.1", "0", "")
				member := logicalDataParallelPod("wideep-0-1", "10.0.0.2", "192.168.1.2", "1", leader.Name)
				member.Labels[dataParallelRankStartLabel] = "1"
				return []*corev1.Pod{leader, member}
			},
		},
		{
			name:        "multiple active ports",
			targetPorts: []int{8000, 8001},
			pods: func() []*corev1.Pod {
				pod := logicalDataParallelPod("wideep", "10.0.0.1", "192.168.1.1", "", "")
				delete(pod.Labels, lwsWorkerIndexLabel)
				delete(pod.Annotations, lwsGroupSizeAnnotation)
				return []*corev1.Pod{pod}
			},
		},
		{
			name:        "leader name does not match index zero pod",
			targetPorts: []int{8000},
			pods: func() []*corev1.Pod {
				leader := logicalDataParallelPod("wideep-0", "10.0.0.1", "192.168.1.1", "0", "wrong-leader")
				member := logicalDataParallelPod("wideep-0-1", "10.0.0.2", "192.168.1.2", "1", "wrong-leader")
				return []*corev1.Pod{leader, member}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ds := NewDatastore(ctx, &mockEndpointFactory{}).WithEndpointPool(&datalayer.EndpointPool{
				Namespace:   "default",
				Selector:    labels.Everything(),
				TargetPorts: test.targetPorts,
			})
			var err error
			for _, pod := range test.pods() {
				err = ds.PodUpdateOrAddIfNotExist(ctx, pod)
			}
			require.Error(t, err)
			require.Empty(t, ds.PodList(AllPodsPredicate))
		})
	}
}

func logicalDataParallelPod(name, podIP, hostIP, workerIndex, leaderName string) *corev1.Pod {
	annotations := map[string]string{lwsGroupSizeAnnotation: "2"}
	if leaderName != "" {
		annotations[lwsLeaderNameAnnotation] = leaderName
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				dataParallelRanksPerMemberLabel: "2",
				lwsWorkerIndexLabel:             workerIndex,
				"llm-d.ai/role":                 "decode",
			},
			Annotations: annotations,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
			PodIP:  podIP,
			HostIP: hostIP,
		},
	}
}

func logicalDataParallelEndpoint(member, leader *corev1.Pod, rank int) *fwkdl.EndpointMetadata {
	return &fwkdl.EndpointMetadata{
		ID: types.NamespacedName{
			Name:      member.Name + "-dp-" + strconv.Itoa(rank),
			Namespace: member.Namespace,
		},
		Name:        member.Name,
		Address:     leader.Status.PodIP,
		KVEventHost: member.Status.PodIP,
		NodeAddress: member.Status.HostIP,
		Port:        "8000",
		MetricsHost: net.JoinHostPort(leader.Status.PodIP, "8000"),
		Labels: map[string]string{
			dataParallelRanksPerMemberLabel: "2",
			lwsWorkerIndexLabel:             member.Labels[lwsWorkerIndexLabel],
			"llm-d.ai/role":                 "decode",
		},
		DataParallelTarget: &fwkdl.DataParallelTarget{GlobalRank: rank, Selector: rank},
	}
}

func endpointMetadata(endpoints []fwkdl.Endpoint) []*fwkdl.EndpointMetadata {
	result := make([]*fwkdl.EndpointMetadata, 0, len(endpoints))
	for _, endpoint := range endpoints {
		result = append(result, endpoint.GetMetadata())
	}
	return result
}
