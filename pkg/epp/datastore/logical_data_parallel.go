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
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	podutil "github.com/llm-d/llm-d-router/pkg/epp/util/pod"
)

const (
	dataParallelRanksPerMemberLabel = "llm-d.ai/data-parallel-ranks-per-member"
	dataParallelRankStartLabel      = "llm-d.ai/data-parallel-rank-start"
	lwsWorkerIndexLabel             = "leaderworkerset.sigs.k8s.io/worker-index"
	lwsLeaderNameAnnotation         = "leaderworkerset.sigs.k8s.io/leader-name"
	lwsGroupSizeAnnotation          = "leaderworkerset.sigs.k8s.io/size"
)

type logicalDataParallelGroup struct {
	Namespace  string
	LeaderName string
	LWS        bool
}

func (g logicalDataParallelGroup) String() string {
	return types.NamespacedName{Namespace: g.Namespace, Name: g.LeaderName}.String()
}

type logicalDataParallelMember struct {
	pod         *corev1.Pod
	workerIndex int
	rankStart   int
	rankCount   int
}

func hasLogicalDataParallelLabel(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	_, ok := pod.Labels[dataParallelRanksPerMemberLabel]
	return ok
}

func logicalDataParallelGroupForPod(pod *corev1.Pod) (logicalDataParallelGroup, error) {
	if pod == nil {
		return logicalDataParallelGroup{}, errors.New("logical data parallel pod is nil")
	}
	indexValue, isLWS := pod.Labels[lwsWorkerIndexLabel]
	if !isLWS {
		return logicalDataParallelGroup{
			Namespace:  pod.Namespace,
			LeaderName: pod.Name,
		}, nil
	}
	workerIndex, err := strconv.Atoi(indexValue)
	if err != nil || workerIndex < 0 {
		return logicalDataParallelGroup{}, fmt.Errorf("pod %s has invalid %s %q", pod.Name, lwsWorkerIndexLabel, indexValue)
	}
	leaderName := pod.Annotations[lwsLeaderNameAnnotation]
	if workerIndex == 0 && leaderName == "" {
		leaderName = pod.Name
	}
	if leaderName == "" {
		return logicalDataParallelGroup{}, fmt.Errorf("pod %s is missing %s", pod.Name, lwsLeaderNameAnnotation)
	}
	return logicalDataParallelGroup{
		Namespace:  pod.Namespace,
		LeaderName: leaderName,
		LWS:        true,
	}, nil
}

func (ds *datastore) upsertLogicalDataParallelPod(
	ctx context.Context,
	pod *corev1.Pod,
	pool *datalayer.EndpointPool,
) error {
	ds.discoveryMu.Lock()
	defer ds.discoveryMu.Unlock()
	ds.removePhysicalEndpointsForPodLocked(pod.Namespace, pod.Name)

	id := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	oldGroup, hadOldGroup := ds.podGroups[id]
	ds.sourcePods[id] = pod.DeepCopy()

	group, err := logicalDataParallelGroupForPod(pod)
	if err != nil {
		delete(ds.podGroups, id)
		if hadOldGroup {
			ds.withdrawLogicalDataParallelGroupLocked(oldGroup)
		}
		return err
	}
	ds.podGroups[id] = group

	if hadOldGroup && oldGroup != group {
		ds.withdrawLogicalDataParallelGroupLocked(oldGroup)
	}
	return ds.reconcileLogicalDataParallelGroupLocked(ctx, group, pool)
}

func (ds *datastore) removeLogicalDataParallelPod(namespace, name string) {
	ds.discoveryMu.Lock()
	defer ds.discoveryMu.Unlock()

	id := types.NamespacedName{Namespace: namespace, Name: name}
	group, ok := ds.podGroups[id]
	if !ok {
		return
	}
	delete(ds.sourcePods, id)
	delete(ds.podGroups, id)
	ds.withdrawLogicalDataParallelGroupLocked(group)
}

func (ds *datastore) removePhysicalEndpointsForPodLocked(namespace, name string) {
	ds.pods.Range(func(key, value any) bool {
		endpoint := value.(fwkdl.Endpoint)
		metadata := endpoint.GetMetadata()
		if metadata.ID.Namespace == namespace && metadata.Name == name && metadata.DataParallelTarget == nil {
			ds.pods.Delete(key)
			ds.epf.ReleaseEndpoint(endpoint)
		}
		return true
	})
}

func (ds *datastore) reconcileLogicalDataParallelGroupLocked(
	ctx context.Context,
	group logicalDataParallelGroup,
	pool *datalayer.EndpointPool,
) error {
	desired, err := ds.buildLogicalDataParallelGroupLocked(group, pool)
	if err != nil {
		ds.withdrawLogicalDataParallelGroupLocked(group)
		return fmt.Errorf("logical data parallel group %s is not advertisable: %w", group, err)
	}

	for _, metadata := range desired {
		if _, err := ds.upsertEndpoint(ctx, metadata); err != nil {
			ds.withdrawLogicalDataParallelGroupLocked(group)
			return err
		}
		ds.endpointGroups[metadata.ID] = group
	}

	desiredIDs := make(map[types.NamespacedName]struct{}, len(desired))
	for _, metadata := range desired {
		desiredIDs[metadata.ID] = struct{}{}
	}
	for id, endpointGroup := range ds.endpointGroups {
		if endpointGroup != group {
			continue
		}
		if _, ok := desiredIDs[id]; ok {
			continue
		}
		ds.EndpointDelete(id)
		delete(ds.endpointGroups, id)
	}
	return nil
}

func (ds *datastore) withdrawLogicalDataParallelGroupLocked(group logicalDataParallelGroup) {
	for id, endpointGroup := range ds.endpointGroups {
		if endpointGroup != group {
			continue
		}
		ds.EndpointDelete(id)
		delete(ds.endpointGroups, id)
	}
}

func (ds *datastore) buildLogicalDataParallelGroupLocked(
	group logicalDataParallelGroup,
	pool *datalayer.EndpointPool,
) ([]*fwkdl.EndpointMetadata, error) {
	if pool == nil {
		return nil, errors.New("InferencePool is not initialized")
	}

	pods := make([]*corev1.Pod, 0)
	for id, podGroup := range ds.podGroups {
		if podGroup == group {
			pods = append(pods, ds.sourcePods[id])
		}
	}
	if len(pods) == 0 {
		return nil, errors.New("group has no Ready members")
	}

	expectedMembers := 1
	if group.LWS {
		var err error
		expectedMembers, err = positiveIntegerAnnotation(pods[0], lwsGroupSizeAnnotation)
		if err != nil {
			return nil, err
		}
	}
	if len(pods) != expectedMembers {
		return nil, fmt.Errorf("expected %d Ready members, found %d", expectedMembers, len(pods))
	}

	members := make([]logicalDataParallelMember, 0, len(pods))
	workerIndexes := make(map[int]struct{}, len(pods))
	var leader *corev1.Pod
	for _, pod := range pods {
		if !podutil.IsPodReady(pod) {
			return nil, fmt.Errorf("pod %s is not Ready", pod.Name)
		}

		workerIndex := 0
		if group.LWS {
			size, err := positiveIntegerAnnotation(pod, lwsGroupSizeAnnotation)
			if err != nil {
				return nil, err
			}
			if size != expectedMembers {
				return nil, fmt.Errorf("pod %s reports group size %d, expected %d", pod.Name, size, expectedMembers)
			}
			workerIndex, err = nonNegativeIntegerLabel(pod, lwsWorkerIndexLabel)
			if err != nil {
				return nil, err
			}
			if workerIndex >= expectedMembers {
				return nil, fmt.Errorf("pod %s has worker index %d outside group size %d", pod.Name, workerIndex, expectedMembers)
			}
		}
		if _, exists := workerIndexes[workerIndex]; exists {
			return nil, fmt.Errorf("worker index %d is duplicated", workerIndex)
		}
		workerIndexes[workerIndex] = struct{}{}
		if workerIndex == 0 {
			leader = pod
		}

		rankCount, err := positiveIntegerLabel(pod, dataParallelRanksPerMemberLabel)
		if err != nil {
			return nil, err
		}
		rankStart := workerIndex * rankCount
		if value, ok := pod.Labels[dataParallelRankStartLabel]; ok {
			rankStart, err = strconv.Atoi(value)
			if err != nil || rankStart < 0 {
				return nil, fmt.Errorf("pod %s has invalid %s %q", pod.Name, dataParallelRankStartLabel, value)
			}
		}

		members = append(members, logicalDataParallelMember{
			pod:         pod,
			workerIndex: workerIndex,
			rankStart:   rankStart,
			rankCount:   rankCount,
		})
	}
	if leader == nil {
		return nil, errors.New("group has no Ready leader")
	}
	if leader.Name != group.LeaderName {
		return nil, fmt.Errorf("leader pod %s does not match declared leader name %s", leader.Name, group.LeaderName)
	}
	activePorts := extractActivePorts(leader, pool.TargetPorts).UnsortedList()
	if len(activePorts) != 1 {
		return nil, fmt.Errorf("leader pod %s must have one active target port, found %d", leader.Name, len(activePorts))
	}
	groupPort := activePorts[0]
	if len(workerIndexes) != expectedMembers {
		return nil, errors.New("worker indexes are not contiguous")
	}
	for workerIndex := 0; workerIndex < expectedMembers; workerIndex++ {
		if _, ok := workerIndexes[workerIndex]; !ok {
			return nil, fmt.Errorf("worker index %d is missing", workerIndex)
		}
	}

	slices.SortFunc(members, func(a, b logicalDataParallelMember) int {
		return a.rankStart - b.rankStart
	})
	nextRank := 0
	for _, member := range members {
		if member.rankStart != nextRank {
			return nil, fmt.Errorf("rank range for pod %s begins at %d, expected %d", member.pod.Name, member.rankStart, nextRank)
		}
		nextRank += member.rankCount
	}

	port := strconv.Itoa(groupPort)
	desired := make([]*fwkdl.EndpointMetadata, 0, nextRank)
	for _, member := range members {
		memberLabels := make(map[string]string, len(member.pod.Labels))
		maps.Copy(memberLabels, member.pod.Labels)
		for rank := member.rankStart; rank < member.rankStart+member.rankCount; rank++ {
			desired = append(desired, &fwkdl.EndpointMetadata{
				ID: types.NamespacedName{
					Name:      fmt.Sprintf("%s-dp-%d", member.pod.Name, rank),
					Namespace: member.pod.Namespace,
				},
				Name:        member.pod.Name,
				Address:     leader.Status.PodIP,
				KVEventHost: member.pod.Status.PodIP,
				NodeAddress: member.pod.Status.HostIP,
				Port:        port,
				MetricsHost: net.JoinHostPort(leader.Status.PodIP, port),
				Labels:      memberLabels,
				DataParallelTarget: &fwkdl.DataParallelTarget{
					GlobalRank: rank,
					Selector:   rank,
				},
			})
		}
	}
	return desired, nil
}

func positiveIntegerLabel(pod *corev1.Pod, key string) (int, error) {
	value, ok := pod.Labels[key]
	if !ok {
		return 0, fmt.Errorf("pod %s is missing %s", pod.Name, key)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("pod %s has invalid %s %q", pod.Name, key, value)
	}
	return parsed, nil
}

func nonNegativeIntegerLabel(pod *corev1.Pod, key string) (int, error) {
	value, ok := pod.Labels[key]
	if !ok {
		return 0, fmt.Errorf("pod %s is missing %s", pod.Name, key)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("pod %s has invalid %s %q", pod.Name, key, value)
	}
	return parsed, nil
}

func positiveIntegerAnnotation(pod *corev1.Pod, key string) (int, error) {
	value, ok := pod.Annotations[key]
	if !ok {
		return 0, fmt.Errorf("pod %s is missing %s", pod.Name, key)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("pod %s has invalid %s %q", pod.Name, key, value)
	}
	return parsed, nil
}
