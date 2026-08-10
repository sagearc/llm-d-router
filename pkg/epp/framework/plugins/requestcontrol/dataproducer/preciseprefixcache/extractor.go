/*
Copyright 2026 The llm-d Authors.

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

package preciseprefixcache

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

var _ fwkdl.EndpointExtractor = &Producer{}

// Extract processes endpoint lifecycle events emitted by the
// endpoint-notification-source: add/update installs a per-pod ZMQ KV-events
// subscriber, delete tears one down. No-op unless per-pod discovery is
// enabled.
func (p *Producer) Extract(ctx context.Context, event fwkdl.EndpointEvent) error {
	if !p.kvEventsConfig.DiscoverPods || p.kvEventsConfig.PodDiscoveryConfig == nil {
		return nil
	}
	meta := event.Endpoint.GetMetadata()
	if meta == nil || meta.ID.Name == "" {
		return nil
	}

	logger := log.FromContext(ctx).WithName(p.typedName.String())
	endpointKey := meta.ID.String()

	switch event.Type {
	case fwkdl.EventAddOrUpdate:
		if err := p.ensureSubscriber(ctx, meta); err != nil {
			return err
		}
		logger.V(logging.DEBUG).Info("Adding subscriber", "endpoint", endpointKey)
	case fwkdl.EventDelete:
		p.subscribersManager.RemoveSubscriber(ctx, endpointKey)
		if cacheIdentity := meta.GetCacheIdentity(); cacheIdentity != "" {
			if err := p.kvCacheIndexer.KVBlockIndex().Clear(ctx, cacheIdentity); err != nil {
				logger.Error(err, "Failed to clear index entries for removed endpoint",
					"endpoint", endpointKey, "address", meta.Address, "port", meta.Port)
			}
		}
		logger.V(logging.DEBUG).Info("Removed KV-events subscriber", "endpoint", endpointKey)
	}
	return nil
}

// ensureSubscriber idempotently installs a KV-events subscriber for the given
// endpoint, dialing the publisher host and rank-specific port associated with it.
func (p *Producer) ensureSubscriber(ctx context.Context, meta *fwkdl.EndpointMetadata) error {
	if meta == nil || meta.GetKVEventHost() == "" {
		return nil
	}
	endpointKey := meta.ID.String()
	eventHost := meta.GetKVEventHost()
	portOffset := meta.GetKVEventPortOffset()
	port := p.kvEventsConfig.PodDiscoveryConfig.SocketPort + portOffset
	zmqEndpoint := fmt.Sprintf("tcp://%s:%d", eventHost, port)
	replayEndpoint := ""
	if replayPort := p.kvEventsConfig.PodDiscoveryConfig.EffectiveReplayPort(); replayPort > 0 {
		replayEndpoint = fmt.Sprintf("tcp://%s:%d", eventHost, replayPort+portOffset)
	}
	sourceEndpoint := meta.GetCacheIdentity()
	var expectedDataParallelRank *int
	if meta.DataParallelTarget != nil {
		rank := meta.DataParallelTarget.GlobalRank
		expectedDataParallelRank = &rank
	}

	logger := log.FromContext(ctx).WithName(p.typedName.String())
	// subscriberCtx is plugin-lifetime; caller ctx would tear subscribers
	// down on request completion.
	if err := p.subscribersManager.EnsureSubscriber(p.subscriberCtx, endpointKey,
		sourceEndpoint, zmqEndpoint, replayEndpoint, p.kvEventsConfig.TopicFilter,
		expectedDataParallelRank, true); err != nil {
		logger.Error(err, "Failed to ensure KV-events subscriber for endpoint",
			"endpoint", endpointKey, "address", meta.Address)
		return fmt.Errorf("ensure subscriber for %s: %w", endpointKey, err)
	}
	logger.V(logging.DEBUG).Info("Ensured KV-events subscriber",
		"endpoint", endpointKey, "zmq", zmqEndpoint, "replay", replayEndpoint)
	return nil
}
