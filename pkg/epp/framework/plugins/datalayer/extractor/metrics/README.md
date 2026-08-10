# Core Metrics Extractor

**Type:** `core-metrics-extractor`

> [!NOTE]
> This plugin is enabled by default together with `metrics-data-source`. You do not need to explicitly declare it in your configuration, but it can be disabled if metrics collection is unnecessary.

The Core Metrics Extractor is a data layer plugin responsible for extracting model server metrics from a data source and storing them as endpoint attributes. It supports multiple inference engines and can be configured to map engine-specific metric names to a standard set of internal keys.

## What it does

1.  Receives a `PrometheusMetricMap` from a metrics data source (e.g., `metrics-data-source`).
2.  Identifies the inference engine type of the endpoint (e.g., vLLM, SGLang, Triton) using a Pod label.
3.  Looks up the metric specifications for that engine.
4.  Extracts values for standard metrics:
    -   **Waiting Queue Size**: Number of requests waiting in the engine's queue.
    -   **Running Requests Size**: Number of requests currently being processed.
    -   **KV Cache Usage**: Percentage of KV cache currently utilized.
    -   **KV Cache Capacity**: Maximum number of tokens the cache can hold.
    -   **LoRA Adapters**: Information about active and waiting LoRA adapters.
    -   **Cache Configuration**: Block size and total number of GPU blocks.
5.  Stores these values as attributes on the endpoint, making them available to scheduling plugins.

## Attributes produced

The plugin populates several standard keys on the endpoint:

-   `WaitingQueueSize` (int)
-   `RunningRequestsSize` (int)
-   `KVCacheUsagePercent` (float64)
-   `KvCacheMaxTokenCapacity` (int)
-   `MaxActiveModels` (int)
-   `ActiveModels` (int)
-   `WaitingModels` (int)
-   `UpdateTime` (time.Time)

## Configuration

The plugin config supports:

-   `engineLabelKey`: The Pod label key used to identify the engine type. Defaults to `llm-d.ai/engine-type`. 
    The deprecated GAIE key `inference.networking.k8s.io/engine-type` is also supported as a fallback, 
    but will be removed in a future release.
-   `defaultEngine`: The engine type to use if the label is missing. Defaults to `vllm`.
-   `engineConfigs`: A list of engine-specific metric specifications.
    `dataParallelRankLabel` identifies the label that distinguishes rank-local series for logical
    endpoints. Built-in SGLang and vLLM mappings use `dp_rank` and `engine`, respectively.
    `maxTokenCapacitySpec` optionally maps a rank-local token-capacity series.
    `timestampSpec` optionally maps a Unix-seconds snapshot timestamp. When configured, it becomes
    the endpoint's `UpdateTime`, so scheduling staleness reflects the engine snapshot rather than
    the router scrape time.
    Each engine config can also include `customMetrics` entries. Each entry
    maps a scalar metric selector to an endpoint attribute key.

### Built-in Engine Configurations

The plugin comes with built-in support for the following engines:
-   `vllm`
-   `sglang`
-   `trtllm-serve`
-   `triton-tensorrt-llm`

To correctly establish the mapping, model server Pods should be labeled using the `engineLabelKey` with the engine type as follows:

```yaml
metadata:
  labels:
    llm-d.ai/engine-type: vllm # other options: sglang, trtllm-serve, triton-tensorrt-llm, triton 

```


### Custom Engine Configuration Example

```yaml
type: core-metrics-extractor
parameters:
  engineConfigs:
    - name: "my-custom-engine"
      dataParallelRankLabel: "dp_rank"
      queuedRequestsSpec: "custom_queue_size{status=waiting}"
      runningRequestsSpec: "custom_running_size"
      kvUsageSpec: "custom_cache_utilization"
      maxTokenCapacitySpec: "custom_max_total_tokens"
      timestampSpec: "custom_snapshot_timestamp"
      customMetrics:
        - attributeKey: "custom.queue_depth"
          metricSpec: "custom_queue_depth{tier=gold}"
```

and the model server deployment Pods should have the label:

```yaml
metadata:
  labels:
    llm-d.ai/engine-type: my-custom-engine

```

```

Rank filtering applies to queue, running-request, KV-usage, and cache-capacity metrics. LoRA and
custom metric selectors are unchanged unless their own metric specification includes labels.

## Multi-cluster support

`multicluster-metrics-extractor` is the cluster-scoped variant. It reads only a pool's aggregate metrics (`llm_d_epp_average_kv_cache_utilization`, `llm_d_epp_average_queue_size`) into the `llm-d.ai/multicluster-*` attributes the multicluster scorers read, rather than running per-pod engine extraction. Pair it with `multicluster-metrics-data-source`. See the [wiring example](../../../README.md#example).
