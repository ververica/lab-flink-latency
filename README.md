lab-flink-latency
=================

Lab to showcase different Flink job latency optimization techniques covered in our Flink Forward 2021 talk
["Getting into Low-Latency Gears with Apache Flink"](https://www.flink-forward.org/global-2021/conference-program#getting-into-low-latency-gears-with-apache-flink).

This lab consists of several jobs which are described as follows.

## IngestingJob

This job is used to ingest randomly generated sensor measurements into a Kafka topic. Use `--kafka` to specify the
Kafka bootstrap servers. This defaults to `localhost:9092`. Use `--topic` to specify the name of the Kakfa topic to
ingest into. This default is `lablatency`. You can also use
`--wait-micro <number of micro seconds>` to adjust the ingestion rate.

## WindowingJob

This job calculates the number of measurements and the sum of the measurement values per minute (window size), and updates the
result every 10 seconds (slide size). The latency of this job can be optimized by using the following techniques.

### Optimization 1
Increase the job parallelism, e.g., from 2 to 3. Best to have the number of the partitions of your Kafka topic
divisible by 2 and by 3 to avoid data skew.

### Optimization 2
Use the hashmap/filesystem state backend by changing the configuration from

    state.backend: rocksdb
    # 0.4 is Flink's default
    taskmanager.memory.managed.fraction: '0.4'

to

    # use filesystem if Flink < 1.13
    state.backend: hashmap
    taskmanager.memory.managed.fraction: '0.0'

### Optimization 3
Reduce the watermark interval from the default `200 ms` to `100 ms`:

    pipeline.auto-watermark-interval: 100 ms

### Optimization 4
Reduce the network buffer timeout from the default `100 ms` to `10 ms`:

    execution.buffer-timeout: 10 ms

## WindowingJobNoAggregation

Similar to WindowingJob, except that there is no incremental aggregation during windowing in this job.

## EnrichingJobSync

This job enriches measurements with the location information retrieved from a simulated external service which has a
random latency in the range of 1-6 ms. When location information is retrieved, the job caches it for 1 second to serve further
retrieving requests.

## EnrichingJobAsync

Similar to `EnrichingJobSync`, except that this job uses
[Flink's Async I/O](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/operators/asyncio/)
to get better performance.

## SortingJobPerEventTimer

This job sorts a stream of measurements keyed by sensor IDs, then calculates an
[exponential moving average](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average ) for each
sensor. When sorting, it creates a timer per event.

## SortingJobCoalescedTimer

Similar to `SortingJobPerEventTimer`, except that when sorting, it coalesces timers to the next 100ms (configurable
via `--round-timer-to`) or to the next watermark if `--round-timer-to` is set to `0`.

This job can be run with the follow options/configurations to manage the per-event overhead.

### User Code

Create only one ObjectMapper per operator instance (default)

    --use-one-mapper true

Create one ObjectMapper per event

    --use-one-mapper false

### Serialization

Use the POJO serializer  (default)

    --force-kryo false

Force using the Kryo serializer

    --force-kryo true

### Object Reuse

Disable object reuse with the following configuration (default)

    pipeline.object-reuse: false

Enable object reuse with the following configuration

    pipeline.object-reuse: true
