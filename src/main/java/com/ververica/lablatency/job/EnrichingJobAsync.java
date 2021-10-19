package com.ververica.lablatency.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.lablatency.event.EnrichedMeasurement;
import com.ververica.lablatency.event.Measurement;
import com.ververica.lablatency.event.MeasurementRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/** EnrichingJob: enrich measurements with location asynchronously. */
public class EnrichingJobAsync {

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(WindowingJob.class);

        ParameterTool params = ParameterTool.fromArgs(args);
        logger.info("params: " + params.getProperties());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpointing Configuration
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        final String jobName = params.get("job-name", EnrichingJobAsync.class.getSimpleName());
        final String kafkaAddress = params.get("kafka", "localhost:9092");
        final String topic = params.get("topic", "lablatency");
        final String group = params.get("group", "lablatency");
        final String startOffset = params.get("offset", "latest");

        final int outOfOrderness = params.getInt("out-of-orderness", 250);
        final int responseTimeMin = params.getInt("response-time-min", 1);
        final int responseTimeMax = params.getInt("response-time-max", 6);
        final int cacheExpiryMs = params.getInt("cache-expiry-ms", 1000);

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", kafkaAddress);
        kafkaConsumerProps.setProperty("group.id", group);
        kafkaConsumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, startOffset);
        FlinkKafkaConsumer<MeasurementRecord> consumer =
                new FlinkKafkaConsumer<>(topic, new KafkaDeSerSchema(), kafkaConsumerProps);
        // start from the latest message
        consumer.setStartFromLatest();

        DataStream<Tuple2<Measurement, Long>> sourceStream =
                env.addSource(consumer)
                        .name("KafkaSource")
                        .uid("KafkaSource")
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<MeasurementRecord>forBoundedOutOfOrderness(
                                                Duration.ofMillis(outOfOrderness))
                                        .withTimestampAssigner(
                                                (element, timestamp) -> element.getTimestamp())
                                        .withIdleness(Duration.ofSeconds(1)))
                        .name("Watermarks")
                        .uid("Watermarks")
                        .flatMap(new MeasurementDeserializer())
                        .name("Deserialization")
                        .uid("Deserialization");

        DataStream<EnrichedMeasurement> enrichedStream =
                AsyncDataStream.unorderedWait(
                                sourceStream.keyBy(x -> x.f0.getLocation()),
                                new EnrichMeasurementWithLocationInfoAsync(
                                        cacheExpiryMs, responseTimeMin, responseTimeMax),
                                0,
                                TimeUnit.MILLISECONDS,
                                30)
                        .name("MainOperator:Enrich");
        enrichedStream
                .addSink(new DiscardingSink<>())
                .name("NormalOutput")
                .uid("NormalOutput")
                .disableChaining();

        env.execute(jobName);
    }

    /** Get MeasurementRecord from Kafka ConsumerRecord. */
    static class KafkaDeSerSchema implements KafkaDeserializationSchema<MeasurementRecord> {

        @Override
        public void open(DeserializationSchema.InitializationContext context) throws Exception {}

        @Override
        public boolean isEndOfStream(MeasurementRecord nextElement) {
            return false;
        }

        @Override
        public MeasurementRecord deserialize(ConsumerRecord<byte[], byte[]> record)
                throws Exception {
            return new MeasurementRecord(
                    record.timestamp(), record.key(), record.value(), record.partition());
        }

        @Override
        public TypeInformation<MeasurementRecord> getProducedType() {
            return getForClass(MeasurementRecord.class);
        }
    }

    /** Deserializes MeasurementRecord into Measurement. */
    public static class MeasurementDeserializer
            extends RichFlatMapFunction<MeasurementRecord, Tuple2<Measurement, Long>> {

        private static final long serialVersionUID = 1L;
        private static Logger logger = LoggerFactory.getLogger(MeasurementDeserializer.class);

        private ObjectMapper objectMapper;

        @Override
        public void open(final Configuration parameters) throws Exception {
            super.open(parameters);
            this.objectMapper = new ObjectMapper();
        }

        @Override
        public void flatMap(
                final MeasurementRecord kafkaRecord,
                final Collector<Tuple2<Measurement, Long>> out) {
            final Measurement measurement;
            try {
                measurement =
                        this.objectMapper.readValue(kafkaRecord.getValue(), Measurement.class);
            } catch (IOException e) {
                logger.error("Failed to deserialize: " + e.getLocalizedMessage());
                return;
            }
            out.collect(new Tuple2<>(measurement, kafkaRecord.getTimestamp()));
        }
    }

    /** Enrich measurement with location asynchronously. */
    public static class EnrichMeasurementWithLocationInfoAsync
            extends RichAsyncFunction<Tuple2<Measurement, Long>, EnrichedMeasurement> {
        private static final long serialVersionUID = 2L;

        private transient LocationInfoServiceClient locationInfoServiceClient;
        private transient Map<String, Tuple2<Long, String>> cache;

        private static final int PROCESSING_TIME_DELAY_WINDOW_SIZE = 10_000;
        private transient DescriptiveStatisticsHistogram processingTimeDelay;

        private final int cacheExpiryMs;
        private Counter cacheSizeMetric;
        private Counter servedFromCacheMetric;
        private final int responseTimeMin;
        private final int responseTimeMax;

        /**
         * Creates a new enrichment function with a (local) cache that expires after the given
         * number of milliseconds.
         */
        public EnrichMeasurementWithLocationInfoAsync(
                int cacheExpiryMs, int responseTimeMin, int responseTimeMax) {
            this.cacheExpiryMs = cacheExpiryMs;
            this.responseTimeMin = responseTimeMin;
            this.responseTimeMax = responseTimeMax;
        }

        @Override
        public void open(final Configuration parameters) {
            locationInfoServiceClient =
                    new LocationInfoServiceClient(this.responseTimeMin, this.responseTimeMax);
            processingTimeDelay =
                    getRuntimeContext()
                            .getMetricGroup()
                            .histogram(
                                    "processingTimeDelay",
                                    new DescriptiveStatisticsHistogram(
                                            PROCESSING_TIME_DELAY_WINDOW_SIZE));
            cache = new HashMap<>();
            servedFromCacheMetric = getRuntimeContext().getMetricGroup().counter("servedFromCache");
            cacheSizeMetric = getRuntimeContext().getMetricGroup().counter("cacheSize");
        }

        @Override
        public void asyncInvoke(
                Tuple2<Measurement, Long> measurement,
                ResultFuture<EnrichedMeasurement> resultFuture) {
            String location = measurement.f0.getLocation();
            final String locationInfo;

            Tuple2<Long, String> cachedLocationInfo = cache.get(location);
            if (cachedLocationInfo != null
                    && System.currentTimeMillis() - cachedLocationInfo.f0 <= cacheExpiryMs) {
                locationInfo = cachedLocationInfo.f1;
                EnrichedMeasurement enrichedMeasurement =
                        new EnrichedMeasurement(measurement.f0, locationInfo);
                resultFuture.complete(Collections.singleton(enrichedMeasurement));
                servedFromCacheMetric.inc();
            } else {
                locationInfoServiceClient.asyncGetLocationInfo(
                        measurement.f0.getLocation(),
                        new LocationServiceCallBack(resultFuture, measurement, location));
            }
        }

        private class LocationServiceCallBack implements Consumer<String> {
            private final ResultFuture<EnrichedMeasurement> resultFuture;
            private final Tuple2<Measurement, Long> measurement;
            private final String location;

            public LocationServiceCallBack(
                    final ResultFuture<EnrichedMeasurement> resultFuture,
                    final Tuple2<Measurement, Long> measurement,
                    final String location) {
                this.resultFuture = resultFuture;
                this.measurement = measurement;
                this.location = location;
            }

            @Override
            public void accept(final String locationInfo) {
                EnrichedMeasurement enrichedMeasurement =
                        new EnrichedMeasurement(measurement.f0, locationInfo);
                resultFuture.complete(Collections.singleton(enrichedMeasurement));

                processingTimeDelay.update(System.currentTimeMillis() - measurement.f1);

                if (cache.put(location, new Tuple2<>(System.currentTimeMillis(), locationInfo))
                        == null) {
                    cacheSizeMetric.inc();
                }
            }
        }
    }

    /** Location service client. */
    public static class LocationInfoServiceClient {
        private static final int LEN_OF_INFO = 100;
        private static final ExecutorService pool =
                Executors.newFixedThreadPool(
                        30,
                        new ThreadFactory() {
                            private final ThreadFactory threadFactory =
                                    Executors.defaultThreadFactory();

                            @Override
                            public Thread newThread(Runnable r) {
                                Thread thread = threadFactory.newThread(r);
                                thread.setName("location-service-client-" + thread.getName());
                                return thread;
                            }
                        });
        private final int responseTimeMin;
        private final int responseTimeMax;

        /**
         * Creates a new enrichment function with a (local) cache that expires after the given
         * number of milliseconds.
         */
        public LocationInfoServiceClient(int responseTimeMin, int responseTimeMax) {
            this.responseTimeMin = responseTimeMin;
            this.responseTimeMax = responseTimeMax;
        }
        /** Gets the info for the given location. */
        public String getLocationInfo(String location) {
            return new LocationInfoSupplier().get();
        }

        /** Asynchronous getter for the info for the given location. */
        public void asyncGetLocationInfo(String location, Consumer<String> callback) {
            CompletableFuture.supplyAsync(new LocationInfoSupplier(), pool)
                    .thenAcceptAsync(
                            callback,
                            org.apache.flink.runtime.concurrent.Executors.directExecutor());
        }

        private class LocationInfoSupplier implements Supplier<String> {
            @Override
            public String get() {
                try {
                    Thread.sleep(RandomUtils.nextInt(responseTimeMin, responseTimeMax));
                } catch (InterruptedException e) {
                    // Swallowing interruption here
                }
                return RandomStringUtils.randomAlphabetic(LEN_OF_INFO);
            }
        }
    }
}
