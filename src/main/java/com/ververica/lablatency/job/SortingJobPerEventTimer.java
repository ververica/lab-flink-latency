package com.ververica.lablatency.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.lablatency.event.Measurement;
import com.ververica.lablatency.event.MeasurementRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/** SortingJob with per event timers. */
public class SortingJobPerEventTimer {

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

        final String jobName =
                params.get("job-name", SortingJobPerEventTimer.class.getSimpleName());
        final String kafkaAddress = params.get("kafka", "localhost:9092");
        final String topic = params.get("topic", "lablatency");
        final String group = params.get("group", "lablatency");
        final String startOffset = params.get("offset", "latest");

        final int outOfOrderness = params.getInt("out-of-orderness", 250);

        final boolean useOneMapper = params.getBoolean("use-one-mapper", true);
        final boolean forceKryo = params.getBoolean("force-kryo", false);

        if (forceKryo) {
            env.getConfig().enableForceKryo();
        }

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
                        .flatMap(
                                useOneMapper
                                        ? new MeasurementDeserializerOneGlobalMapper()
                                        : new MeasurementDeserializerOneMapperPerEvent())
                        .name("Deserialization")
                        .uid("Deserialization");

        DataStream<Tuple2<Measurement, Long>> sortedStream =
                sourceStream
                        .keyBy(x -> x.f0.getSensorId())
                        .process(new SortFunction())
                        .name("MainOperator:Sort")
                        .uid("MainOperator:Sort");

        DataStreamUtils.reinterpretAsKeyedStream(
                        sortedStream
                                .map(new FixSensorsFunction(Tuple2.of("Berlin", 1.0)))
                                .name("Fix defective sensors")
                                .uid("Fix defective sensors"),
                        x -> x.f0.getSensorId())
                .process(new MovingAverageSensors())
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

    /** Deserializes MeasurementRecord into Measurement: create one ObjectMapper per event */
    public static class MeasurementDeserializerOneMapperPerEvent
            extends RichFlatMapFunction<MeasurementRecord, Tuple2<Measurement, Long>> {

        private static final long serialVersionUID = 1L;
        private static Logger logger =
                LoggerFactory.getLogger(MeasurementDeserializerOneMapperPerEvent.class);

        @Override
        public void open(final Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void flatMap(
                final MeasurementRecord kafkaRecord,
                final Collector<Tuple2<Measurement, Long>> out) {
            final Measurement measurement;
            try {
                measurement =
                        new ObjectMapper().readValue(kafkaRecord.getValue(), Measurement.class);
            } catch (IOException e) {
                logger.error("Failed to deserialize: " + e.getLocalizedMessage());
                return;
            }
            out.collect(Tuple2.of(measurement, kafkaRecord.getTimestamp()));
        }
    }

    /**
     * Deserializes MeasurementRecord into Measurement: create one global ObjectMapper per operator
     * instance
     */
    public static class MeasurementDeserializerOneGlobalMapper
            extends RichFlatMapFunction<MeasurementRecord, Tuple2<Measurement, Long>> {

        private static final long serialVersionUID = 1L;
        private static Logger logger =
                LoggerFactory.getLogger(MeasurementDeserializerOneGlobalMapper.class);

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
            out.collect(Tuple2.of(measurement, kafkaRecord.getTimestamp()));
        }
    }

    /** SortFunction without timer coealescing. */
    public static class SortFunction
            extends KeyedProcessFunction<
                    Integer, Tuple2<Measurement, Long>, Tuple2<Measurement, Long>> {

        private static final Logger logger = LoggerFactory.getLogger(SortFunction.class);
        private ListState<Tuple2<Measurement, Long>> listState;

        private transient DescriptiveStatisticsHistogram eventTimeLag;
        private static final int EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ListStateDescriptor<Tuple2<Measurement, Long>> desc =
                    new ListStateDescriptor<Tuple2<Measurement, Long>>(
                            "events",
                            TypeInformation.of(new TypeHint<Tuple2<Measurement, Long>>() {}));
            listState = getRuntimeContext().getListState(desc);

            eventTimeLag =
                    getRuntimeContext()
                            .getMetricGroup()
                            .histogram(
                                    "eventTimeLag",
                                    new DescriptiveStatisticsHistogram(EVENT_TIME_LAG_WINDOW_SIZE));
        }

        @Override
        public void processElement(
                Tuple2<Measurement, Long> value,
                Context ctx,
                Collector<Tuple2<Measurement, Long>> out)
                throws Exception {

            TimerService timerService = ctx.timerService();
            long currentTimestamp = ctx.timestamp();

            if (currentTimestamp > timerService.currentWatermark()) {
                listState.add(value);
                timerService.registerEventTimeTimer(currentTimestamp);
            }
        }

        @Override
        public void onTimer(
                long timestamp, OnTimerContext ctx, Collector<Tuple2<Measurement, Long>> out)
                throws Exception {

            ArrayList<Tuple2<Measurement, Long>> list = new ArrayList<>();
            listState
                    .get()
                    .forEach(
                            event -> {
                                // we do not emit all events earlier than watermark because
                                // otherwise
                                // those emitted events will all have the same timestamp as this
                                // timer
                                if (event.f1 == timestamp) {
                                    eventTimeLag.update(System.currentTimeMillis() - timestamp);
                                    out.collect(event);
                                } else {
                                    list.add(event);
                                }
                            });
            listState.update(list);
        }
    }

    /**
     * Implements an exponentially moving average with a coefficient of <code>0.5</code>, i.e.
     *
     * <ul>
     *   <li><code>avg[0] = value[0]</code> (not forwarded to the next stream)
     *   <li><code>avg[i] = avg[i-1] * 0.5 + value[i] * 0.5</code> (for <code>i > 0</code>)
     * </ul>
     *
     * <p>See <a href="https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">
     * https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average</a>
     */
    private static class MovingAverageSensors
            extends KeyedProcessFunction<
                    Integer, Tuple2<Measurement, Long>, Tuple3<Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Double> movingAverage;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            movingAverage =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("movingAverage", Types.DOUBLE));
        }

        @Override
        public void processElement(
                Tuple2<Measurement, Long> value,
                KeyedProcessFunction<
                                        Integer,
                                        Tuple2<Measurement, Long>,
                                        Tuple3<Integer, Double, Long>>
                                .Context
                        ctx,
                Collector<Tuple3<Integer, Double, Long>> out)
                throws Exception {

            Double last = movingAverage.value();
            if (last != null) {
                last = (last + value.f0.getValue()) / 2.0;
                movingAverage.update(last);

                // do not forward the first value (it only stands alone)
                out.collect(Tuple3.of(ctx.getCurrentKey(), last, ctx.timestamp()));
            } else {
                movingAverage.update(value.f0.getValue());
            }
        }
    }

    private static class FixSensorsFunction
            implements MapFunction<Tuple2<Measurement, Long>, Tuple2<Measurement, Long>> {

        private final Map<String, Double> locationCorrections = new HashMap<>();

        @SafeVarargs
        public FixSensorsFunction(Tuple2<String, Double>... locationCorrections) {
            for (Tuple2<String, Double> locationCorrection : locationCorrections) {
                this.locationCorrections.put(locationCorrection.f0, locationCorrection.f1);
            }
        }

        @Override
        public Tuple2<Measurement, Long> map(Tuple2<Measurement, Long> value) throws Exception {
            if (locationCorrections.containsKey(value.f0.getLocation())) {
                return Tuple2.of(
                        new Measurement(
                                value.f0.getSensorId(),
                                value.f0.getValue()
                                        + locationCorrections.get(value.f0.getLocation()),
                                value.f0.getLocation(),
                                value.f0.getMeasurementInformation()),
                        value.f1);
            } else {
                return value;
            }
        }
    }
}
