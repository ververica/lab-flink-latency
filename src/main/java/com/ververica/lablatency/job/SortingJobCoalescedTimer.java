package com.ververica.lablatency.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/** SortingJob with coalesced timers. */
public class SortingJobCoalescedTimer {
    private static final Logger LOG = LoggerFactory.getLogger(SortingJobCoalescedTimer.class);

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        LOG.info("params: " + params.getProperties());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final String jobName =
                params.get("job-name", SortingJobCoalescedTimer.class.getSimpleName());
        final String kafkaAddress = params.get("kafka", "localhost:9092");
        final String topic = params.get("topic", "lablatency");
        final String group = params.get("group", "lablatency");

        final int outOfOrderness = params.getInt("out-of-orderness", 250);

        final int roundTimerTo = params.getInt("round-timer-to", 100);

        final boolean useOneMapper = params.getBoolean("use-one-mapper", true);
        final boolean forceKryo = params.getBoolean("force-kryo", false);

        if (forceKryo) {
            env.getConfig().enableForceKryo();
        }

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", kafkaAddress);
        kafkaConsumerProps.setProperty("group.id", group);
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
                        .process(new SortFunction(roundTimerTo))
                        .name("Sort")
                        .uid("Sort")
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple2<Measurement, Long>>forMonotonousTimestamps()
                                        .withTimestampAssigner((element, timestamp) -> element.f1)
                                        .withIdleness(Duration.ofSeconds(1)))
                        .name("Watermarks2")
                        .uid("Watermarks2")
                        .map(new MapMeasurement())
                        .name("MainOperator:After2ndWatermark")
                        .uid("MainOperator:After2ndWatermark");

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
        private static final Logger LOG =
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
                LOG.error("Failed to deserialize: " + e.getLocalizedMessage());
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
        private static final Logger LOG =
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
                LOG.error("Failed to deserialize: " + e.getLocalizedMessage());
                return;
            }
            out.collect(Tuple2.of(measurement, kafkaRecord.getTimestamp()));
        }
    }

    private static class MeasurementByTimeComparator
            implements Comparator<Tuple2<Measurement, Long>> {
        @Override
        public int compare(Tuple2<Measurement, Long> o1, Tuple2<Measurement, Long> o2) {
            return Long.compare(o1.f1, o2.f1);
        }
    }

    /** SortFunction with timer coalescing: round timer to {@code roundTo} */
    public static class SortFunction
            extends KeyedProcessFunction<
                    Integer, Tuple2<Measurement, Long>, Tuple2<Measurement, Long>> {

        private static final Logger LOG = LoggerFactory.getLogger(SortFunction.class);
        private ListState<Tuple2<Measurement, Long>> listState;
        private final int roundTo;

        public SortFunction(int roundTo) {
            this.roundTo = roundTo;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ListStateDescriptor<Tuple2<Measurement, Long>> desc =
                    new ListStateDescriptor<>(
                            "events",
                            TypeInformation.of(new TypeHint<Tuple2<Measurement, Long>>() {}));
            listState = getRuntimeContext().getListState(desc);
        }

        @Override
        public void processElement(
                Tuple2<Measurement, Long> value,
                Context ctx,
                Collector<Tuple2<Measurement, Long>> out)
                throws Exception {

            TimerService timerService = ctx.timerService();
            long currentTimestamp = ctx.timestamp();
            long currentWatermark = timerService.currentWatermark();

            if (currentTimestamp > currentWatermark) {
                listState.add(value);
                if (this.roundTo == 0) {
                    timerService.registerEventTimeTimer(currentWatermark + 1);
                } else {
                    timerService.registerEventTimeTimer(
                            currentTimestamp / this.roundTo * this.roundTo + this.roundTo);
                }
            }
        }

        @Override
        public void onTimer(
                long timestamp, OnTimerContext ctx, Collector<Tuple2<Measurement, Long>> out)
                throws Exception {

            long currentWatermark = ctx.timerService().currentWatermark();

            ArrayList<Tuple2<Measurement, Long>> list = new ArrayList<>();
            listState.get().forEach(list::add);
            LOG.info("Sorting list with size: " + list.size());
            list.sort(new MeasurementByTimeComparator());

            int index = 0;
            for (Tuple2<Measurement, Long> event : list) {
                // this requires re-assign timestamps and watermarks. Otherwise, the emitted events
                // here are all having the same timestamp as this timer.
                if (event != null && event.f1 <= currentWatermark) {
                    out.collect(event);
                    index++;
                } else {
                    break;
                }
            }
            list.subList(0, index).clear();
            listState.update(list);
        }
    }

    /** This is class is used to calculate eventTimeLag after the second watermark are added */
    public static class MapMeasurement
            extends RichMapFunction<Tuple2<Measurement, Long>, Tuple2<Measurement, Long>> {

        private transient DescriptiveStatisticsHistogram eventTimeLag;
        private static final int EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

        @Override
        public void open(final Configuration parameters) throws Exception {
            eventTimeLag =
                    getRuntimeContext()
                            .getMetricGroup()
                            .histogram(
                                    "eventTimeLag",
                                    new DescriptiveStatisticsHistogram(EVENT_TIME_LAG_WINDOW_SIZE));
        }

        @Override
        public Tuple2<Measurement, Long> map(Tuple2<Measurement, Long> value) throws Exception {
            eventTimeLag.update(System.currentTimeMillis() - value.f1);
            return value;
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
                Context ctx,
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
