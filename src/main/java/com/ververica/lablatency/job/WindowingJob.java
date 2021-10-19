package com.ververica.lablatency.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.lablatency.event.Measurement;
import com.ververica.lablatency.event.MeasurementRecord;
import com.ververica.lablatency.event.WindowedMeasurement;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/** WindowingJob */
public class WindowingJob {

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

        final String jobName = params.get("job-name", WindowingJob.class.getSimpleName());
        final String kafkaAddress = params.get("kafka", "localhost:9092");
        final String topic = params.get("topic", "lablatency");
        final String group = params.get("group", "lablatency");
        final String startOffset = params.get("offset", "latest");

        final int slideSize = params.getInt("slide-size", 10);
        final int outOfOrderness = params.getInt("out-of-orderness", 250);

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", kafkaAddress);
        kafkaConsumerProps.setProperty("group.id", group);
        kafkaConsumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, startOffset);
        FlinkKafkaConsumer<MeasurementRecord> consumer =
                new FlinkKafkaConsumer<>(topic, new KafkaDeSerSchema(), kafkaConsumerProps);
        // start from the latest message
        consumer.setStartFromLatest();

        DataStream<Measurement> sourceStream =
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

        SingleOutputStreamOperator<WindowedMeasurement> aggregatedPerLocation =
                sourceStream
                        .keyBy(Measurement::getLocation)
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.of(1, TimeUnit.MINUTES),
                                        Time.of(slideSize, TimeUnit.SECONDS)))
                        .aggregate(
                                new MeasurementAggregateFunction(),
                                new MeasurementProcessWindowFunction())
                        .name("MainOperator:Window")
                        .uid("MainOperator:Window");

        aggregatedPerLocation
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
            extends RichFlatMapFunction<MeasurementRecord, Measurement> {

        private static final long serialVersionUID = 1L;
        private static Logger logger = LoggerFactory.getLogger(MeasurementDeserializer.class);

        private ObjectMapper objectMapper;

        @Override
        public void open(final Configuration parameters) throws Exception {
            super.open(parameters);
            this.objectMapper = new ObjectMapper();
        }

        @Override
        public void flatMap(final MeasurementRecord kafkaRecord, final Collector<Measurement> out) {
            final Measurement measurement;
            try {
                measurement =
                        this.objectMapper.readValue(kafkaRecord.getValue(), Measurement.class);
            } catch (IOException e) {
                logger.error("Failed to deserialize: " + e.getLocalizedMessage());
                return;
            }
            out.collect(measurement);
        }
    }

    /** Incrementally aggregate measurements. */
    public static class MeasurementAggregateFunction
            implements AggregateFunction<Measurement, Tuple2<Long, Double>, Tuple2<Long, Double>> {

        @Override
        public Tuple2<Long, Double> createAccumulator() {
            return new Tuple2<>(0L, 0.0);
        }

        @Override
        public Tuple2<Long, Double> add(Measurement measurement, Tuple2<Long, Double> accumulator) {
            return new Tuple2<>(accumulator.f0 + 1, accumulator.f1 + measurement.getValue());
        }

        @Override
        public Tuple2<Long, Double> getResult(Tuple2<Long, Double> accumulator) {
            return new Tuple2<>(accumulator.f0, accumulator.f1);
        }

        @Override
        public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    /** ProcessWindowFunction produces WindowedMeasurement. */
    public static class MeasurementProcessWindowFunction
            extends ProcessWindowFunction<
                    Tuple2<Long, Double>, WindowedMeasurement, String, TimeWindow> {

        private static final long serialVersionUID = 1L;
        private static final int EVENT_TIME_LAG_WINDOW_SIZE = 10_000;

        private transient DescriptiveStatisticsHistogram eventTimeLag;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            eventTimeLag =
                    getRuntimeContext()
                            .getMetricGroup()
                            .histogram(
                                    "eventTimeLag",
                                    new DescriptiveStatisticsHistogram(EVENT_TIME_LAG_WINDOW_SIZE));
        }

        @Override
        public void process(
                final String location,
                final Context context,
                final Iterable<Tuple2<Long, Double>> input,
                final Collector<WindowedMeasurement> out) {

            WindowedMeasurement windowedMeasurement = new WindowedMeasurement();
            Tuple2<Long, Double> aggregated = input.iterator().next();

            windowedMeasurement.setEventsPerWindow(aggregated.f0);
            windowedMeasurement.setSumPerWindow(aggregated.f1);

            final TimeWindow window = context.window();
            windowedMeasurement.setWindow(window);
            windowedMeasurement.setLocation(location);

            eventTimeLag.update(System.currentTimeMillis() - window.getEnd());
            out.collect(windowedMeasurement);
        }
    }
}
