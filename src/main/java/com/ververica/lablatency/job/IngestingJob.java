package com.ververica.lablatency.job;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.lablatency.event.Measurement;
import com.ververica.lablatency.source.MeasurementSource;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Properties;

/** This is a Flink job. */
public class IngestingJob {

    static class KafkaSerSchema implements KafkaSerializationSchema<Measurement> {

        private Logger logger = LoggerFactory.getLogger(KafkaSerSchema.class);
        private ObjectMapper mapper;
        private String topic;

        public KafkaSerSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public void open(SerializationSchema.InitializationContext context) throws Exception {
            this.mapper = new ObjectMapper();
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Measurement measurement, @Nullable Long aLong) {
            try {
                return new ProducerRecord<>(
                        this.topic,
                        null,
                        System.currentTimeMillis(),
                        null,
                        this.mapper.writeValueAsBytes(measurement));
            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize measurement: " + e.getMessage());
                return null;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Logger logger = LoggerFactory.getLogger(IngestingJob.class);

        final ParameterTool params = ParameterTool.fromArgs(args);

        final String jobName = params.get("job-name", IngestingJob.class.getSimpleName());
        final String kafkaAddress = params.get("kafka", "localhost:9092");
        final String topic = params.get("topic", "lablatency");

        // when spikeInterval==1, every minute is a spike, it actually means there is no spikes
        int spikeInterval = params.getInt("spike-interval", 1);
        int waitMicro = params.getInt("wait-micro", 0);

        // Flink environment setup
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Flink check/save point setting
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.disableOperatorChaining(); // to check throughput

        // Properties for Kafka
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("bootstrap.servers", kafkaAddress);
        kafkaProducerProps.setProperty("transaction.timeout.ms", "600000");

        FlinkKafkaProducer<Measurement> producer =
                new FlinkKafkaProducer<Measurement>(
                        topic,
                        new KafkaSerSchema(topic),
                        kafkaProducerProps,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        env.addSource(new MeasurementSource(spikeInterval, waitMicro)).addSink(producer);

        env.execute(jobName);
    }
}
