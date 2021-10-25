/*
 * Copyright 2021 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ververica.lablatency.job;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
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

        private static final Logger LOG = LoggerFactory.getLogger(KafkaSerSchema.class);
        private ObjectMapper mapper;
        private final String topic;

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
                LOG.error("Failed to serialize measurement: " + e.getMessage());
                return null;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final String jobName = params.get("job-name", IngestingJob.class.getSimpleName());
        final String kafkaAddress = params.get("kafka", "localhost:9092");
        final String topic = params.get("topic", "lablatency");

        // when spikeInterval==1, every minute is a spike, it actually means there is no spikes
        int spikeInterval = params.getInt("spike-interval", 1);
        int waitMicro = params.getInt("wait-micro", 0);

        // Flink environment setup
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining(); // to check throughput

        // Properties for Kafka
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("bootstrap.servers", kafkaAddress);
        kafkaProducerProps.setProperty("transaction.timeout.ms", "600000");

        FlinkKafkaProducer<Measurement> producer =
                new FlinkKafkaProducer<>(
                        topic,
                        new KafkaSerSchema(topic),
                        kafkaProducerProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        env.addSource(new MeasurementSource(spikeInterval, waitMicro)).addSink(producer);

        env.execute(jobName);
    }
}
