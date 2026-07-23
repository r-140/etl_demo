package com.analytics.datagen.producer;

import com.analytics.datagen.EventSink;
import com.analytics.datagen.model.DomainEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Kafka event producer implementing EventSink.
 * Publishes domain events to topic-per-customer pattern.
 */
public class KafkaEventProducer implements EventSink {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaEventProducer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final KafkaProducer<String, String> producer;
    private final String topicPrefix;

    public KafkaEventProducer(String bootstrapServers) {
        this(bootstrapServers, "etl.events");
    }

    public KafkaEventProducer(String bootstrapServers, String topicPrefix) {
        this.topicPrefix = topicPrefix;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        this.producer = new KafkaProducer<>(props);
        LOG.info("Kafka producer initialized: bootstrap={}, topicPrefix={}", bootstrapServers, topicPrefix);
    }

    @Override
    public void send(String topicSuffix, DomainEvent event) {
        try {
            String topic = topicPrefix + "." + topicSuffix;
            String key = event.getCustomerId();
            String value = MAPPER.writeValueAsString(event);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Failed to send event to {}: {}", topic, exception.getMessage());
                } else {
                    LOG.debug("Sent event to {} partition={} offset={}", 
                            topic, metadata.partition(), metadata.offset());
                }
            });

        } catch (Exception e) {
            LOG.error("Error serializing event: {}", e.getMessage());
            throw new RuntimeException("Failed to send event", e);
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        LOG.info("Closing Kafka producer...");
        producer.flush();
        producer.close(10, TimeUnit.SECONDS);
    }
}
