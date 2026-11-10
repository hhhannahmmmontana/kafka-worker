package com.volodya.kafkaworker.infrastructure.consuming.configuration;

import com.volodya.kafkaworker.application.exceptions.InvalidTaskMessageException;
import com.volodya.kafkaworker.infrastructure.consuming.dto.requests.TaskRequest;
import jakarta.validation.ConstraintViolationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.listener.concurrency:}")
    private int concurrency;

    @Value("${application.kafka.error-handling.backoff-ms}")
    private long errorBackoffMs;

    @Value("${application.kafka.error-handling.max-retries}")
    private int errorMaxRetries;

    @Bean
    public ConsumerFactory<UUID, TaskRequest> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var jsonDeserializer = new JacksonJsonDeserializer<>(TaskRequest.class, false);
        return new DefaultKafkaConsumerFactory<>(
            props,
            new UUIDDeserializer(),
            new ErrorHandlingDeserializer<>(jsonDeserializer)
        );
    }

    @Bean
    public ProducerFactory<UUID, Object> dltProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        Map<Class<?>, Serializer<?>> serializers = new HashMap<>();
        serializers.put(byte[].class, new ByteArraySerializer());
        serializers.put(TaskRequest.class, new JacksonJsonSerializer<>());

        return new DefaultKafkaProducerFactory<>(
            props,
            new UUIDSerializer(),
            new DelegatingByTypeSerializer(serializers)
        );
    }

    @Bean
    public KafkaTemplate<UUID, Object> dltKafkaTemplate(
        ProducerFactory<UUID, Object> dltProducerFactory
    ) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
        KafkaTemplate<UUID, Object> dltKafkaTemplate
    ) {
        DeadLetterPublishingRecoverer recoverer =
            new DeadLetterPublishingRecoverer(
                dltKafkaTemplate,
                (record, _) -> new TopicPartition(
                    record.topic() + ".DLT", record.partition()
                )
            );
        recoverer.setFailIfSendResultIsError(true);

        DefaultErrorHandler handler = new DefaultErrorHandler(
            recoverer, new FixedBackOff(errorBackoffMs, errorMaxRetries)
        );
        handler.addNotRetryableExceptions(
            ConstraintViolationException.class,
            InvalidTaskMessageException.class
        );
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<UUID, TaskRequest>
    kafkaListenerContainerFactory(
        ConsumerFactory<UUID, TaskRequest> consumerFactory,
        DefaultErrorHandler kafkaErrorHandler
    ) {
        if (concurrency < 1) {
            throw new IllegalArgumentException("Kafka concurrency value must be positive value");
        }

        var factory = new ConcurrentKafkaListenerContainerFactory<UUID, TaskRequest>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }
}
