package com.volodya.kafkaworker.brokers;

import com.volodya.kafkaworker.application.exceptions.InvalidTaskMessageException;
import com.volodya.kafkaworker.application.services.TaskRegistrationService;
import com.volodya.kafkaworker.brokers.dto.requests.TaskRequest;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class TaskKafkaListener {

    private final TaskRegistrationService taskRegistrationService;
    private final Validator validator;

    @KafkaListener(
        topics = "${kafka.topic}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void receive(ConsumerRecord<UUID, TaskRequest> record) {
        if (record.key() == null) {
            throw new InvalidTaskMessageException("Message does not contain an event id");
        }
        var request = record.value();
        if (request == null) {
            throw new InvalidTaskMessageException("Task message doesn't contain a task");
        }

        Set<ConstraintViolation<TaskRequest>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }

        if (!Objects.equals(record.key(), request.eventId())) {
            throw new InvalidTaskMessageException("Message's key is not equal to event id");
        }

        taskRegistrationService.register(request);
    }
}
