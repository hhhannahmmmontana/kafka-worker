package com.volodya.kafkaworker.application.services;

import com.volodya.kafkaworker.application.exceptions.InvalidTaskMessageException;
import com.volodya.kafkaworker.brokers.dto.requests.TaskRequest;
import com.volodya.kafkaworker.persistence.entities.TaskEntity;
import com.volodya.kafkaworker.persistence.repositories.TaskEntityJpaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TaskRegistrationService {

    private final TaskEntityJpaRepository taskRepository;

    @Transactional
    public void register(TaskRequest request) {
        String normalizedName = request.name().strip();
        int inserted = taskRepository.insertIfAbsent(
            request.eventId(),
            normalizedName,
            request.duration()
        );

        if (inserted == 0) { // idk why we would even need this...
            TaskEntity existing = taskRepository.findByEventId(request.eventId())
                .orElseThrow(() -> new IllegalStateException(
                    "No record with event id " + request.eventId() + " is present"
                ));
            if (!existing.getName().equals(normalizedName) || !existing.getDuration().equals(request.duration())) {
                throw new InvalidTaskMessageException("Other task with the same event id already registered");
            }
        }
    }
}
