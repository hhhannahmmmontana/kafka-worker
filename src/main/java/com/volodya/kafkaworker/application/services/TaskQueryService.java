package com.volodya.kafkaworker.application.services;

import com.volodya.kafkaworker.application.entites.dto.TaskDto;
import com.volodya.kafkaworker.application.exceptions.TaskNotFoundException;
import com.volodya.kafkaworker.persistence.repositories.TaskEntityJpaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TaskQueryService {

    private final TaskEntityJpaRepository taskRepository;

    @Transactional(readOnly = true)
    public TaskDto getTaskById(long id) {
        var taskEntity = taskRepository.findById(id)
            .orElseThrow(() -> new TaskNotFoundException(id));
        return new TaskDto(
            taskEntity.getId(),
            taskEntity.getEventId(),
            taskEntity.getName(),
            taskEntity.getDuration(),
            taskEntity.getStatus(),
            taskEntity.getProgress(),
            taskEntity.getResult(),
            taskEntity.getCreatedAt(),
            taskEntity.getStartedAt(),
            taskEntity.getFinishedAt()
        );
    }
}
