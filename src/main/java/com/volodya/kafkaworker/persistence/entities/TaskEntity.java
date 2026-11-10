package com.volodya.kafkaworker.persistence.entities;

import com.volodya.kafkaworker.persistence.entities.enums.TaskStatus;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "tasks")
@Getter
@Setter(AccessLevel.PROTECTED)
@NoArgsConstructor
public class TaskEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private Long duration;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    @Setter(AccessLevel.PUBLIC)
    private TaskStatus status = TaskStatus.NEW;

    @Column(nullable = false)
    @Setter(AccessLevel.PUBLIC)
    private Integer progress = 0;

    @Column(columnDefinition = "text")
    @Setter(AccessLevel.PUBLIC)
    private @Nullable String result;

    @Column(name = "execution_id")
    @Setter(AccessLevel.PUBLIC)
    private @Nullable UUID executionId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "started_at")
    @Setter(AccessLevel.PUBLIC)
    private @Nullable Instant startedAt;

    @Column(name = "finished_at")
    @Setter(AccessLevel.PUBLIC)
    private @Nullable Instant finishedAt;

    @Version
    @Column(nullable = false)
    private Long version;
}