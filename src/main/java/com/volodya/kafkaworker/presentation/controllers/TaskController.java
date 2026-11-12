package com.volodya.kafkaworker.presentation.controllers;

import com.volodya.kafkaworker.application.entites.dto.TaskDto;
import com.volodya.kafkaworker.application.services.TaskQueryService;
import com.volodya.kafkaworker.presentation.responses.ErrorResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/tasks")
@RequiredArgsConstructor
@Validated
@Tag(name = "Task API", description = "API для получения состояния задач")
public class TaskController {

    private final TaskQueryService taskQueryService;

    @GetMapping("/{id}")
    @Operation(summary = "Получить статус и результат задачи")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Задача найдена",
            content = @Content(mediaType = "application/json",
                schema = @Schema(implementation = TaskDto.class))),
        @ApiResponse(responseCode = "400", description = "Некорректный идентификатор",
            content = @Content(mediaType = "application/json",
                schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(responseCode = "404", description = "Задача не найдена",
            content = @Content(mediaType = "application/json",
                schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(responseCode = "503", description = "База данных недоступна",
            content = @Content(mediaType = "application/json",
                schema = @Schema(implementation = ErrorResponse.class))),
        @ApiResponse(responseCode = "500", description = "Внутренняя ошибка сервиса",
            content = @Content(mediaType = "application/json",
                schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<TaskDto> getTask(
        @PathVariable("id") @Positive Long id
    ) {
        return ResponseEntity.ok(taskQueryService.getTaskById(id));
    }
}