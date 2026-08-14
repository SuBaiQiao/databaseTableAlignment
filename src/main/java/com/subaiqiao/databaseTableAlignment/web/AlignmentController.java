package com.subaiqiao.databaseTableAlignment.web;

import com.subaiqiao.databaseTableAlignment.service.AlignmentService;
import com.subaiqiao.databaseTableAlignment.web.dto.AlignmentResponse;
import com.subaiqiao.databaseTableAlignment.web.dto.CompareRequest;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/align")
public class AlignmentController {
    private final AlignmentService service;

    public AlignmentController(AlignmentService service) {
        this.service = service;
    }

    @GetMapping("/database-types")
    public List<String> databaseTypes() {
        return service.supportedDatabaseTypes();
    }

    @PostMapping("/preview")
    public AlignmentResponse preview(@RequestBody CompareRequest request) {
        return service.preview(request);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> badRequest(IllegalArgumentException ex) {
        return Map.of("error", ex.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    public Map<String, String> databaseError(IllegalStateException ex) {
        return Map.of("error", ex.getMessage());
    }
}
