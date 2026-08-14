package com.subaiqiao.databaseTableAlignment.web.dto;

import java.util.List;

public class AlignmentResponse {
    private final List<String> tips;
    private final List<String> sql;

    public AlignmentResponse(List<String> tips, List<String> sql) {
        this.tips = tips;
        this.sql = sql;
    }

    public List<String> getTips() { return tips; }
    public List<String> getSql() { return sql; }
}
