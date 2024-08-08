package com.subaiqiao.databaseTableAlignment;

import java.util.List;

public class Table {
    private String tableName;

    private List<Columns> columns;

    public List<Columns> getColumns() {
        return columns;
    }

    public void setColumns(List<Columns> columns) {
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
