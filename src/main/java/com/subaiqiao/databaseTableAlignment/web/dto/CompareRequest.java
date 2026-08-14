package com.subaiqiao.databaseTableAlignment.web.dto;

public class CompareRequest {
    private String databaseType;
    private String schema;
    private ConnectionConfig source;
    private ConnectionConfig target;

    public String getDatabaseType() { return databaseType; }
    public void setDatabaseType(String databaseType) { this.databaseType = databaseType; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public ConnectionConfig getSource() { return source; }
    public void setSource(ConnectionConfig source) { this.source = source; }
    public ConnectionConfig getTarget() { return target; }
    public void setTarget(ConnectionConfig target) { this.target = target; }
}
