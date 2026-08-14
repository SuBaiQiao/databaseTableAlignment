package com.subaiqiao.databaseTableAlignment.service;

import com.subaiqiao.databaseTableAlignment.pojo.Columns;
import com.subaiqiao.databaseTableAlignment.pojo.Comments;
import com.subaiqiao.databaseTableAlignment.pojo.Table;
import com.subaiqiao.databaseTableAlignment.strategy.DatabaseEnum;
import com.subaiqiao.databaseTableAlignment.strategy.DatabaseStrategy;
import com.subaiqiao.databaseTableAlignment.strategy.DatabaseStrategyFactory;
import com.subaiqiao.databaseTableAlignment.web.dto.AlignmentResponse;
import com.subaiqiao.databaseTableAlignment.web.dto.CompareRequest;
import com.subaiqiao.databaseTableAlignment.web.dto.ConnectionConfig;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class AlignmentService {
    private static final Set<DatabaseEnum> SUPPORTED_TYPES = Set.of(DatabaseEnum.DM, DatabaseEnum.KING_BASE);

    public List<String> supportedDatabaseTypes() {
        return SUPPORTED_TYPES.stream().map(Enum::name).sorted().toList();
    }

    public AlignmentResponse preview(CompareRequest request) {
        validate(request);
        DatabaseEnum type;
        try {
            type = DatabaseEnum.valueOf(request.getDatabaseType().trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("不支持的数据库类型，仅支持 DM 或 KING_BASE");
        }
        if (!SUPPORTED_TYPES.contains(type)) {
            throw new IllegalArgumentException("不支持的数据库类型，仅支持 DM 或 KING_BASE");
        }

        DatabaseStrategy strategy = DatabaseStrategyFactory.getStrategy(type);
        Connection sourceConnection = null;
        Connection targetConnection = null;
        try {
            sourceConnection = connect(strategy, request.getSource(), request.getSource().getSchema());
            targetConnection = connect(strategy, request.getTarget(), request.getTarget().getSchema());
            Snapshot source = snapshot(strategy, request.getSource().getSchema(), sourceConnection);
            Snapshot target = snapshot(strategy, request.getTarget().getSchema(), targetConnection);
            return compare(strategy, request.getSource().getSchema(), request.getTarget().getSchema(), source, target);
        } finally {
            close(strategy, sourceConnection);
            close(strategy, targetConnection);
        }
    }

    private Connection connect(DatabaseStrategy strategy, ConnectionConfig config, String schema) {
        try {
            return strategy.connection(config.getHost().trim(), config.getPort().trim(),
                    config.getUsername().trim(), config.getPassword(), schema.trim());
        } catch (RuntimeException ex) {
            throw new IllegalStateException("数据库连接失败: " + config.getHost() + ":" + config.getPort(), ex);
        }
    }

    private Snapshot snapshot(DatabaseStrategy strategy, String schema, Connection connection) {
        List<Table> tables = strategy.getTables(schema, connection);
        Map<String, List<Columns>> columns = strategy.getColumnsMap(schema, connection);
        tables.forEach(table -> table.setColumns(columns.getOrDefault(table.getTableName(), Collections.emptyList())));
        return new Snapshot(tables, strategy.getComments(schema, connection));
    }

    private AlignmentResponse compare(DatabaseStrategy strategy, String sourceSchema, String targetSchema,
                                      Snapshot source, Snapshot target) {
        Map<String, Table> sourceMap = source.tables().stream().collect(Collectors.toMap(t -> key(t.getTableName()), t -> t, (a, b) -> a));
        Map<String, Table> targetMap = target.tables().stream().collect(Collectors.toMap(t -> key(t.getTableName()), t -> t, (a, b) -> a));
        Map<String, Comments> sourceComments = comments(source.comments());
        Map<String, Comments> targetComments = comments(target.comments());
        List<String> tips = new ArrayList<>();
        List<String> sql = new ArrayList<>();

        targetMap.forEach((tableKey, targetTable) -> {
            String tableName = targetTable.getTableName();
            List<Columns> expected = Optional.ofNullable(targetTable.getColumns()).orElseGet(Collections::emptyList);
            Table actualTable = sourceMap.get(tableKey);
            if (actualTable == null) {
                tips.add("缺少表：\t" + tableName + "\t字段：" + expected.stream().map(Columns::getColumnName).collect(Collectors.joining("、")));
                sql.add(strategy.generateCreateSql(sourceSchema, tableName, expected));
            } else {
                Set<String> actualColumns = Optional.ofNullable(actualTable.getColumns()).orElseGet(Collections::emptyList)
                        .stream().map(c -> key(c.getColumnName())).collect(Collectors.toSet());
                List<Columns> missing = expected.stream().filter(c -> !actualColumns.contains(key(c.getColumnName()))).toList();
                if (!missing.isEmpty()) {
                    tips.add("表" + tableName + "缺少字段：" + missing.stream().map(Columns::getColumnName).collect(Collectors.joining("、")));
                    missing.forEach(column -> sql.add(strategy.generateUpdateSql(sourceSchema, tableName, column)));
                }
            }
            String commentSql = commentsSql(strategy, sourceSchema, tableName, expected, sourceComments, targetComments);
            if (!commentSql.isEmpty()) sql.add(commentSql);
        });
        return new AlignmentResponse(tips, sql);
    }

    private String commentsSql(DatabaseStrategy strategy, String schema, String table, List<Columns> columns,
                               Map<String, Comments> actual, Map<String, Comments> expected) {
        StringBuilder result = new StringBuilder();
        for (Columns column : columns) {
            String key = key(table) + "." + key(column.getColumnName());
            Comments wanted = expected.get(key);
            if (wanted == null) continue;
            Comments existing = actual.get(key);
            if (existing == null || !Objects.equals(existing.getComments(), wanted.getComments())) {
                result.append(String.format("comment%non column %s.%s.%s is '%s';%n", schema, table, column.getColumnName(), wanted.getComments()));
            }
        }
        return result.toString();
    }

    private Map<String, Comments> comments(List<Comments> list) {
        Map<String, Comments> result = new HashMap<>();
        for (Comments comment : Optional.ofNullable(list).orElseGet(Collections::emptyList)) {
            result.put(key(comment.getTableName()) + "." + key(comment.getColumnName()), comment);
        }
        return result;
    }

    private String key(String value) { return value == null ? "" : value.toUpperCase(Locale.ROOT); }

    private void validate(CompareRequest request) {
        if (request == null || blank(request.getDatabaseType()) || request.getSource() == null || request.getTarget() == null) {
            throw new IllegalArgumentException("数据库类型、源数据库和基准数据库均不能为空");
        }
        validateConnection(request.getSource(), "源数据库");
        validateConnection(request.getTarget(), "基准数据库");
    }

    private void validateConnection(ConnectionConfig config, String label) {
        if (blank(config.getSchema()) || !config.getSchema().matches("[A-Za-z0-9_$#]+")) {
            throw new IllegalArgumentException(label + " Schema 只允许字母、数字、下划线、$ 和 #");
        }
        if (blank(config.getHost()) || blank(config.getPort()) || blank(config.getUsername()) || config.getPassword() == null) {
            throw new IllegalArgumentException(label + "连接信息不完整");
        }
        if (!config.getPort().matches("\\d{1,5}")) throw new IllegalArgumentException(label + "端口格式不正确");
    }

    private boolean blank(String value) { return value == null || value.isBlank(); }
    private void close(DatabaseStrategy strategy, Connection connection) { if (connection != null) strategy.close(connection); }
    private record Snapshot(List<Table> tables, List<Comments> comments) {}
}
