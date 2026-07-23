package com.subaiqiao.databaseTableAlignment.strategy;

import com.subaiqiao.databaseTableAlignment.pojo.Columns;
import com.subaiqiao.databaseTableAlignment.pojo.Comments;
import com.subaiqiao.databaseTableAlignment.pojo.Table;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @author Caozhaoyu
 * @date 2025年10月11日 17:51
 */
public class DatabaseContext {

    private DatabaseStrategy strategy;

    public void setStrategy(DatabaseStrategy strategy) {
        this.strategy = strategy;
    }

    public DatabaseStrategy getStrategy() {
        return strategy;
    }

    public Connection getConnection(String host, String port, String user, String password, String schema) {
        return strategy.connection(host, port, user, password, schema);
    }

    public void close(Connection connection) {
        strategy.close(connection);
    }

    public String getDataType(Columns column) {
        return strategy.getDataType(column);
    }

    public List<Table> getTables(String schema, Connection connection) {
        return strategy.getTables(schema, connection);
    }

    public List<Comments> getComments(String schema, Connection connection) {
        return strategy.getComments(schema, connection);
    }

    public Map<String, List<Columns>> getColumnsMap(String schema, Connection connection) {
        return strategy.getColumnsMap(schema, connection);
    }

    public List<Columns> getColumns(String schema, String tableName, Connection connection) {
        return strategy.getColumns(schema, tableName, connection);
    }

    public String generateCommentsSql(String schema, String tableName, Columns column, List<Comments> commentsList, List<Comments> commentsList2) {
        return strategy.generateCommentsSql(schema, tableName, column, commentsList, commentsList2);
    }

    public String generateCommentsSql(String schema, String tableName, List<Columns> list, List<Comments> commentsList, List<Comments> commentsList2) {
        return strategy.generateCommentsSql(schema, tableName, list, commentsList, commentsList2);
    }

    public String generateCreateSql(String schema, String tableName, List<Columns> list) {
        return strategy.generateCreateSql(schema, tableName, list);
    }

    public String generateUpdateSql(String schema, String tableName, Columns column) {
        return strategy.generateUpdateSql(schema, tableName, column);
    }

}
