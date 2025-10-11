package com.subaiqiao.databaseTableAlignment.strategy;


import com.subaiqiao.databaseTableAlignment.pojo.Columns;
import com.subaiqiao.databaseTableAlignment.pojo.Comments;
import com.subaiqiao.databaseTableAlignment.pojo.Table;

import java.sql.Connection;
import java.util.List;

/**
 * @author Caozhaoyu
 */
public interface DatabaseStrategy {
    String getDataType(Columns column);
    Connection connection(String host, String port, String user, String password, String schema);
    void close(Connection connection);
    List<Table> getTables(String schema, Connection connection);
    List<Comments> getComments(String schema, Connection connection);
    List<Columns> getColumns(String schema, String tableName, Connection connection);
    String generateCommentsSql(String schema, String tableName, Columns column, List<Comments> commentsList, List<Comments> commentsList2);
    String generateCommentsSql(String schema, String tableName, List<Columns> list, List<Comments> commentsList, List<Comments> commentsList2);
    String generateCreateSql(String schema, String tableName, List<Columns> list);
    String generateUpdateSql(String schema, String tableName, Columns column);
}
