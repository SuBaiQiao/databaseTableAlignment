package com.subaiqiao.databaseTableAlignment.strategy.kingBase;

import com.subaiqiao.databaseTableAlignment.pojo.Columns;
import com.subaiqiao.databaseTableAlignment.pojo.Comments;
import com.subaiqiao.databaseTableAlignment.pojo.Table;
import com.subaiqiao.databaseTableAlignment.strategy.DatabaseStrategy;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author Caozhaoyu
 * @date 2025年10月11日 17:46
 */
public class KingBaseDatabaseStrategy implements DatabaseStrategy {
    @Override
    public String getDataType(Columns column) {
        String dateType = column.getDataType();
        if ("timestamp without time zone".equalsIgnoreCase(dateType)) {
            dateType = "datetime";
        }
        if ("bpchar".equalsIgnoreCase(dateType)) {
            dateType = "char";
            if (null != column.getCharacterMaximumLength() && !"".equals(column.getCharacterMaximumLength())) {
                dateType += "(" + column.getCharacterMaximumLength() + ")";
            }
        }
        if ("varchar".equalsIgnoreCase(dateType)) {
            dateType = "varchar2";
            if (null != column.getCharacterMaximumLength() && !"".equals(column.getCharacterMaximumLength())) {
                dateType += "(" + column.getCharacterMaximumLength() + ")";
            }
        }
        if ("integer".equalsIgnoreCase(dateType)) {
            dateType = "int";
        }
        if ("numeric".equalsIgnoreCase(dateType)) {
            dateType = "decimal";
            if (null != column.getNumericPrecision() && !"".equals(column.getNumericPrecision())) {
                dateType += "(" + column.getNumericPrecision();
                String scale = "0";
                if (null != column.getNumericScale() && !"".equals(column.getNumericScale())) {
                    scale = column.getNumericScale();
                }
                dateType += "," + scale + ")";
            }
        }
        return dateType;
    }

    @Override
    public Connection connection(String host, String port, String user, String password) {
        Connection conn = null;
        String url = "jdbc:kingbase8://" + host + ":" + port + "/jwrs?clientEncoding=UTF8";
        try {
            // 对于JDBC 4.0及以上版本，可以省略Class.forName()
            // Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("数据库连接成功！");
        } catch (SQLException e) {
            throw new RuntimeException("数据库连接失败！", e);
        }
        return conn;
    }

    @Override
    public void close(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Table> getTables(String schema, Connection connection) {
        List<Table> list = new ArrayList<>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select table_name\n" +
                    "from information_schema.tables\n" +
                    "where table_schema = '" + schema + "'");
            while (rs.next()) {
                // 处理结果集
                String tableName = rs.getString("table_name");
                Table table = new Table();
                table.setTableName(tableName.toUpperCase());
                list.add(table);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    @Override
    public List<Comments> getComments(String schema, Connection connection) {
        List<Comments> list = new ArrayList<>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select comments.OWNER as OWNER," +
                    " comments.TABLE_NAME as TABLE_NAME," +
                    " comments.COLUMN_NAME as COLUMN_NAME," +
                    " comments.COMMENTS as COMMENTS" +
                    " from all_col_comments comments, information_schema.tables tables" +
                    " where tables.table_schema = '" + schema + "'" +
                    " and upper(comments.table_name) = upper(tables.table_name)");
            while (rs.next()) {
                // 处理结果集
                String owner = rs.getString("OWNER");
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                String comments = rs.getString("COMMENTS");
                Comments commentsPojo = new Comments();
                commentsPojo.setOwner(owner);
                commentsPojo.setTableName(tableName.toUpperCase());
                commentsPojo.setColumnName(columnName.toUpperCase());
                commentsPojo.setComments(comments);
                commentsPojo.setValid(false);
                list.add(commentsPojo);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    @Override
    public List<Columns> getColumns(String schema, String tableName, Connection connection) {
        List<Columns> list = new ArrayList<>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select column_name, is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale, is_identity from information_schema.columns where table_schema = '" + schema + "' and upper(table_name) = '" + tableName + "'");
            while (rs.next()) {
                // 处理结果集
                String columnName = rs.getString("column_name");
                String isNullable = rs.getString("is_nullable");
                String dataType = rs.getString("data_type");
                String characterMaximumLength = rs.getString("character_maximum_length");
                String numericPrecision = rs.getString("numeric_precision");
                String numericScale = rs.getString("numeric_scale");
                String isIdentity = rs.getString("is_identity");
                Columns columns = new Columns();
                columns.setColumnName(columnName.toUpperCase());
                columns.setIsNullable(isNullable);
                columns.setDataType(dataType);
                columns.setCharacterMaximumLength(characterMaximumLength);
                columns.setNumericPrecision(numericPrecision);
                columns.setNumericScale(numericScale);
                columns.setIsIdentity(isIdentity);
                list.add(columns);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    @Override
    public String generateCommentsSql(String schema, String tableName, Columns column, List<Comments> commentsList, List<Comments> commentsList2) {
        StringBuilder sql = new StringBuilder();
        Optional<Comments> optionalComments = commentsList.stream().filter(comments -> comments.getTableName().equalsIgnoreCase(tableName) &&
                comments.getColumnName().equalsIgnoreCase(column.getColumnName())).findFirst();
        Optional<Comments> optionalComments2 = commentsList2.stream().filter(comments -> comments.getTableName().equalsIgnoreCase(tableName) &&
                comments.getColumnName().equalsIgnoreCase(column.getColumnName())).findFirst();
        if (optionalComments2.isEmpty() || optionalComments2.get().isValid()) {
            return "";
        }
        Comments comments2 = optionalComments2.get();
        if (optionalComments.isEmpty()) {
            comments2.setValid(true);
            sql.append("comment\n");
            sql.append(String.format("on column %s.%s.%s is '%s';\n", schema, tableName, column.getColumnName(), comments2.getComments()));
            return "";
        }
        Comments comments = optionalComments.get();
        if (!comments.getComments().equals(comments2.getComments())) {
            comments2.setValid(true);
            sql.append("comment\n");
            sql.append(String.format("on column %s.%s.%s is '%s';\n", schema, tableName, column.getColumnName(), comments2.getComments()));
        }
        return sql.toString();
    }

    @Override
    public String generateCommentsSql(String schema, String tableName, List<Columns> list, List<Comments> commentsList, List<Comments> commentsList2) {
        StringBuilder sql = new StringBuilder();
        for (Columns column : list) {
            Optional<Comments> optionalComments = commentsList.stream().filter(comments -> comments.getTableName().equalsIgnoreCase(tableName) &&
                    comments.getColumnName().equalsIgnoreCase(column.getColumnName())).findFirst();
            Optional<Comments> optionalComments2 = commentsList2.stream().filter(comments -> comments.getTableName().equalsIgnoreCase(tableName) &&
                    comments.getColumnName().equalsIgnoreCase(column.getColumnName())).findFirst();
            if (optionalComments2.isEmpty() || optionalComments2.get().isValid()) {
                continue;
            }
            Comments comments2 = optionalComments2.get();
            if (optionalComments.isEmpty()) {
                comments2.setValid(true);
                sql.append("comment\n");
                sql.append(String.format("on column %s.%s.%s is '%s';\n", schema, tableName, column.getColumnName(), comments2.getComments()));
                continue;
            }
            Comments comments = optionalComments.get();
            if (!comments.getComments().equals(comments2.getComments())) {
                comments2.setValid(true);
                sql.append("comment\n");
                sql.append(String.format("on column %s.%s.%s is '%s';\n", schema, tableName, column.getColumnName(), comments2.getComments()));
            }
        }
        return sql.toString();
    }

    @Override
    public String generateCreateSql(String schema, String tableName, List<Columns> list) {
        StringBuilder sql = new StringBuilder(String.format("create table %s.%s\n" +
                "(\n", schema, tableName));
        for (Columns column : list) {
            sql.append(String.format("\t%s %s%s,\n", column.getColumnName(), getDataType(column), "id".equalsIgnoreCase(column.getColumnName()) ? " not null" : ""));
        }
        sql.append(String.format("\tconstraint PK_%s primary key (ID)\n" + ");", tableName.toUpperCase()));
        return sql.toString();
    }

    @Override
    public String generateUpdateSql(String schema, String tableName, Columns column) {
        return "alter table " + schema + "." + tableName + " add " + column.getColumnName() + " " + getDataType(column) + ";";
    }
}
