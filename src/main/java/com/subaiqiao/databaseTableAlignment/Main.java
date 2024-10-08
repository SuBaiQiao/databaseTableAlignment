package com.subaiqiao.databaseTableAlignment;

import com.subaiqiao.databaseTableAlignment.pojo.Columns;
import com.subaiqiao.databaseTableAlignment.pojo.Comments;
import com.subaiqiao.databaseTableAlignment.pojo.Table;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        String schema = "jwrs";
        // 需要给谁检查
        Connection connection = connection("192.168.68.150", "54321", "system", "system");
        List<Table> jwrs = getTables(schema, connection);
        jwrs.forEach(e -> e.setColumns(getColumns(schema, e.getTableName(), connection)));
        List<Comments> commentsList = getComments(schema, connection);

        // 谁是对的
        Connection connection2 = connection("localhost", "54321", "system", "system");
        List<Table> jwrs2 = getTables(schema, connection2);
        jwrs2.forEach(e -> e.setColumns(getColumns(schema, e.getTableName(), connection2)));
        List<Comments> commentsList2 = getComments(schema, connection2);

        List<String> list = validateAndGenerateSQL(jwrs, jwrs2, schema, commentsList, commentsList2);
        if (list.isEmpty()) {
            System.out.println("无变动情况");
        } else {
            System.out.println("===================修改SQL信息START======================");
            list.forEach(System.out::println);
            System.out.println("===================修改SQL信息END======================");
        }
        // 关闭资源
        try {
            if (connection != null) connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            if (connection2 != null) connection2.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static List<String> validateAndGenerateSQL(List<Table> errorList, List<Table> successList, String schema, List<Comments> commentsList, List<Comments> commentsList2) {
        Map<String, List<Columns>> successMap = successList.stream().collect(Collectors.toMap(Table::getTableName, Table::getColumns));
        Map<String, List<Columns>> errorMap = errorList.stream().collect(Collectors.toMap(Table::getTableName, Table::getColumns));
        List<String> sqlList = new ArrayList<>();
        List<String> tipsList = new ArrayList<>();
        successMap.forEach((k, v) -> {
            List<String> sql = new ArrayList<>();
            if (!errorMap.containsKey(k)) {
                tipsList.add("缺少表：\t" + k + "\t字段：" + v.stream().map(Columns::getColumnName).collect(Collectors.joining("、")));
                sql.add(generateCreateSQL(schema, k, v));
                sql.add(generateCommentsSQL(schema, k, v, commentsList, commentsList2));
            } else {
                List<Columns> columns = errorMap.get(k);
                Set<String> lackSet = v.stream().map(Columns::getColumnName).collect(Collectors.toSet());
                lackSet.removeAll(columns.stream().map(Columns::getColumnName).collect(Collectors.toSet()));
                if (!lackSet.isEmpty()) {
                    tipsList.add("表" + k + "缺少字段：" + String.join("、", lackSet));
                    for (Columns column : v) {
                        if (lackSet.contains(column.getColumnName())) {
                            sql.add(generateUpdateSQL(schema, k, column));
                        }
                        String generateCommentsSQL = generateCommentsSQL(schema, k, column, commentsList, commentsList2);
                        if (!generateCommentsSQL.isEmpty()) {
                            sql.add(generateCommentsSQL);
                        }
                    }
                }
                for (Columns column : v) {
                    String generateCommentsSQL = generateCommentsSQL(schema, k, column, commentsList, commentsList2);
                    if (!generateCommentsSQL.isEmpty()) {
                        sql.add(generateCommentsSQL);
                    }
                }
            }
            if (!sql.isEmpty()) {
                sqlList.addAll(sql);
            }
        });
        System.out.println("===================提示信息START======================");
        tipsList.forEach(System.out::println);
        System.out.println("===================提示信息END======================");
        return sqlList;
    }

    public static String generateUpdateSQL(String schema, String tableName, Columns column) {
        return "alter table " + schema + "." + tableName + " add " + column.getColumnName() + " " + getDataType(column) + ";";
    }

    public static String generateCreateSQL(String schema, String tableName, List<Columns> list) {
        StringBuilder sql = new StringBuilder(String.format("create table %s.%s\n" +
                "(\n", schema, tableName));
        for (Columns column : list) {
            sql.append(String.format("\t%s %s%s,\n", column.getColumnName(), getDataType(column), "id".equalsIgnoreCase(column.getColumnName()) ? " not null" : ""));
        }
        sql.append(String.format("\tconstraint PK_%s primary key (ID)\n" + ");", tableName.toUpperCase()));
        return sql.toString();
    }

    public static String generateCommentsSQL(String schema, String tableName, List<Columns> list, List<Comments> commentsList, List<Comments> commentsList2) {
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

    public static String generateCommentsSQL(String schema, String tableName, Columns column, List<Comments> commentsList, List<Comments> commentsList2) {
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

    public static String getDataType(Columns column) {
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

    public static List<Table> getTables(String schema, Connection connection) {
        List<Table> list = new ArrayList<>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select table_name\n" +
                    "from information_schema.tables\n" +
                    "where table_schema = '" + schema + "'");
            while (rs.next()) {
                // 处理结果集
                String table_name = rs.getString("table_name");
                Table table = new Table();
                table.setTableName(table_name.toUpperCase());
                list.add(table);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public static List<Comments> getComments(String schema, Connection connection) {
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
                String table_name = rs.getString("TABLE_NAME");
                String column_name = rs.getString("COLUMN_NAME");
                String comments = rs.getString("COMMENTS");
                Comments commentsPojo = new Comments();
                commentsPojo.setOwner(owner);
                commentsPojo.setTableName(table_name.toUpperCase());
                commentsPojo.setColumnName(column_name.toUpperCase());
                commentsPojo.setComments(comments);
                commentsPojo.setValid(false);
                list.add(commentsPojo);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public static List<Columns> getColumns(String schema, String tableName, Connection connection) {
        List<Columns> list = new ArrayList<>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select column_name, is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale, is_identity from information_schema.columns where table_schema = '" + schema + "' and upper(table_name) = '" + tableName + "'");
            while (rs.next()) {
                // 处理结果集
                String column_name = rs.getString("column_name");
                String is_nullable = rs.getString("is_nullable");
                String data_type = rs.getString("data_type");
                String character_maximum_length = rs.getString("character_maximum_length");
                String numeric_precision = rs.getString("numeric_precision");
                String numeric_scale = rs.getString("numeric_scale");
                String is_identity = rs.getString("is_identity");
                Columns columns = new Columns();
                columns.setColumnName(column_name.toUpperCase());
                columns.setIsNullable(is_nullable);
                columns.setDataType(data_type);
                columns.setCharacterMaximumLength(character_maximum_length);
                columns.setNumericPrecision(numeric_precision);
                columns.setNumericScale(numeric_scale);
                columns.setIsIdentity(is_identity);
                list.add(columns);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public static Connection connection(String host, String port, String user, String password) {
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
}
