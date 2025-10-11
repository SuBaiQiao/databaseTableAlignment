package com.subaiqiao.databaseTableAlignment.strategy.dm;

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
public class DmDatabaseStrategy implements DatabaseStrategy {
    @Override
    public String getDataType(Columns column) {
        String dateType = column.getDataType();
        if ("timestamp without time zone".equalsIgnoreCase(dateType)) {
            dateType = "datetime";
        }
        if ("timestamp".equalsIgnoreCase(dateType)) {
            dateType = "TIMESTAMP";
        }
        if ("bpchar".equalsIgnoreCase(dateType)) {
            dateType = "char";
            if (null != column.getCharacterMaximumLength() && !"".equals(column.getCharacterMaximumLength())) {
                dateType += "(" + column.getCharacterMaximumLength() + ")";
            }
        }
        if ("varchar".equalsIgnoreCase(dateType)) {
            dateType = "varchar";
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
        if ("decimal".equalsIgnoreCase(dateType)) {
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
    public Connection connection(String host, String port, String user, String password, String schema) {
        Connection conn = null;
        String url = "jdbc:dm://" + host + ":" + port + "?schema=" + schema;
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
            ResultSet rs = stmt.executeQuery("SELECT TABLE_NAME FROM ALL_TAB_COMMENTS WHERE OWNER = '" + schema + "' AND TABLE_TYPE = 'TABLE' ORDER BY TABLE_NAME");
            while (rs.next()) {
                // 处理结果集
                String tableName = rs.getString("TABLE_NAME");
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
            ResultSet rs = stmt.executeQuery("SELECT COL.TABLE_NAME, COL.OWNER, COL.COLUMN_NAME, COM.COMMENTS FROM ALL_TAB_COLS COL LEFT JOIN USER_COL_COMMENTS COM ON COL.COLUMN_NAME = COM.COLUMN_NAME AND COL.TABLE_NAME = COM.TABLE_NAME AND COM.OWNER = COL.OWNER WHERE COL.OWNER = '" + schema + "' ORDER BY COL.COLUMN_ID;");
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
            ResultSet rs = stmt.executeQuery("SELECT COL.COLUMN_NAME, COL.DATA_TYPE, COL.DATA_LENGTH AS CHARACTER_MAXIMUM_LENGTH, COL.DATA_PRECISION AS NUMERIC_PRECISION, COL.DATA_SCALE AS NUMERIC_SCALE, COL.NULLABLE AS IS_NULLABLE, CASE WHEN PK.COLUMN_NAME IS NOT NULL THEN 'Y' ELSE 'N' END AS IS_IDENTITY FROM ALL_TAB_COLS COL LEFT JOIN ( SELECT CU.COLUMN_NAME FROM USER_CONSTRAINTS C JOIN USER_CONS_COLUMNS CU ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME AND C.OWNER = '" + schema + "' WHERE CU.COLUMN_NAME IS NOT NULL AND C.CONSTRAINT_TYPE = 'P' AND C.TABLE_NAME = '" + tableName + "') PK ON COL.COLUMN_NAME = PK.COLUMN_NAME WHERE COL.TABLE_NAME = '" + tableName + "' AND COL.OWNER = '" + schema + "' ORDER BY COL.COLUMN_ID;");
            while (rs.next()) {
                // 处理结果集
                String columnName = rs.getString("COLUMN_NAME");
                String isNullable = rs.getString("IS_NULLABLE");
                String dataType = rs.getString("DATA_TYPE");
                String characterMaximumLength = rs.getString("CHARACTER_MAXIMUM_LENGTH");
                String numericPrecision = rs.getString("NUMERIC_PRECISION");
                String numericScale = rs.getString("NUMERIC_SCALE");
                String isIdentity = rs.getString("IS_IDENTITY");
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
        StringBuilder sql = new StringBuilder(String.format("create table \"%s\".\"%s\"\n" +
                "(\n", schema, tableName));
        for (Columns column : list) {
            sql.append(String.format("\t\"%s\" %s %s %s,\n",
                    column.getColumnName(),
                    getDataType(column),
                    "id".equalsIgnoreCase(column.getColumnName()) ? "IDENTITY(1, 1)" : "",
                    "id".equalsIgnoreCase(column.getColumnName()) ? "NOT NULL" : "")
            );
        }
        sql.append("UNIQUE(\"ID\"),\n NOT CLUSTER PRIMARY KEY(\"ID\")\n) STORAGE(ON \"MAIN\", CLUSTERBTR);");
        return sql.toString();
    }

    @Override
    public String generateUpdateSql(String schema, String tableName, Columns column) {
        return "alter table " + schema + "." + tableName + " add " + column.getColumnName() + " " + getDataType(column) + ";";
    }
}
