package com.subaiqiao.databaseTableAlignment;

import com.subaiqiao.databaseTableAlignment.pojo.Columns;
import com.subaiqiao.databaseTableAlignment.pojo.Comments;
import com.subaiqiao.databaseTableAlignment.pojo.Table;
import com.subaiqiao.databaseTableAlignment.strategy.DatabaseContext;
import com.subaiqiao.databaseTableAlignment.strategy.DatabaseEnum;
import com.subaiqiao.databaseTableAlignment.strategy.DatabaseStrategy;
import com.subaiqiao.databaseTableAlignment.strategy.DatabaseStrategyFactory;
import com.subaiqiao.databaseTableAlignment.strategy.kingBase.KingBaseDatabaseStrategy;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Caozhaoyu
 */
public class Main {

    public static DatabaseContext context = new DatabaseContext();

    private static final DatabaseEnum DATABASE_TYPE = DatabaseEnum.DM;

    private static final String SCHEMA = "HW_SSIP";

    public static void main(String[] args) {
        context.setStrategy(DatabaseStrategyFactory.getStrategy(DATABASE_TYPE));
        if (Objects.isNull(context.getStrategy())) {
            throw new RuntimeException("请选择数据库类型");
        }
        // 需要给谁检查
        Connection connection = context.getConnection("192.168.0.105", "5236", "SYSDBA", "Aa123456", SCHEMA);
        List<Table> jwrs = context.getTables(SCHEMA, connection);
        jwrs.forEach(e -> e.setColumns(context.getColumns(SCHEMA, e.getTableName(), connection)));
        List<Comments> commentsList = context.getComments(SCHEMA, connection);

        // 谁是对的
        Connection connection2 = context.getConnection("192.168.0.200", "30236", "SYSDBA", "SYSDBA001", SCHEMA);
        List<Table> jwrs2 = context.getTables(SCHEMA, connection2);
        jwrs2.forEach(e -> e.setColumns(context.getColumns(SCHEMA, e.getTableName(), connection2)));
        List<Comments> commentsList2 = context.getComments(SCHEMA, connection2);

        List<String> list = validateAndGenerateSql(jwrs, jwrs2, SCHEMA, commentsList, commentsList2);
        if (list.isEmpty()) {
            System.out.println("无变动情况");
        } else {
            System.out.println("===================修改SQL信息START======================");
            list.forEach(System.out::println);
            System.out.println("===================修改SQL信息END======================");
        }
        // 关闭资源
        context.close(connection);
        context.close(connection2);
    }

    public static List<String> validateAndGenerateSql(List<Table> errorList, List<Table> successList, String schema, List<Comments> commentsList, List<Comments> commentsList2) {
        Map<String, List<Columns>> successMap = successList.stream().collect(Collectors.toMap(Table::getTableName, Table::getColumns));
        Map<String, List<Columns>> errorMap = errorList.stream().collect(Collectors.toMap(Table::getTableName, Table::getColumns));
        List<String> sqlList = new ArrayList<>();
        List<String> tipsList = new ArrayList<>();
        successMap.forEach((k, v) -> {
            List<String> sql = new ArrayList<>();
            if (!errorMap.containsKey(k)) {
                tipsList.add("缺少表：\t" + k + "\t字段：" + v.stream().map(Columns::getColumnName).collect(Collectors.joining("、")));
                sql.add(context.generateCreateSql(schema, k, v));
                sql.add(context.generateCommentsSql(schema, k, v, commentsList, commentsList2));
            } else {
                List<Columns> columns = errorMap.get(k);
                Set<String> lackSet = v.stream().map(Columns::getColumnName).collect(Collectors.toSet());
                lackSet.removeAll(columns.stream().map(Columns::getColumnName).collect(Collectors.toSet()));
                if (!lackSet.isEmpty()) {
                    tipsList.add("表" + k + "缺少字段：" + String.join("、", lackSet));
                    for (Columns column : v) {
                        if (lackSet.contains(column.getColumnName())) {
                            sql.add(context.generateUpdateSql(schema, k, column));
                        }
                        String generateCommentsSql = context.generateCommentsSql(schema, k, column, commentsList, commentsList2);
                        if (!generateCommentsSql.isEmpty()) {
                            sql.add(generateCommentsSql);
                        }
                    }
                }
                for (Columns column : v) {
                    String generateCommentsSql = context.generateCommentsSql(schema, k, column, commentsList, commentsList2);
                    if (!generateCommentsSql.isEmpty()) {
                        sql.add(generateCommentsSql);
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

}
