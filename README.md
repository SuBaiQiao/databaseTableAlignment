# 数据库表结构对齐工具

这是一个数据库表结构对齐工具，用于比较两个数据库实例之间的表结构差异，并自动生成对齐所需的SQL语句。

## 功能特性

- **跨数据库支持**：支持人大金仓（KingBase）和达梦（DM）数据库
- **表结构对比**：检测缺少的表、缺少的字段
- **注释同步**：检测并同步字段注释差异
- **SQL自动生成**：自动生成创建表、添加字段、更新注释的SQL语句

## 技术架构

采用策略模式设计，便于扩展支持更多数据库类型：

```
┌─────────────────────────────────────────┐
│              Main.java                  │  ← 主程序入口
├─────────────────────────────────────────┤
│         DatabaseContext                 │  ← 策略上下文
├─────────────────────────────────────────┤
│      DatabaseStrategy (接口)            │  ← 策略接口
├─────────────────────────────────────────┤
│  KingBaseDatabaseStrategy               │  ← 人大金仓实现
│  DmDatabaseStrategy                     │  ← 达梦数据库实现
├─────────────────────────────────────────┤
│  POJO: Table, Columns, Comments         │  ← 数据模型
└─────────────────────────────────────────┘
```

## 快速开始

### 环境要求

- JDK 20+
- Maven 3.6+

### 编译运行

```bash
# 进入项目目录
cd databaseTableAlignment

# 编译项目
mvn clean compile

# 运行项目
mvn exec:java -Dexec.mainClass="com.subaiqiao.databaseTableAlignment.Main"
```

## 使用说明

### 配置数据库连接

在 `src/main/java/com/subaiqiao/databaseTableAlignment/Main.java` 中配置：

```java
// 数据库类型：DM 或 KING_BASE
private static final DatabaseEnum DATABASE_TYPE = DatabaseEnum.DM;

// Schema名称
private static final String SCHEMA = "HW_SSIP";

// 源数据库（需要检查/修复的数据库）
Connection connection = context.getConnection("192.168.0.105", "5236", "SYSDBA", "Aa123456", SCHEMA);

// 目标数据库（作为基准的正确数据库）
Connection connection2 = context.getConnection("192.168.0.200", "30236", "SYSDBA", "SYSDBA001", SCHEMA);
```

### 执行流程

1. 连接源数据库和目标数据库
2. 获取两个数据库的表结构、字段和注释信息
3. 对比差异并生成对齐SQL
4. 输出提示信息和执行SQL

## 支持的数据库类型

| 数据库类型 | 枚举值 | 驱动依赖 |
| :--- | :--- | :--- |
| 达梦数据库 | DM | DmJdbcDriver18 |
| 人大金仓 | KING_BASE | kingbase8 |

## 项目结构

```
databaseTableAlignment/
├── src/main/java/com/subaiqiao/databaseTableAlignment/
│   ├── Main.java                 # 主程序入口
│   ├── pojo/                     # 数据模型
│   │   ├── Table.java            # 表信息
│   │   ├── Columns.java          # 字段信息
│   │   └── Comments.java         # 注释信息
│   └── strategy/                 # 策略模式实现
│       ├── DatabaseStrategy.java         # 策略接口
│       ├── DatabaseContext.java          # 策略上下文
│       ├── DatabaseEnum.java             # 数据库类型枚举
│       ├── DatabaseStrategyFactory.java  # 策略工厂
│       ├── dm/                           # 达梦数据库策略
│       │   └── DmDatabaseStrategy.java
│       └── kingBase/                     # 人大金仓策略
│           └── KingBaseDatabaseStrategy.java
├── pom.xml                       # Maven配置
└── README.md                     # 项目说明
```

## 输出示例

```
数据库连接成功！
数据库连接成功！
===================提示信息START======================
缺少表：	HW_TEST	字段：ID, NAME, AGE
表 HW_USER 缺少字段：EMAIL
===================提示信息END======================
===================修改SQL信息START======================
create table HW_SSIP.HW_TEST
(
	ID int not null,
	NAME varchar2(50),
	AGE int,
	constraint PK_HW_TEST primary key (ID)
);
comment
on column HW_SSIP.HW_USER.EMAIL is '邮箱地址';
===================修改SQL信息END======================
```

## License

MIT License
