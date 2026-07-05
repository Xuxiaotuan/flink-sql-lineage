# FlinkSQL Lineage

The Lineage Analysis system for FlinkSQL supports advanced syntax such as Watermark, UDTF, CEP, Windowing TVFs, and CTAS. The core process involves parsing SQL using Calcite to generate a RelNode tree. Subsequently, RelMetadataQuery is utilized to retrieve field-level lineage information, which is then presented through visualization.

> If you are interested, you can add me on WeChat: HamaWhite, or send email to baisongxx@gmail.com

## Features

![Product.gif](https://github.com/HamaWhiteGG/flink-sql-lineage/blob/main/data/images/Product.gif)

* Support lineage at both table and column levels in Flink SQL.
* Support processing and transformation relationships for field lineage in Flink SQL.
* Visualize data lineage, displaying the total number of downstream fields for a specific column.
* Simultaneously support parsing multiple versions of Flink.
* Automatically parse JAR content when creating a new Flink UDF to populate function information.
* Support view reference jobs for custom functions.

## Architecture

![Architecture.png](https://github.com/HamaWhiteGG/flink-sql-lineage/blob/main/data/images/Architecture.png)

* Frontend development based on [Butterfly](https://github.com/alibaba/butterfly) with customizations.
* Lineage-server designed using Domain-Driven Design (DDD) architecture.
* Plugin-based design, with each Flink version corresponding to a plugin.
* Utilizing SPI and Classloaders, each plugin is loaded using a separate Classloader.

## Principle

![Principle.png](https://github.com/HamaWhiteGG/flink-sql-lineage/blob/main/data/images/Principle.png)

1. Syntax parsing involves converting Flink SQL into an Abstract Syntax Tree (AST), where Calcite uses SqlNode for representation.
2. Syntax validation is performed by leveraging metadata information for syntax verification, resulting in AST composed of SqlNode elements.
3. Semantic analysis entails constructing a RelNode tree based on SqlNode and metadata information.
4. The getColumnOrigins(RelNode rel, int column) function from RelMetadataQuery is invoked to query original field details.

> If encountering unresolved lineage relationships, please examine the types within the RelNode Tree. 
> Generally, to accurately parse them, you can add corresponding methods within the RelMdColumnOrigins class in the source code.


## Build and Deploy
Prerequisites for building Flink:
* Java 8
* Unix-like environment (we use Linux, Mac OS X, Cygwin, WSL)
* Maven (we recommend version 3.8.6 and require at least 3.5.4)

```shell
git clone https://github.com/HamaWhiteGG/flink-sql-lineage.git
cd flink-sql-lineage

# export JAVA_HOME=JDK8_INSTALL_HOME && mvn clean package
mvn clean package
```
> Note: If you use IntelliJ IDEA, please install the Manifold plugin.

The database is MySQL, and scripts are located in the directory [scripts/mysql](scripts/mysql).

If you don't have MySQL, you can switch to the test profile to quick start (which uses the H2 database).

Then Run [LineageServerApplication](lineage-server/lineage-server-start/src/main/java/com/hw/lineage/server/start/LineageServerApplication.java) and then open http://127.0.0.1:8194.

## 本地开发快速启动

这一段记录的是在 macOS 本地验证过的启动方式：使用 `test` profile、内存 H2 数据库，以及 Maven 构建出来的本地插件目录。

### 1. 构建后端和 Flink 插件

当前已有的 Flink 1.14/1.16 插件模块建议使用 JDK 8 构建。

```shell
cd /Users/xujiawei/magic/workbench/flink-sql-lineage

JAVA_HOME=$(/usr/libexec/java_home -v 1.8) \
MAVEN_OPTS="-Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=7890 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=7890 -Djava.net.preferIPv4Stack=true" \
/Users/xujiawei/software/apache-maven-3.8.8/bin/mvn -U \
  -pl lineage-flink1.14.x,lineage-flink1.16.x,lineage-client,lineage-server/lineage-server-start \
  -am \
  -DskipTests \
  -Dprofile.active=test \
  package
```

构建完成后会生成：

* `lineage-client/target/plugins`: plugin bundles loaded by the server.
* `lineage-server/lineage-server-start/target/lineage-server-1.0.0.jar`: Spring Boot server jar.
* `lineage-web/build`: production frontend assets copied into the server build.

如果本机不需要代理，可以去掉 `MAVEN_OPTS`。

如果要使用 Flink 2.1 插件，需要使用 JDK 17 构建 `lineage-flink2.1.x`。Flink 2.0+ 默认 Java 版本已经切到 Java 17，因此后端进程如果要加载 `flink2.1.x` 插件，也建议用 JDK 17 启动。

```shell
cd /Users/xujiawei/magic/workbench/flink-sql-lineage

JAVA_HOME=$(/usr/libexec/java_home -v 17) \
MAVEN_OPTS="-Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=7890 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=7890 -Djava.net.preferIPv4Stack=true" \
/Users/xujiawei/software/apache-maven-3.8.8/bin/mvn \
  -pl lineage-flink2.1.x \
  -am \
  -Dmaven.test.skip=true \
  package

JAVA_HOME=$(/usr/libexec/java_home -v 17) \
MAVEN_OPTS="-Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=7890 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=7890 -Djava.net.preferIPv4Stack=true" \
/Users/xujiawei/software/apache-maven-3.8.8/bin/mvn \
  -pl lineage-client \
  -am \
  -Dmaven.test.skip=true \
  package
```

构建完成后会额外生成：

* `lineage-flink2.1.x/target/lineage-flink2.1.x-1.0.0.jar`
* `lineage-flink2.1.x/target/lib`: Flink 2.1 运行依赖。
* `lineage-client/target/plugins/flink2.1.x`: server/client 实际加载的 Flink 2.1 插件目录。

### 2. 启动 H2 后端

当前打出来的 jar 里，`application.properties` 仍保留了几处 Maven 风格占位符；同时 H2 初始化 SQL 没有被打进 boot jar。用 jar 做本地启动时，建议显式传入这些配置，并从源码目录加载 H2 脚本：

```shell
cd /Users/xujiawei/magic/workbench/flink-sql-lineage

JAVA_HOME=$(/usr/libexec/java_home -v 17) \
java -jar lineage-server/lineage-server-start/target/lineage-server-1.0.0.jar \
  --spring.profiles.active=test \
  --profile.active=test \
  --server.port=8194 \
  --spring.sql.init.platform=h2 \
  --spring.sql.init.mode=always \
  --spring.sql.init.schema-locations=file:lineage-server/lineage-server-start/src/main/resources/scripts/h2/schema.sql \
  --spring.sql.init.data-locations=file:lineage-server/lineage-server-start/src/main/resources/scripts/h2/data.sql \
  --spring.datasource.driver-class-name=org.h2.Driver \
  '--spring.datasource.url=jdbc:h2:mem:lineage;DB_CLOSE_DELAY=-1;MODE=MySQL;' \
  --spring.datasource.username=sa \
  --spring.datasource.password=sa \
  --spring.servlet.multipart.max-file-size=20MB \
  --spring.servlet.multipart.max-request-size=20MB \
  --lineage.plugin.dir=lineage-client/target/plugins \
  --lineage.storage.dir=data/storage \
  --logging.file.path=data/logs
```

验证后端：

```shell
curl http://127.0.0.1:8194/actuator
```

返回内容可能会被应用登录逻辑包装，但只要 `8194` 端口有 HTTP 响应，就说明后端已经起来。

### 3. 启动前端

首次启动前先安装依赖：

```shell
cd /Users/xujiawei/magic/workbench/flink-sql-lineage/lineage-web
yarn install
```

启动 dev server。如果本机 `3000` 已被占用，可以用 `3001`。

```shell
PORT=3001 BROWSER=none HOST=127.0.0.1 yarn start
```

打开：

```text
http://127.0.0.1:3001
```

前端开发代理配置在 `lineage-web/config-overrides.js`。本地调试时建议指向：

```js
target: 'http://127.0.0.1:8194'
```

`scripts/h2/data.sql` 初始化的默认用户包括：

* `admin / admin`
* `demo / demo`
* `HamaWhite / 123456`

本地已经验证过的 Flink 2.1 插件回归命令：

```shell
JAVA_HOME=$(/usr/libexec/java_home -v 17) \
MAVEN_OPTS="-Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=7890 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=7890 -Djava.net.preferIPv4Stack=true" \
/Users/xujiawei/software/apache-maven-3.8.8/bin/mvn \
  -pl lineage-client \
  -am \
  -Dtest=LineageClientTest \
  -DfailIfNoTests=false \
  test
```

这个测试会动态加载 `lineage-client/target/plugins/flink2.1.x`，注册 `generic_in_memory` catalog，创建 `datagen` source 和 `blackhole` sink，然后验证 Flink 1.14/1.16 的老 demo 以及 Flink 2.1 的 basic、transform、join demo。最小 Flink 2.1 用例会验证：

```text
flink21_source.id       -> flink21_sink.id
flink21_source.name     -> flink21_sink.name
flink21_source.birthday -> flink21_sink.partition, transform = DATE_FORMAT(birthday, 'yyyyMMdd')
```

### 4. Flink 2.1 示例任务

H2 初始化数据里已经内置了 `Flink21_memory` catalog。它使用 Flink 2.1 插件，初始化 4 张轻量表：

* `flink21_users_source`: `datagen` source，包含 `id/name/birthday/score/proc_time`。
* `flink21_company_source`: `datagen` source，包含 `user_id/company_name`。
* `flink21_users_sink`: `blackhole` sink，用于用户明细类血缘。
* `flink21_stats_sink`: `blackhole` sink，用于转换类血缘。

对应的 Job 列表里有 3 个 Flink 2.1 demo：

* `flink21-basic-demo`: 普通投影、类型转换、`DATE_FORMAT`。
* `flink21-transform-demo`: `UPPER`、数值计算、`CAST`、日期格式化。
* `flink21-join-demo`: 两张 source join 后写入 sink，展示跨表字段血缘。

这些 demo 不依赖本地 MySQL、Hudi 或 JDBC 服务。`datagen` 和 `blackhole` 只负责让 Flink 2.1 planner 能完成 connector 校验；字段血缘依然来自初始化 DDL 注册到 Flink CatalogManager 后生成的 `RelNode`。

## 字段血缘解析原理

这个项目不是简单调用 Calcite 的 SQL parser。它的主链路是：

1. `lineage-server` 接收任务，通过 `LineageClient` 加载用户选择的 Flink 插件。
2. `LineageClient` 把线程上下文 classloader 切到插件 classloader，然后调用插件里的 `LineageService`。
3. Flink 插件创建一个真实的 Flink `TableEnvironment`。
4. 任务里的 DDL 会先执行，让 catalog、database、table、view、connector、function 都注册进 Flink 的 catalog manager。
5. 对 `INSERT` 或 CTAS，Flink parser 会完成 parse、validate、convert，返回 Flink `Operation`。
6. 插件从 `SinkModifyOperation -> PlannerQueryOperation -> getCalciteTree()` 里取出查询部分的 `RelNode`。
7. 插件注册 Flink 自己的 metadata provider：`FlinkDefaultRelMetadataProvider`，再对目标表每个字段调用 `RelMetadataQuery#getColumnOrigins(relNode, index)`。
8. 服务端把 source column、target column、transform 表达式保存成 `TaskLineage`，最后构建成 UI 中看到的表级和字段级血缘图。

核心代码位置：

* `lineage-client/src/main/java/com/hw/lineage/client/LineageClient.java`
* `lineage-flink1.16.x/src/main/java/com/hw/lineage/flink/LineageServiceImpl.java`
* `lineage-flink2.1.x/src/main/java/com/hw/lineage/flink/LineageServiceImpl.java`
* `lineage-server/lineage-server-infrastructure/src/main/java/com/hw/lineage/server/infrastructure/facade/impl/LineageFacadeImpl.java`

### 为什么直接用 Calcite 解析不出来

直接用 Calcite 通常只会走到 `SqlNode`，也就是语法树。`SqlNode` 能告诉你 SQL 里有 `a + b AS c`、join、函数调用、insert target，但它还不知道足够多的 Flink 语义信息，无法直接还原生产级字段血缘。

字段血缘需要这些语义信息：

* 一个 identifier 到底指向哪个 catalog/database/table？
* source 和 sink 的 schema 是什么？
* 经过 projection、alias、join、window、UDTF、CTAS、view 之后，字段名对应到哪个字段序号？
* 哪些函数是内置函数，哪些是 catalog/user-defined function？
* 哪些语法是 Flink 扩展，比如 watermark、computed column、connector options、temporal join、window TVF？
* 哪个 metadata provider 能回答 Flink planner node 的 column origins？

所以这个项目走的是 Flink 自己的 planner 栈。Flink 内部仍然使用 Calcite，但它在 Calcite 之上补了 Flink SQL 解析、校验、catalog 解析、operation 转换，以及 Flink 专用 metadata。真正关键的对象不是原始 `SqlNode`，而是经过 validate/convert 后的 `RelNode`，再配合 Flink 的 `RelMetadataQuery` provider。

所以你自己直接调用 Calcite 时，如果解析不出来或拿不到字段血缘，常见原因是：

* 你用的是 Calcite 默认 SQL dialect，而不是 Flink parser/dialect。
* 分析 query 前，没有先把 DDL 执行进 catalog。
* 只 parse 到 `SqlNode`，没有 validate 并 convert 到 `RelNode`。
* `RelMetadataQuery` 用的是 Calcite 默认 metadata provider，而不是 Flink 的 `FlinkDefaultRelMetadataProvider`。
* 某些 Flink planner node 需要额外的 `RelMdColumnOrigins` 支持，`getColumnOrigins` 才能解析。

## 截图中的血缘图是怎么做出来的

截图里的血缘图分成两层：后端负责产出结构化血缘数据，前端负责把这些数据渲染成表格节点、字段行、曲线连线和高亮效果。

### 1. 页面触发分析

任务详情页会先请求任务详情：

```text
GET /tasks/{taskId}
```

点击分析血缘时会调用：

```text
POST /tasks/{taskId}/lineage
```

接口入口在 `TaskController#analyzeTaskLineage`。后端随后进入 `TaskServiceImpl#analyzeTaskLineage`，主要步骤是：

1. 从数据库读取任务。
2. 清理旧的 graph、task sql、lineage、function 结果。
3. 将任务里的 SQL 拆成单条 `TaskSql`。
4. 根据任务所属 catalog 找到对应 plugin，例如 `flink1.16.x`。
5. 调用 `lineageFacade.analyze(pluginCode, catalogName, task)`。
6. 保存分析出的 `TaskLineage`。
7. 将 `TaskLineage` 组装成前端可渲染的 `lineageGraph`。

### 2. Flink 插件产出字段血缘

`LineageFacadeImpl` 会逐条处理 SQL：

* 普通 DDL：直接执行到 Flink `TableEnvironment`。
* `INSERT`：调用插件的 `analyzeLineage`。
* CTAS：先把 `CREATE TABLE` 注册到 catalog，再把 SELECT 部分转成 sink modify operation 分析。

在 `lineage-flink1.16.x` 插件里，核心链路是：

```text
tableEnv.getParser().parse(sql)
  -> Operation
  -> SinkModifyOperation
  -> PlannerQueryOperation
  -> RelNode
  -> RelMetadataQuery#getColumnOrigins(relNode, targetColumnIndex)
```

这里的 `tableEnv.getParser().parse(sql)` 不是单纯的 Calcite parse。Flink 这层会做 parse、validate、convert，因此能拿到已经绑定 catalog、schema、function 和类型信息的 `RelNode`。

每个目标字段都会调用一次：

```text
getColumnOrigins(relNode, index)
```

然后生成类似这样的血缘事实：

```text
source: ods_mysql_users.name
target: dwd_hudi_users.name
transform: CONCAT(a.name, b.company_name)
```

### 3. 后端组装 graph 数据

`GraphFactory#createLineageGraph` 会把 `TaskLineage` 转成两类图：

* `TableGraph`：表到表的关系。
* `ColumnGraph`：字段到字段的关系。

之后 `DtoAssembler#toLineageGraph` 会转换成前端使用的 JSON：

```json
{
  "nodes": [
    {
      "id": "1",
      "name": "ods_mysql_users",
      "columns": [
        { "id": "11", "name": "id", "childrenCnt": 1 },
        { "id": "12", "name": "name", "childrenCnt": 1 }
      ],
      "hasUpstream": false,
      "hasDownstream": true
    }
  ],
  "links": [
    {
      "id": "100",
      "relU": "1",
      "relV": "2",
      "u": "12",
      "v": "22",
      "transform": "CONCAT(a.name, b.company_name)"
    }
  ]
}
```

字段含义：

* `nodes`: 表节点。
* `nodes[].columns`: 表里的字段行。
* `links[].relU`: 源表 id。
* `links[].relV`: 目标表 id。
* `links[].u`: 源字段 id。
* `links[].v`: 目标字段 id。
* `links[].transform`: 字段转换表达式。

如果一个 link 只有 `relU/relV`，它是表级血缘；如果同时有 `u/v`，它是字段级血缘。

### 4. 前端把 graph 画成截图里的样子

前端页面拿到 `lineageGraph` 后传给：

```jsx
<LineageTable {...data} />
```

`LineageTable` 在 `lineage-web/src/component/react-lineage-dag/index.js` 中实现，内部依赖 `butterfly-react`。它做的事情包括：

* 将每张表渲染成一个卡片节点。
* 将字段渲染成表格行。
* 给表和字段生成左右 endpoint。
* 将 `relU/relV/u/v` 转成 Butterfly 的 source/target endpoint。
* 使用 DAG layout 自动排布节点。
* 用灰色曲线表示普通关系，用橙色高亮 hover/选中的字段路径。
* 支持字段血缘和表血缘切换。
* 支持全屏和左下角 minimap。

所以“好看”的部分主要来自前端图组件和样式；“准”的部分来自后端是否能产出正确的 `nodes + links`。

## 它的 schema 从哪里来

这个项目看起来没有让用户显式传 schema，是因为 schema 已经藏在系统初始化数据和运行时 catalog 里了。

本地 `test` profile 启动时，H2 的 `scripts/h2/data.sql` 会初始化：

* `bas_catalog`: 插件和 catalog 信息，例如 `Flink16_memory`。
* `bas_table`: 表信息和表 DDL。
* `bas_function`: UDF/UDTF 信息。
* `bas_task`: 示例任务 SQL。

其中 `bas_table.ddl` 存的是 Base64 编码后的 `CREATE TABLE`。例如 `ods_mysql_users`、`dim_mysql_company`、`dwd_hudi_users` 这些表，初始化数据里都有完整 DDL。

启动 Spring Boot 时，`LineageServerApplication` 里有几个 `CommandLineRunner`：

```text
initStorage
createMemoryCatalogs
createMemoryTables
createMemoryFunctions
```

关键是 `createMemoryTables`：

```text
bas_table.ddl
  -> Base64 decode
  -> CREATE TABLE ...
  -> lineageClient.execute(...)
  -> Flink TableEnvironment.executeSql(...)
  -> Flink CatalogManager 里有了表和字段 schema
```

对应代码是：

* `Table.ddl` 注释说明它是 Base64 encode。
* `TableServiceImpl#createMemoryTables` 查询内存 catalog 下的表。
* `LineageFacadeImpl#createTable` 对 DDL 做 `Base64Utils.decode(ddl)`。
* `LineageClient#execute` 切到插件 classloader 后执行 SQL。
* `LineageServiceImpl#executeSql` 调用 `tableEnv.executeSql(singleSql)`。

所以它不是“没有 schema 也能准确解析”，而是：

```text
用户没有手动传 schema
但系统启动时已经把初始化表 DDL 注册进 Flink Catalog 了
分析任务 SQL 时，Flink Validator 可以从 CatalogManager 查到 schema
```

这也是你自己只拿到别人 SQL、没有任何 schema 时会卡在 `RelNode` 的原因。这个项目的示例之所以能跑，是因为 `data.sql` 里已经给了表 DDL；生产场景如果只给 SQL，就需要额外从 Hive Metastore、Flink Catalog、MySQL information_schema、Paimon/Hudi catalog 或你自己的元数据服务补齐 schema。

### 只给 SQL、没有 schema 时怎么办

如果别人只给一段 `INSERT INTO sink SELECT ... FROM source`，Calcite 或 Flink 都没法凭空知道 `source` 和 `sink` 的字段列表、字段类型、主键、水印、计算列、catalog/database 归属。你需要先补一层元数据输入，再进入 Flink planner。

可选做法：

* SQL 里本身包含完整 `CREATE TABLE`/`CREATE VIEW`：先按顺序执行 DDL，再分析 DML。
* SQL 里只有 DML：从外部系统补 schema，例如 Hive Metastore、JDBC `information_schema`、Flink Catalog、Paimon/Hudi/Iceberg catalog、内部元数据平台。
* schema 暂时拿不到：只能做语法级别的“弱血缘”，例如识别 `SELECT a AS b`、`JOIN`、`INSERT target`，但不能可靠处理 `SELECT *`、同名字段、函数类型推导、computed column、view 展开和 connector 方言。

这个项目做到截图效果的最小闭环是：

```text
准备 plugin 目录
  -> 启动 LineageClient
  -> 创建 catalog
  -> 执行所有 source/sink/view/function DDL
  -> 对 INSERT/CTAS 调 Flink parser
  -> 拿 PlannerQueryOperation 的 RelNode
  -> RelMetadataQuery#getColumnOrigins
  -> 保存 TaskLineage
  -> GraphFactory 生成 nodes/links
  -> React LineageTable + butterfly-react 渲染
```

也就是说，它好看的 UI 依赖前端 DAG 组件；它能解析得准，依赖的是 Flink planner 已经拥有完整 schema，而不是 Calcite 单独“猜”出来。

For deploy on a remote server, you can quickly deploy using [deploy.sh](sbin/deploy.sh) (remember to modify the IP).
```shell
# export JAVA_HOME=JDK8_INSTALL_HOME && sh sbin/deploy.sh
sh sbin/deploy.sh
```

If you prefer to deploy using Docker Compose, run the following command:
```shell
# export JAVA_HOME=JDK8_INSTALL_HOME && sh sbin/start-docker-compose.sh
sh sbin/start-docker-compose.sh
```


## Support
Don’t hesitate to ask!

[Open an issue](https://github.com/HamaWhiteGG/flink-sql-lineage/issues) if you find a bug or need any help.

## Fork and Contribute
This is an active open-source project. We are always open to people who want to use the system or contribute to it. Please note that pull requests should be merged into the **dev** branch.

Contact [me](baisongxx@gmail.com) if you are looking for implementation tasks that fit your skills.

## Reward
If the project has been helpful to you, you can treat me to a cup of coffee.
<img src="https://github.com/HamaWhiteGG/flink-sql-lineage/blob/main/data/images/Appreciation%20code.png" alt="Appreciation code" style="width:40%;">
> This is a WeChat appreciation code.
