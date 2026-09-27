# `SparkSqlExecutor` Operator

---

A general-purpose operator that executes any user-provided Spark SQL query on the input DataFrame. 一个通用运算符，用于对输入的 DataFrame 执行任何用户提供的 Spark SQL

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.other.spark_sql_executor`
- Class name 类名: `SparkSqlExecutor`
- Inherit from 继承于: [Operator](../meta_operator/operator.md)
- Test code 测试代码: [test code](../../../tests/ops/other/test_spark_sql_executor.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`sql_query`|`<class 'str'>`|`None`|`True`|The Spark SQL statement to execute. Any referenced tables or views must already exist in the provided Spark session. 要执行的 Spark SQL 语句；引用的表或视图必须已存在于传入的 Spark 会话中。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束
### Input Column Constraints 输入列约束
      * `Referenced Relations 引用关系`: This operator does not receive or register an input DataFrame. Every table or view referenced by `sql_query` must already exist in
      the provided Spark session. Required columns and data types depend on the SQL statement. 该算子不接收或注册输入 DataFrame。`sql_query` 引用的表或视图必须已存在于传入的
      Spark 会话中，所需字段及其类型由 SQL 语句决定。

      ### Argument and Column Mapping 参数与列的映射
      * `sql_query` -> SQL Statement SQL 语句: The Spark SQL statement to execute. It determines the referenced relations, expressions, filters, aggregations, and output
      columns. 要执行的 Spark SQL 语句，由其决定引用的表或视图、表达式、过滤条件、聚合方式和输出字段。

      ### Schema Transformation Process Schema 转换过程
      * `Relation Resolution 关系解析`: The operator does not create temporary views. Spark resolves tables and views already registered in the supplied session. 算子不会创建
      临时视图；Spark 从传入的会话中解析已注册的表和视图。
      * `SQL Execution SQL 执行`: The operator executes `spark.sql(sql_query)`. Spark parses, analyzes, optimizes, and executes the statement. 算子通过 `spark.sql(sql_query)`
      执行语句，由 Spark 完成解析、分析、优化和执行。

      ### Output Schema Final State 输出 Schema 最终态
      * `Dynamic Output Schema 动态输出 Schema`: For result-producing queries, output columns and data types are determined by the SQL projection and aliases. For other SQL
      statements, the returned DataFrame follows Spark SQL behavior. 对于产生查询结果的语句，输出字段及其类型由 SQL 投影和别名决定；其他 SQL 语句返回的 DataFrame 遵循 Spark
      SQL 的行为。

🏡 Back to [operator market 算子市场](../ops_market.md)