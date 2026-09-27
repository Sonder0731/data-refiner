# `HiveTableWriter` Operator

---

Save data to a table. 将数据保存到表中

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.writer.hive_table_writer`
- Class name 类名: `HiveTableWriter`
- Inherit from 继承于: [Writer](../meta_operator/writer.md)
- Test code 测试代码: [test code](../../../tests/ops/writer/test_hive_table_writer.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`dynamic_partition_overwrite`|`<class 'bool'>`|`False`|`False`|Enable dynamic partition overwrite. If True, the 'overwrite' mode will only overwrite the partitions contained in the DataFrame, not the entire table.||
|`external_location`|`Optional[str]`|`None`|`False`|The HDFS or S3 path to the external table. Valid only if is_external is True.||
|`format_type`|`Literal['parquet', 'orc', 'csv']`|`parquet`|`False`|For data storage format, we recommend using 'parquet' or 'orc'.||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`is_external`|`<class 'bool'>`|`False`|`False`|Whether to create an external table. If True, you must provide external_location.||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`options`|`Optional[dict]`|`None`|`False`|Additional options passed to the Spark Writer, such as {'compression': 'snappy'}.||
|`output_df`|`Optional[str]`|`None`|`False`|The output dataframe name. 输出 DataFrame 的名称||
|`partition_by`|`Optional[List[str]]`|`None`|`False`|A list of column names used for partitioning. For example, ['dt', 'country'].||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`save_mode`|`Literal['append', 'overwrite', 'errorifexists', 'ignore']`|`append`|`False`|Write modes: 'append', 'overwrite', 'errorifexists' (error if it exists), 'ignore' (ignore if it exists).||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`table_name`|`<class 'str'>`|`None`|`True`|The full name of the target Hive table, such as database.table_name.||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `partition_by` -> Partition Key Columns 分区键列: If this parameter list is provided, the input DataFrame must contain all specified partition columns. 如果提供了该参数列表，输入 DataFrame 必须包含所有指定的分区列。
  * Data Type 数据类型: `Any` 任意类型. The column data types can be any valid primitive Spark SQL types (e.g., `StringType()`, `IntegerType()`, `DateType()`) capable of acting as directional keys for Hive physical path directory slicing. 列数据类型可以是任何有效的 Spark SQL 基础类型（例如 `StringType()`, `IntegerType()`, `DateType()`），能够充当 Hive 物理路径目录切分的定向键。

### Argument and Column Mapping 参数与列的映射
* `table_name` -> Target Persistence Catalog 目标持久化目录: This parameter dictates the logical database and table routing where the input layout columns are permanently mapped. 该参数决定了输入结构列永久映射的逻辑数据库和表路由。
* `partition_by` -> Structural Partitioning Column Keys 结构化分区列键: This parameter maps to a specific subset of the input schema columns to partition the physical output structure on storage blocks. 该参数映射到输入 schema 列的一个特定子集，以便在存储块上对物理输出结构进行分区。
* `dynamic_partition_overwrite` -> Runtime Partition Overwrite Strategy 运行时分区覆盖策略: When `save_mode` is `"overwrite"`, this configuration argument overrides Spark session variables to selectively replace data subsets matching the runtime evaluation of the column values. 当 `save_mode` 为 `"overwrite"` 时，该配置参数覆盖 Spark 会话变量，以选择性地替换与列值的运行时评估相匹配的数据子集。

### Schema Transformation Process Schema 转换过程
* Phase 1: Context Interception and Dynamic Overwrite Alignment 阶段 1：上下文拦截与动态覆盖对齐
  * The operator intercepts global Spark options by querying `save_mode` and `dynamic_partition_overwrite`. 算子通过查询 `save_mode` 和 `dynamic_partition_overwrite` 来拦截全局 Spark 选项。
  * If both evaluate positively, `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")` is executed, changing the runtime catalog engine's transactional layout evaluation without altering the DataFrame column sequence. 如果两者均评估为真，则执行 `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")`，在不改变 DataFrame 列顺序的情况下改变运行时目录引擎的事务布局评估。
* Phase 2: Schema Integrity Verification 阶段 2：Schema 完整性校验
  * Programmatic lookup logic validates that all column text strings inside `partition_by` physically exist as native attributes inside `df.columns` to block corrupted data serialization down stream. 程序化查找逻辑验证 `partition_by` 中的所有列文本字符串在物理上作为原生属性存在于 `df.columns` 中，以阻止下游损坏的数据序列化。
* Phase 3: Catalyst Writer Instantiation and Target Saving 阶段 3：Catalyst 写入器实例化与目标保存
  * A `DataFrameWriter` instance is chain-constructed via `df.write.mode().format()`, binding configuration options including serialization layout format types, external paths, and storage parameters. 一个 `DataFrameWriter` 实例通过 `df.write.mode().format()` 链式构建，绑定了包括序列化布局格式类型、外部路径和存储参数在内的配置选项。
  * The final persistent storage execution invokes `.saveAsTable()`, tracking and pushing the active schema layer directly into the active Hive storage layer without executing runtime schema modifications or data conversions. 最终的持久化存储执行调用 `.saveAsTable()`，直接跟踪并推入当前 schema 层到当前的 Hive 存储层，而不执行运行时 schema 修改或数据转换。

### Output Schema Final State 输出 Schema 最终态
* Original Passthrough Schema 原始透传 Schema: The operator strictly returns the identical input DataFrame instance untouched, retaining the exact structural names, sequence order, nullability settings, and primitive data types. 算子严格返回未触动的相同输入 DataFrame 实例，保留准确的结构名称、排列顺序、可空性设置和基础数据类型。
  * Although data bytes are exported and written onto physical cluster storage, the active programmatic Spark DataFrame schema pipeline remains completely unaltered. 尽管数据字节被导出并写入物理集群存储，但当前的程序化 Spark DataFrame schema 管道保持完全不发生改变。

🏡 Back to [operator market 算子市场](../ops_market.md)