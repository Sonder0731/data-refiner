# `HiveReader` Operator

---

This class reads table data from Hive. 从 Hive 读取表数据

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.reader.hive_reader`
- Class name 类名: `HiveReader`
- Inherit from 继承于: [TableReader](../meta_operator/table_reader.md)
- Test code 测试代码: [test code](../../../tests/ops/reader/test_hive_reader.py)
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
|`table_name`|`<class 'str'>`|`None`|`True`|The input table name. 输入表名||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `table_name` -> Target Hive Table 目标 Hive 表: The external Hive metastore must contain the table schema pointed to by this identifier. 外部 Hive 元数据仓储必须包含此标识符所指向的表结构。
  * Data Type 数据类型: `Any` 任意类型. The input dataset originates from a physical persistent storage engine, and its schema can contain any valid Spark SQL compliant data types defined during table creation. 输入数据集源自物理持久化存储引擎，其结构可以包含建表时定义的任何有效且兼容 Spark SQL 的数据类型。

### Argument and Column Mapping 参数与列的映射
* `table_name` -> Hive Catalog Identifier Hive 目录标识符: This parameter specifies the qualified database and table name string used by the Spark session to resolve columns. 该参数指定 Spark 会话用于解析列的限定数据库和表名字符串。
  * No explicit parameter-to-column mutation occurs inside this operator; instead, the parameter maps to the entire set of existing structural fields defined within the target Hive table definition. 在该算子内部不发生显式的“参数-列”变动；相反，该参数直接映射到目标 Hive 表定义中所包含的全部现有结构化字段集合。

### Schema Transformation Process Schema 转换过程
* Phase 1: Metastore Schema Resolution 阶段 1：元数据仓储 Schema 解析
  * The operator invokes `spark.table()`, passing the identifier token specified by `table_name` to trigger a catalog lookup. 算子调用 `spark.table()`，将 `table_name` 指定的标识符标记传递给 Spark 以触发目录查找。
  * The Spark Catalyst analyzer connects to the Hive metastore to fetch the pre-existing column names and their corresponding structural types. Spark Catalyst 分析器连接到 Hive 元数据仓储，以获取预先存在的列名及其对应的结构类型。
* Phase 2: Execution Plan Binding 阶段 2：执行计划绑定
  * The retrieved schema definition is directly bound to the returned DataFrame without undergoing any internal programmatic modifications, inline filtering, or runtime column casting. 获取的 schema 定义直接绑定到返回的 DataFrame，不经历任何内部程序化修改、行内过滤或运行时列类型转换。

### Output Schema Final State 输出 Schema 最终态
* Metastore Defined Columns 元数据仓储定义列: The final schema structure is an exact reflection of the columns registered inside the external Hive system. 最终的 schema 结构完全反映了注册在外部 Hive 系统内部的列。
  * Data Type 数据类型: `StructType` 结构类型. The final output DataFrame contains all persistent fields, preserving their exact names, positions, nullability configurations, and underlying data types as specified in the Hive metadata catalog. 最终输出的 DataFrame 包含所有持久化字段，并完整保留 Hive 元数据目录中指定的准确名称、位置、可空性配置以及底层数据类型。

🏡 Back to [operator market 算子市场](../ops_market.md)