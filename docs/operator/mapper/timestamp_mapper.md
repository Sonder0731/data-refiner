# `TimestampMapper` Operator

---

Converts a datetime string column to a Unix timestamp (seconds since epoch). 将日期时间字符串转换为 Unix 时间戳（自 Unix 纪元以来的秒数）

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.timestamp_mapper`
- Class name 类名: `TimestampMapper`
- Inherit from 继承于: [SimpleMapper](../meta_operator/simple_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_timestamp_mapper.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`field`|`<class 'str'>`|`None`|`True`|The field name to map on. 要进行映射的字段名||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`output_field`|`<class 'str'>`|`None`|`True`|The field name to store the mapped value. 用于存储映射结果的字段名||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||
|`tz`|`<class 'str'>`|`UTC`|`False`|No description 无描述||


## Constraint 约束

### Input Column Constraints 输入列约束
* `field` -> Target Source Column 目标源列: The input DataFrame must contain this specified datetime string column. 输入 DataFrame 必须包含此指定的日期时间字符串列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column must contain standard or custom datetime formatted strings parseable by the dateutil parser. 该列必须包含可被 dateutil 解析器解析的标准或自定义日期时间格式字符串。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Datetime Column 源日期时间列: This parameter defines the name of the input column containing datetime strings to be converted. 该参数指定包含待转换日期时间字符串的输入列名称。
* `output_field` -> Result Timestamp Column 结果时间戳列: This parameter defines the name of the newly generated column holding the converted Unix timestamp. 该参数指定新生成的列名称，用于存放转换后的 Unix 时间戳。
* `tz` -> Timezone Argument 时区参数: This configuration parameter determines the target timezone applied to the parsed datetime object before extracting the epoch timestamp. 该配置参数决定在提取自历元以来的时间戳之前，应用于已解析日期时间对象的目标时区。

### Schema Transformation Process Schema 转换过程
* Phase 1: Partition-Level RDD Transformation 阶段 1：分区级 RDD 转换
  * The transformation executes via an RDD mapPartitions operation, creating an intermediate state where a new dictionary key specified by `output_field` is dynamically injected into each row dictionary. 转换通过 RDD mapPartitions 操作执行，创建一个中间状态，在此状态下，由 `output_field` 指定的新字典键被动态注入到每行字典中。
  * If parsing succeeds, the key maps to an `IntegerType()` representing the Unix timestamp; if parsing fails or the input value is missing, it maps to `None` (Null). 如果解析成功，该键映射为代表 Unix 时间戳的 `IntegerType()`；如果解析失败或输入值缺失，则映射为 `None` (空值)。
* Phase 2: Schema Evolution with explicit Typing 阶段 2：带有显式类型的 Schema 演变
  * The final DataFrame Schema is explicitly derived by invoking the `.add()` method on the original input schema. 最终的 DataFrame Schema 是通过对原始输入 schema 调用 `.add()` 方法显式派生出来的。
  * The column mapping defined by `output_field` is strictly registered as a `LongType()` field within the Spark engine. 由 `output_field` 定义的列映射在 Spark 引擎中被严格注册为 `LongType()` 字段。

### Output Schema Final State 输出 Schema 最终态
* Original Columns 原始列: All schema columns existing in the input DataFrame are retained with their original positions and data types. 输入 DataFrame 中存在的所有 Schema 列都将保留其原始位置和数据类型。
* `output_field` -> Epoch Timestamp Column 历元时间戳列: The final appended output column containing the calculated Unix timestamp. 最终追加的包含计算出的 Unix 时间戳的输出列。
  * Data Type 数据类型: `LongType()` 长整型. This column represents the standard 64-bit integer Unix timestamp denoting seconds elapsed since Jan 01 1970 (UTC). 该列代表标准的 64 位整型 Unix 时间戳，表示自 1970 年 1 月 1 日 (UTC) 以来流逝的秒数。

🏡 Back to [operator market 算子市场](../ops_market.md)