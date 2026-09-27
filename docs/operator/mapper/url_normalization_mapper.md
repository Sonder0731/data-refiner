# `UrlNormalizationMapper` Operator

---

Normalize a URL field: lowercase scheme/host, remove default ports, sort query params, drop empty params and fragments, resolve path. 规范化 URL 字段：将协议/主机名转换为小写，移除默认端口，对查询参数进行排序，丢弃片段，解析路径

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.url_normalization_mapper`
- Class name 类名: `UrlNormalizationMapper`
- Inherit from 继承于: [SimpleMapper](../meta_operator/simple_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_url_normalization_mapper.py)
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


## Constraint 约束

### Input Column Constraints 输入列约束
* `field` -> Target Source Column 目标源列: The input DataFrame must contain this specified URL column. 输入 DataFrame 必须包含此指定的 URL 列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column must contain raw URL strings to be processed by the normalization routine. 该列必须包含待由规范化程序处理的原始 URL 字符串。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source URL Column 源 URL 列: This parameter defines the name of the input column containing the unnormalized URL strings. 该参数指定包含未规范化 URL 字符串的输入列名称。
* `output_field` -> Normalized URL Column 规范化后 URL 列: This parameter defines the name of the generated column holding the standardized URL strings. 该参数指定存放标准化后 URL 字符串的生成列名称。

### Schema Transformation Process Schema 转换过程
* Phase 1: UDF Evaluation and URL Standardization 阶段 1：UDF 求值与 URL 标准化
  * A Spark User-Defined Function (UDF) is instantiated with a declared return type of `StringType()` to encapsulate the URL standardization logic. 一个显式声明返回类型为 `StringType()` 的 Spark 用户自定义函数 (UDF) 被实例化，用以封装 URL 标准化逻辑。
  * The internal logic performs string manipulations including downcasing the scheme/host, removing default network ports (80/443), sorting query strings while filtering empty parameters, discarding URL fragments, and resolving path trajectories. 内部逻辑执行字符串操作，包括将协议/主机名转换为小写、移除默认网络端口 (80/443)、在过滤空参数的同时对查询字符串进行排序、丢弃 URL 片段以及解析路径轨迹。
* Phase 2: Schema Expansion via Column Append 阶段 2：通过列追加进行 Schema 扩展
  * The Catalyst optimizer evaluates the `.withColumn()` operator to safely append the new field to the active DataFrame schema. Catalyst 优化器求值 `.withColumn()` 算子，将新字段安全地追加到当前 DataFrame schema 中。
  * The column specified by `output_field` is formally registered in the schema metadata as a standard nullable `StringType()` field. 由 `output_field` 指定的列作为标准的、可空的 `StringType()` 字段正式注册到 schema 元数据中。

### Output Schema Final State 输出 Schema 最终态
* Original Columns 原始列: All schema columns present in the initial input DataFrame are fully preserved in their original sequence and data types. 初始输入 DataFrame 中存在的所有 Schema 列都将完整保留其原始顺序和数据类型。
* `output_field` -> Normalized URL Column 规范化后 URL 列: The final generated output column containing the fully transformed and uniform URL text. 最终生成的包含完全转换且统一的 URL 文本的输出列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column holds the structurally normalized, clean URL strings. 该列存放结构规范化、清洗后的 URL 字符串。

🏡 Back to [operator market 算子市场](../ops_market.md)