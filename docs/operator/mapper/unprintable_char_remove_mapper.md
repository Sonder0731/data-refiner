# `UnprintableCharRemoveMapper` Operator

---

Remove unprintable characters from the input text. 从输入文本中移除不可打印字符

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.unprintable_char_remove_mapper`
- Class name 类名: `UnprintableCharRemoveMapper`
- Inherit from 继承于: [SimpleMapper](../meta_operator/simple_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_unprintable_char_remove_mapper.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`exclude_chars`|`<class 'str'>`|`
`|`False`|Characters to exclude from removal.||
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
* `field` -> Target Source Column 目标源列: The input DataFrame must contain this specified source text column. 输入 DataFrame 必须包含此指定的源文本列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column must contain string data that potentially includes unprintable characters requiring cleaning. 该列必须包含可能含有需要清洗的不可打印字符的字符串数据。

### Argument and Column Mapping 参数与列的映射
* `field` -> Raw Text Column 原始文本列: This parameter defines the name of the input column containing the raw string data to be filtered. 该参数指定包含待过滤原始字符串数据的输入列名称。
* `output_field` -> Cleaned Text Column 清洗后文本列: This parameter defines the name of the generated column holding the cleaned string data. 该参数指定存放清洗后字符串数据的生成列名称。
* `exclude_chars` -> Exclusion Characters Argument 排除字符参数: This configuration parameter defines a set of specific characters that must be preserved even if they are non-printable. 该配置参数定义了一组特定字符，即使它们属于不可打印字符也必须予以保留。

### Schema Transformation Process Schema 转换过程
* Phase 1: UDF Registration and Internal Evaluation 阶段 1：UDF 注册与内部求值
  * A Spark User-Defined Function (UDF) is dynamically instantiated with an explicit return type of `StringType()`. 一个显式返回类型为 `StringType()` 的 Spark 用户自定义函数 (UDF) 被动态实例化。
  * The UDF evaluates each character within the column defined by `field`, filtering out characters based on Python's `.isprintable()` logic while exempting characters defined in `exclude_chars`. 该 UDF 对由 `field` 定义的列中的每个字符进行求值，根据 Python 的 `.isprintable()` 逻辑过滤掉字符，同时豁免 `exclude_chars` 中定义的字符。
* Phase 2: Schema Expansion via column addition 阶段 2：通过列追加进行 Schema 扩展
  * The catalyst optimizer processes the `.withColumn()` operator to append the new field to the DataFrame schema. Catalyst 优化器处理 `.withColumn()` 算子，将新字段追加到 DataFrame schema 中。
  * The column specified by `output_field` is explicitly integrated into the schema definition as a `StringType()` column. 由 `output_field` 指定的列作为 `StringType()` 列被显式整合到 schema 定义中。

### Output Schema Final State 输出 Schema 最终态
* Original Columns 原始列: All schema columns existing in the input DataFrame are retained with their original positions, schemas, and data types. 输入 DataFrame 中存在的所有 Schema 列都将保留其原始位置、结构和数据类型。
* `output_field` -> Cleaned Text Column 清洗后文本列: The final generated output column containing the processed text. 最终生成的包含已处理文本的输出列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column represents the final sanitized string where unprintable characters have been stripped out. 该列代表已去除不可打印字符的最终净化后的字符串。

🏡 Back to [operator market 算子市场](../ops_market.md)