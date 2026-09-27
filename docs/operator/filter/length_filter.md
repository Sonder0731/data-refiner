# `LengthFilter` Operator

---

Filter the text by its length. 按文本长度筛选文本

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.filter.length_filter`
- Class name 类名: `LengthFilter`
- Inherit from 继承于: [Filter](../meta_operator/filter.md)
- Test code 测试代码: [test code](../../../tests/ops/filter/test_length_filter.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`field`|`<class 'str'>`|`None`|`True`|The field name (column name in dataframe) to process. This parameter may have special meaning in some operators; see the specific operator document. 要处理的字段名（DataFrame 列名）；在部分算子中该参数可能有特殊含义，请参考具体算子文档||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`max_length`|`<class 'int'>`|`None`|`False`|max_length (int): Maximum length of the text.||
|`min_length`|`<class 'int'>`|`None`|`False`|min_length (int): Minimum length of the text.||
|`mode`|`<class 'str'>`|`filter`|`False`|The filter mode. 过滤模式|- `tag`<br/>tag only 仅打标<br/>- `filter`<br/>filter rows 过滤数据行<br/>- `tag_and_filter`<br/>tag and filter 打标并过滤<br/>|
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`tag_field`|`<class 'str'>`|`tag`|`False`|The tag field name (column name). 标签字段名（列名）||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operator directly depends on the target text column specified by `field`. 该算子直接依赖于由 `field` 指定的目标文本列。
* Required Data Types 要求的数据类型: The column specified by `field` must be of `StringType()`, which is strictly validated by the `check_column_schema` method before execution. 由 `field` 指定的列必须为 `StringType()`，在执行前通过 `check_column_schema` 方法进行严格验证。

### Argument and Column Mapping 参数与列的映射
* `field` -> Inspected Text Column 被检查文本列: This parameter maps to the specific column whose text character length is calculated and validated against the length bounds. 该参数映射到特定的列，其文本字符长度将被计算并根据长度边界进行验证。
* `min_length` & `max_length` -> Range Constraints 范围约束: These parameters define the conditional logic thresholds applied to the string length evaluation of the mapped `field`. 这些参数定义了应用于被映射 `field` 的字符串长度评估的条件逻辑阈值。
* `tag_field` -> Result Boolean Column 结果布尔列: This parameter defines the name of the new column created to store the boolean evaluation result of the UDF filter. 该参数定义了新创建的列名，用于存储 UDF 过滤器布尔评估的结果。
  * When text length satisfies `min_length <= length <= max_length` 当文本长度满足 `min_length <= length <= max_length` 时: The row value in `tag_field` resolves to `True`. `tag_field` 中的行值解析为 `True`。
  * When text length violates the bounds 当文本长度违反边界时: The row value in `tag_field` resolves to `False`. `tag_field` 中的行值解析为 `False`。
* `mode` -> Filtering Action Mode 过滤行为模式: This parameter governs how rows are pruned or retained based on the boolean status inside `tag_field` via the `return_df_by_filter_level` routine. 该参数控制如何通过 `return_df_by_filter_level` 例程基于 `tag_field` 内的布尔状态对行进行剪枝或保留。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: A user-defined function `length_filter_udf` mapping to `BooleanType()` is executed via `withColumn`, appending a new intermediate boolean column named after `tag_field` to the current DataFrame. 一个映射到 `BooleanType()` 的用户自定义函数 `length_filter_udf` 通过 `withColumn` 被执行，向当前 DataFrame 追加一个以 `tag_field` 命名的全新中间布尔列。
* Type Modifications 类型改变: The schema of the existing columns remains unmodified, while the schema structural width is expanded by exactly one `BooleanType` column. 现有列的 Schema 保持未修改状态，而 Schema 的结构宽度恰好扩展了一个 `BooleanType` 列。
* Elimination of Intermediate Columns 中间列的消除: The intermediate `tag_field` column is passed into `return_df_by_filter_level`, which may drop this temporary indicator column prior to delivering the final structural output. 中间 `tag_field` 列被传入 `return_df_by_filter_level` 中，该函数可能会在交付最终结构输出之前删除此临时指示列。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: The final DataFrame maintains the identical schema structure as the input dataset, while the generated `tag_field` is stripped or retained depending on the downstream configuration of the filter level utility. 最终的 DataFrame 保持与输入数据集完全相同的 Schema 结构，而生成的 `tag_field` 则取决于过滤级别工具的下游配置而被剥离或保留。
* Final Column Data Types 最终列数据类型: Every column present in the final output retains its initial data type (e.g., `field` strictly remains `StringType()`) without any type distortion. 最终输出中存在的每一列都保留其初始数据类型（例如，`field` 严格保持为 `StringType()`），没有任何类型畸变。

🏡 Back to [operator market 算子市场](../ops_market.md)