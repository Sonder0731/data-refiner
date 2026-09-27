# `NullRowsFilter` Operator

---

Remove rows that contain any null value across specified or all columns. 删除指定列或所有列中包含空值的行

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.filter.null_rows_filter`
- Class name 类名: `NullRowsFilter`
- Inherit from 继承于: [Filter](../meta_operator/filter.md)
- Test code 测试代码: [test code](../../../tests/ops/filter/test_null_rows_filter.py)
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
|`fields`|`list[str]`|`None`|`False`|List of columns to check for null values. If None, checks all columns.||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
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
* Dependent Initial Columns 依赖的初始列: The operation depends on the columns specified in `columns_to_check`. If `columns_to_check` is `None`, it depends on all columns present in the input DataFrame. 该操作依赖于 `columns_to_check` 中指定的列。如果 `columns_to_check` 为 `None`，则依赖于输入 DataFrame 中存在的所有列。
* Required Data Types 要求的数据类型: All target columns to be checked support any valid Spark DataType, as the `isNotNull()` operator is universally applicable. 所有被检查的目标列支持任何有效的 Spark 数据类型，因为 `isNotNull()` 算子是普遍适用的。

### Argument and Column Mapping 参数与列的映射
* `columns_to_check` -> Checked Columns 被检查列: This parameter explicitly defines the subset of columns to be evaluated for null values. 该参数显式定义了需要评估空值的列子集。
  * When `columns_to_check` is provided 当提供 `columns_to_check` 时: The operator filters rows based only on the specified list of columns. 算子仅基于指定的列列表过滤行。
  * When `columns_to_check` is `None` 当 `columns_to_check` 为 `None` 时: The operator dynamically binds to all columns in the input DataFrame schema. 算子动态绑定到输入 DataFrame Schema 中的所有列。
* `tag_field` -> Intermediate Tag Column 中间标记列: This parameter specifies the name of the intermediate boolean column used to store the filter condition evaluation result. 该参数指定用于存储过滤条件评估结果的中间布尔列的名称。
* `mode` -> Filtering Strategy 行过滤策略: This parameter determines how rows are retained or discarded based on the boolean value in `tag_field`. 该参数决定如何基于 `tag_field` 中的布尔值保留或丢弃行。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: A temporary boolean column named after `tag_field` is appended to the DataFrame via the `withColumn` operator, holding the result of the combined non-null conditions. 一个以 `tag_field` 命名的临时布尔列通过 `withColumn` 算子被追加到 DataFrame 中，保存组合非空条件的结果。
* Elimination of Intermediate Columns 中间列的消除: During the execution of `return_df_by_filter_level`, the intermediate `tag_field` column may be dropped or handled depending on the internal logic of the filtering level helper, returning the final cleansed dataset. 在 `return_df_by_filter_level` 的执行过程中，中间的 `tag_field` 列可能会根据过滤级别辅助函数的内部逻辑被删除或处理，从而返回最终清洗后的数据集。
* Type Modifications 类型改变: No existing columns undergo data type modifications during the lifecycle of this operator. 在该算子的生命周期内，没有任何现有列会经历数据类型改变。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: The final DataFrame contains the same set of business schema columns as the input DataFrame, while the temporary `tag_field` column is processed and excluded from the definitive output schema. 最终的 DataFrame 包含与输入 DataFrame 相同的业务 Schema 列集合，而临时 `tag_field` 列已被处理并排除在最终输出 Schema 之外。
* Final Column Data Types 最终列数据类型: All retained columns strictly preserve their original input data types without any modification. 所有保留的列严格保持其原始的输入数据类型，未发生任何改变。

🏡 Back to [operator market 算子市场](../ops_market.md)