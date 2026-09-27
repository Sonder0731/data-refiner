# `GarbledTextFilter` Operator

---

Identifying garbled text in a given field using a binary classification model trained by fasttext. 使用 fasttext 训练的二元分类模型判断乱码文本

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.filter.garbled_text_filter`
- Class name 类名: `GarbledTextFilter`
- Inherit from 继承于: [Filter](../meta_operator/filter.md)
- Test code 测试代码: [test code](../../../tests/ops/filter/test_garbled_text_filter.py)
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
|`label_prefix`|`<class 'str'>`|`__label__`|`False`|The prefix of the label in the model output.||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`mode`|`<class 'str'>`|`filter`|`False`|The filter mode. 过滤模式|- `tag`<br/>tag only 仅打标<br/>- `filter`<br/>filter rows 过滤数据行<br/>- `tag_and_filter`<br/>tag and filter 打标并过滤<br/>|
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`tag_field`|`<class 'str'>`|`is_garbled`|`False`|The name of the new field to indicate if the text is garbled or not.||
|`tag_field`|`<class 'str'>`|`tag`|`False`|The tag field name (column name). 标签字段名（列名）||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||
|`threshold`|`<class 'float'>`|`0.5`|`False`|The threshold of the model output, if the output is lower than the threshold, the text is considered as garbled.||


## Constraint 约束

### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operation strictly depends on the specific source text column defined by `field`. 该操作严格依赖于由 `field` 指定的特定源文本列。
* Required Data Types 要求的数据类型: The column specified by `field` must be of `StringType()`, which is explicitly verified by the `check_column_schema` validator. 由 `field` 指定的列必须为 `StringType()`，这通过 `check_column_schema` 校验器进行了显式验证。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Text Column 源文本列: This parameter determines the target string column inside the Spark partition rows extracted for fasttext model inference. 该参数决定了在 Spark 分区行中提取并用于 fasttext 模型推理的目标字符串列。
* `tag_field` -> Identification Tag Column 识别标记列: This parameter defines the name of the newly injected boolean column that indicates whether the text is classified as garbled. 该参数定义了新注入的布尔列的名称，用于指示文本是否被分类为乱码。
  * When `grade < threshold` 当 `grade < threshold` 时: The corresponding row in `tag_field` is set to `True`. `tag_field` 中的对应行被设置为 `True`。
  * When `grade >= threshold` 当 `grade >= threshold` 时: The corresponding row in `tag_field` is set to `False`. `tag_field` 中的对应行被设置为 `False`。
* `mode` -> Filtering Execution Mode 过滤执行模式: This parameter dictates how rows are sliced or retained using `return_df_by_filter_level` based on the values in `tag_field`. 该参数决定了如何基于 `tag_field` 中的值，通过 `return_df_by_filter_level` 对行进行切片或保留。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: Within the `mapPartitions` RDD transformation, each row dictionary is modified to include a new boolean key-value pair under the name of `tag_field`, expanding the internal record structure. 在 `mapPartitions` RDD 转换 rows 中，每个行字典都被修改，以在 `tag_field` 的名称下包含一个新的布尔键值对，从而扩展了内部记录结构。
* Type Modifications 类型改变: The RDD is converted back to a DataFrame using `rdd.toDF()`, which implicitly dynamically infers the updated schema, changing the Schema status by appending `tag_field` as a `BooleanType` column. RDD 通过 `rdd.toDF()` 转换回 DataFrame，这隐式地动态推导了更新后的 Schema，通过追加 `tag_field` 作为 `BooleanType` 列改变了 Schema 状态。
* Elimination of Intermediate Columns 中间列的消除: The intermediate `tag_field` column is processed within `return_df_by_filter_level`, and depending on the filter strategy settings, it may be dropped from the final structural presentation. 中间 `tag_field` 列在 `return_df_by_filter_level` 中被处理，并且根据过滤策略设置，它可能会从最终的结构呈现中被删除。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: The final DataFrame outputs the original business columns, while the conditional `tag_field` is decoupled or stripped according to the operational architecture of the filter level utility. 最终的 DataFrame 输出原始业务列，而条件 `tag_field` 则根据过滤级别工具的运行架构被解耦或剥离。
* Final Column Data Types 最终列数据类型: All preexisting columns retain their strict input types (e.g., `field` remains `StringType()`), ensuring structural consistency for subsequent pipeline stages. 所有先前存在的列都保持其严格的输入类型（例如，`field` 保持为 `StringType()`），从而确保后续流水线阶段的结构一致性。

🏡 Back to [operator market 算子市场](../ops_market.md)