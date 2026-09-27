# `DuplicateParagraphChrFractionMapper` Operator

---

Calculate the character fraction of duplicate paragraphs. 计算重复段落的字符数占比

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.duplicate_paragraph_chr_fraction_mapper`
- Class name 类名: `DuplicateParagraphChrFractionMapper`
- Inherit from 继承于: [SimpleMapper](../meta_operator/simple_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_duplicate_paragraph_chr_fraction_mapper.py)
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
* `field` -> Input Column 输入列: The component requires an input DataFrame containing at least one specific column designated by the `field` parameter. 该组件要求输入 DataFrame 至少包含一个由 `field` 参数指定的特定列。
  * Data Type 数据类型: `StringType` 字符串类型. The text in this column will be segmented by double newlines to analyze duplicate paragraphs. 该列中的文本将被双换行符分割以分析重复段落。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Column 源列: This parameter specifies the name of the input column containing the raw text data to be processed. 该参数指定包含待处理原始文本数据的输入列名称。
* `output_field` -> Target Column 目标列: This parameter defines the name of the new column where the calculated duplicate paragraph character fraction will be stored. 该参数定义了用于存储计算出的重复段落字符数占比的新列名称。

### Schema Transformation Process Schema 转换过程
* Column Addition 列添加: During the execution of the `process` method, a new column specified by `output_field` is appended to the DataFrame. 在 `process` 方法的执行过程中，一个由 `output_field` 指定的新列会被追加到 DataFrame 中。
* UDF Execution UDF 执行: The user-defined function `duplicate_paragraph_chr_fraction_udf` processes the string values from `field` row by row, computes the ratio of characters in duplicate paragraphs, and writes the resulting float values into the `output_field` column. 用户自定义函数 `duplicate_paragraph_chr_fraction_udf` 逐行处理 `field` 中的字符串值，计算重复段落的字符占比，并将生成的浮点数值写入 `output_field` 列。
* Schema Retention Schema 保留: All original columns from the input DataFrame are preserved without any deletion or data type modification. 输入 DataFrame 的所有原始列均被保留，未进行任何删除或数据类型修改。

### Output Schema Final State 输出 Schema 最终态
* Retained Columns 保留列: All columns present in the input DataFrame remain unchanged in the output DataFrame. 输入 DataFrame 中存在的所有列在输出 DataFrame 中均保持不变。
* New Column 新增列: A new column named after the value of `output_field` is successfully added. 成功添加了一个以 `output_field` 的值命名的新列。
  * Data Type 数据类型: `FloatType` 浮点型. This column stores the calculated fraction of characters belonging to duplicate paragraphs as a float value between 0.0 and 1.0. 该列将属于重复段落的字符数计算占比存储为 0.0 到 1.0 之间的浮点数值。

🏡 Back to [operator market 算子市场](../ops_market.md)