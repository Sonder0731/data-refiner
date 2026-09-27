# `EditDistanceMapper` Operator

---

Calculate the edit distance between two strings. 计算两个字符串之间的编辑距离

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.edit_distance_mapper`
- Class name 类名: `EditDistanceMapper`
- Inherit from 继承于: [MultiInSingleOutMapper](../meta_operator/multi_in_single_out_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_edit_distance_mapper.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`fields`|`List[str]`|`None`|`True`|The list of field names to map on. 要进行映射的字段名列表||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`output_field`|`<class 'str'>`|`None`|`True`|The output field name after transforming. 转换后的输出字段名||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `fields`: The input DataFrame must contain all columns specified within this parameters list, and every individual column's data type must be `StringType`. 输入 DataFrame 必须包含该参数列表中指定的所有列，且每个单独列的数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `fields` -> Input Columns 输入列: This parameter specifies an ordered collection of exactly two source string columns used as arguments to compute the edit distance. 该参数指定了一个包含恰好两个源字符串列的有序集合，用作计算编辑距离的参数。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the calculated edit distance integer values will be stored. 该参数定义了存储计算出的编辑距离整数值的新列的列名。

### Schema Transformation Process Schema 转换过程
* A loop iterates through the collection defined by `fields` to validate that each target column strictly matches the `StringType` requirement. 循环遍历由 `fields` 定义的集合，以验证每个目标列 sampled 是否严格符合 `StringType` 要求。
* A User Defined Function (UDF) named `calc_edit_distance_udf` is registered, which encapsulates the `calculate_edit_distance` evaluation logic and explicitly sets its return type to `IntegerType()`. 注册了一个名为 `calc_edit_distance_udf` 的用户自定义函数（UDF），该函数封装了 `calculate_edit_distance` 评估逻辑，并将其返回类型明确设置为 `IntegerType()`。
* The `withColumn` operator is invoked on the DataFrame, passing the unpacked columns from `fields` into the UDF to append the new column designated by `output_field`. 在 DataFrame 上调用 `withColumn` 算子，将 `fields` 中解包的列传入 UDF 中，以追加由 `output_field` 指定的新列。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly structured as `IntegerType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确构建为 `IntegerType`。

🏡 Back to [operator market 算子市场](../ops_market.md)