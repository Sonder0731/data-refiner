# `FieldTypeConverter` Operator

---

Converts the data types of multiple specified fields in a DataFrame. 转换 DataFrame 中多个指定字段的数据类型

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.other.field_type_converter`
- Class name 类名: `FieldTypeConverter`
- Inherit from 继承于: [InputOutputOperator](../meta_operator/input_output_operator.md)
- Test code 测试代码: [test code](../../../tests/ops/other/test_field_type_converter.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`field_type_mapping`|`<class 'dict'>`|`None`|`True`|A dictionary mapping field names to target Spark SQL data types as strings (e.g., {'age': 'integer', 'score': 'double'}).||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `field_type_mappingKeys` -> Target Source Columns 目标源列: The input DataFrame must contain the columns specified as keys in the type mapping dictionary. 输入 DataFrame 必须包含类型映射字典中作为键指定的列。
  * Data Type 数据类型: `Any` 任意类型. The initial data types of these columns can be any valid Spark SQL data types that support casting to the target data types. 这些列的初始数据类型可以是任何支持转换为目标数据类型的有效 Spark SQL 数据类型。

### Argument and Column Mapping 参数与列的映射
* `field_type_mapping` -> Type Conversion Mapping 类型转换映射: This parameter accepts a dictionary where keys represent existing column names and values represent the target Spark SQL data types as strings (e.g., `'integer'`, `'double'`, `'string'`). 该参数接收一个字典，其中键代表现有的列名，值代表字符串形式的目标 Spark SQL 数据类型（例如 `'integer'`, `'double'`, `'string'`）。

### Schema Transformation Process Schema 转换过程
* Phase 1: Schema Rebuilding and Target Type Resolution 阶段 1：Schema 重构与目标类型解析
  * The operator iterates over the fields of `original_schema` to build a `new_schema` using `StructType`. 算子遍历 `original_schema` 的字段，使用 `StructType` 构建一个 `new_schema`。
  * For each field matching a key in `field_type_mapping`, its data type is resolved via an internal map and replaced with the corresponding Spark `DataType`, while its original nullability property is preserved. 对于每个匹配 `field_type_mapping` 中键的字段，其数据类型通过内部映射进行解析，并替换为相应的 Spark `DataType`，同时保留其原始的可空性属性。
* Phase 2: Catalyst Expression Generation and Column Casting 阶段 2：Catalyst 表达式生成与列类型转换
  * A list of projection expressions is constructed by iterating through the DataFrame columns. 通过遍历 DataFrame 的列构建一个投影表达式列表。
  * For mapped columns, the `.cast()` operator is applied to convert the column to the resolved target Spark type, and `.alias()` ensures the original column name is retained; unmapped columns are passed through unchanged. 对于被映射的列，应用 `.cast()` 算子将该列转换为解析后的目标 Spark 类型，并使用 `.alias()` 确保保留原始列名；未被映射的列则保持不变直接传入。
  * The DataFrame executes a `.select()` transformation using these projection expressions to complete the type conversion without restructuring the top-level column order. DataFrame 使用这些投影表达式执行 `.select()` 转换，在不重构顶级列顺序的情况下完成类型转换。

### Output Schema Final State 输出 Schema 最终态
* Unmapped Columns 未映射列: Columns not specified in `field_type_mapping` are fully retained with their original data types and positions. 未在 `field_type_mapping` 中指定的列将完整保留其原始数据类型和位置。
* Mapped Columns 映射列: Columns specified in `field_type_mapping` retain their original names and positions, but their data types are updated. 在 `field_type_mapping` 中指定的列保留其原始名称和位置，但其数据类型会被更新。
  * Data Type 数据类型: `DataType` 目标数据类型. The final data types for these columns will strictly match the Spark SQL types resolved from the parameter values (e.g., `IntegerType()`, `DoubleType()`, `StringType()`). 这些列的最终数据类型将严格匹配从参数值解析出的 Spark SQL 类型（例如 `IntegerType()`, `DoubleType()`, `StringType()`）。

🏡 Back to [operator market 算子市场](../ops_market.md)