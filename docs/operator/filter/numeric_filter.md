# `NumericFilter` Operator

---

Filter rows based on a numeric column comparison. 按数值列进行过滤<br><br>    Supports comparison operators: ``<``, ``>``, ``<=``, ``>=``, ``==``.<br>    The target column must be a numeric type (int, long, float, double, decimal).

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.filter.numeric_filter`
- Class name 类名: `NumericFilter`
- Inherit from 继承于: [Filter](../meta_operator/filter.md)
- Test code 测试代码: [test code](../../../tests/ops/filter/test_numeric_filter.py)
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
|`mode`|`<class 'str'>`|`filter`|`False`|The filter mode. 过滤模式|- `tag`<br/>tag only 仅打标<br/>- `filter`<br/>filter rows 过滤数据行<br/>- `tag_and_filter`<br/>tag and filter 打标并过滤<br/>|
|`operator`|`Literal['lt', 'gt', 'le', 'ge', 'eq']`|`gt`|`False`|Comparison operator. 比较运算符。<br>  - `lt`: less than (<) 小于<br>  - `gt`: greater than (>) 大于<br>  - `le`: less than or equal (<=) 小于等于<br>  - `ge`: greater than or equal (>=) 大于等于<br>  - `eq`: equal (==) 等于||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`tag_field`|`<class 'str'>`|`tag`|`False`|The tag field name (column name). 标签字段名（列名）||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||
|`threshold`|`<class 'float'>`|`None`|`True`|The numeric threshold value to compare against. 用于比较的数值阈值（支持整数和小数）||


## Constraint 约束

### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operator directly relies on the specific numeric column specified by `field`. 该算子直接依赖于由 `field` 指定的特定数值列。
* Required Data Types 要求的数据类型: The column specified by `field` must be a numeric type (`IntegerType`, `LongType`, `FloatType`, `DoubleType`, or `DecimalType`), verified by `check_column_schema` before processing. 由 `field` 指定的列必须为数值类型（`IntegerType`、`LongType`、`FloatType`、`DoubleType` 或 `DecimalType`），在处理前由 `check_column_schema` 方法进行验证。

### Argument and Column Mapping 参数与列的映射
* `field` -> Inspected Numeric Column 被检查数值列: This parameter designates the target column whose numeric values are compared against `threshold` using the specified `operator`. 该参数指定目标列，其数值将使用指定的 `operator` 与 `threshold` 进行比较。
* `operator` -> Comparison Operator 比较运算符: Defines the arithmetic comparison to apply. Supported values are `lt` (<), `gt` (>), `le` (<=), `ge` (>=), `eq` (==). 定义要应用的算术比较。支持的值包括 `lt` (<)、`gt` (>)、`le` (<=)、`ge` (>=)、`eq` (==)。
* `threshold` -> Comparison Threshold 比较阈值: The numeric value (integer or decimal) to compare each row's `field` value against. 用于比较每行 `field` 值的数值（整数或小数）。
* `tag_field` -> Conditional Evaluation Column 条件评估列: This parameter names the appended intermediate boolean column that holds the outcome of the numeric comparison. 该参数命名了追加的中间布尔列，用于保存数值比较的结果。
  * When the row's value satisfies the operator-threshold condition 当行值满足运算符-阈值条件时: The entry value in `tag_field` evaluates to `True`. `tag_field` 中的条目值解析为 `True`。
  * When the row's value does NOT satisfy the condition 当行值不满足条件时: The entry value in `tag_field` evaluates to `False`. `tag_field` 中的条目值解析为 `False`。
* `mode` -> Row Retention Filter 过滤行保留模式: This parameter dictates the row-slicing actions applied within `return_df_by_filter_level` based on the values in `tag_field`. 该参数决定了在 `return_df_by_filter_level` 中基于 `tag_field` 中的值所执行的行切片操作。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: A user-defined function mapping to `BooleanType()` is executed via `withColumn`, appending a new intermediate boolean column named after `tag_field`. 一个映射到 `BooleanType()` 的用户自定义函数通过 `withColumn` 被执行，向当前 DataFrame 追加一个以 `tag_field` 命名的全新中间布尔列。
* Type Modifications 类型改变: The preexisting operational columns maintain their initial data types, and no inplace type transformations occur. 原有业务列保持其初始数据类型，未发生任何就地类型转换。
* Elimination of Intermediate Columns 中间列的消除: The intermediate `tag_field` column is sent to `return_df_by_filter_level`, where it might be structurally stripped out of the final DataFrame depending on the filtering level routine. 中间 `tag_field` 列被发送至 `return_df_by_filter_level`，在那里它可能会根据过滤级别例程从最终 DataFrame 中被结构化地剥离。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: The final DataFrame returns the baseline structural columns identical to the input schema, whereas the temporary indicator column `tag_field` is decoupled or omitted. 最终的 DataFrame 返回与输入 Schema 相同的基线结构列，而临时指示列 `tag_field` 则被解耦或省略。
* Final Column Data Types 最终列数据类型: Every baseline column preserves its incoming operational data type without structural modifications. 每一个基线列都保留其输入的业务数据类型，未发生结构性修改。

🏡 Back to [operator market 算子市场](../ops_market.md)