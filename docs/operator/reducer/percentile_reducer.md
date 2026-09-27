# `PercentileReducer` Operator

---

Calculates the percentile of a numeric column and returns a single-row DataFrame with the result. 计算数值列的百分位数，并返回包含结果的单行 DataFrame

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.reducer.percentile_reducer`
- Class name 类名: `PercentileReducer`
- Inherit from 继承于: [Reducer](../meta_operator/reducer.md)
- Test code 测试代码: [test code](../../../tests/ops/reducer/test_percentile_reducer.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`field`|`<class 'str'>`|`None`|`True`|The field name used for reduction. 用于归并计算的字段名||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`percentile`|`<class 'float'>`|`0.95`|`False`|The percentile to calculate. Must be between 0 and 1.||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `field` -> Target Numeric Column 目标数值列: The input DataFrame must contain this specified numeric column. 输入 DataFrame 必须包含此指定的数值列。
  * Data Type 数据类型: `IntegerType()`, `LongType()`, `FloatType()`, or `DoubleType()` 整型、长整型、单精度浮点型或双精度浮点型. This column must consist of numeric values to support the approximation histogram calculation. 该列必须由数值组成，以支持近似直方图计算。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Metric and Result Column 源指标与结果列: This parameter defines the name of the input column to evaluate, which also dictates the name of the single generated column in the output DataFrame. 该参数指定要评估的输入列名称，它同时也决定了输出 DataFrame 中生成的单个列的名称。
* `percentile` -> Rank Fraction Parameter 百分位数分数参数: This configuration parameter determines the exact mathematical percentile rank (between 0 and 1) to approximate using the internal histogram aggregation. 该配置参数决定了内部直方图聚合要近似计算的准确数学百分位数排名（介于 0 和 1 之间）。

### Schema Transformation Process Schema 转换过程
* Phase 1: Aggregate Projection and Functional Approximation 阶段 1：聚合投影与函数近似
  * The operator invokes a `.select()` transformation on the input dataset, collapsing all original structural dimensions down to a single row-expression. 算子对输入数据集调用 `.select()` 转换，将所有原始结构维度降维至单个行表达式。
  * Inside the projection, the `F.percentile_approx()` catalyst expression is evaluated, utilizing a fixed accuracy parameter of `10000` histogram buckets to compute the target value. 在投影内部，求值 `F.percentile_approx()` Catalyst 表达式，利用固定的 `10000` 个直方图桶的准确度参数来计算目标值。
* Phase 2: Metadata Renaming and Width Pruning 阶段 2：元数据重命名与宽度裁剪
  * The aggregate function result is immediately bound to the `.alias()` modifier matching the string defined by `field`. 聚合函数结果立即绑定到与 `field` 定义的字符串匹配的 `.alias()` 修饰符。
  * All input columns other than this freshly aliased calculated result are explicitly discarded, reducing the global DataFrame to a single-column, single-row summary state. 除此新起别名的计算结果之外，所有其他输入列都会被显式丢弃，从而将全局 DataFrame 缩减为单列、单行的摘要状态。

### Output Schema Final State 输出 Schema 最终态
* `field` -> Approximated Percentile Result Column 近似百分位数结果列: The sole output column containing the computed rank metric value. 包含计算出的排名指标值的唯一输出列。
  * Data Type 数据类型: `DoubleType()` 双精度浮点型. Regardless of whether the input numeric column is typed as an integer or a float, the `percentile_approx` aggregation engine natively evaluates and returns the continuous percentile marker as a double precision float field. 无论输入的数值列是整型还是浮点型，`percentile_approx` 聚合引擎原生求值并返回连续的百分位数标记作为双精度浮点型字段。

🏡 Back to [operator market 算子市场](../ops_market.md)