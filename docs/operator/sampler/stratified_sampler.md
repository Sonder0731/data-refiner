# `StratifiedSampler` Operator

---

Sample data by stratifying on a specified column (e.g., city), returning a fixed number of samples per group if available. 按指定列分层抽样数据，如果可用，则返回每个组的固定数量的样本

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.sampler.stratified_sampler`
- Class name 类名: `StratifiedSampler`
- Inherit from 继承于: [Sampler](../meta_operator/sampler.md)
- Test code 测试代码: [test code](../../../tests/ops/sampler/test_stratified_sampler.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`samples_per_stratum`|`<class 'int'>`|`100`|`False`|The number of samples to take from each stratum.||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`strata_column`|`<class 'str'>`|`None`|`True`|The column name to stratify on.||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `strata_column` -> Partition Target Column 分区目标列: The input DataFrame must contain this specified stratification key column. 输入 DataFrame 必须包含此指定的层化键列。
  * Data Type 数据类型: `Any` 任意类型. This column can hold any valid Spark SQL data type that supports grouping and partitioning (such as `StringType()` or `IntegerType()`). 该列可以包含任何支持分组和分区的有效 Spark SQL 数据类型（例如 `StringType()` 或 `IntegerType()`）。

### Argument and Column Mapping 参数与列的映射
* `strata_column` -> Stratification Key Column 层化键列: This parameter defines the name of the input column used to slice the dataset into separate categorical strata or sub-groups. 该参数指定用于将数据集切分为独立分类层或子组的输入列名称。
* `samples_per_stratum` -> Capacity Cap Parameter 容量上限参数: This configuration parameter determines the maximum integer threshold of row samples to preserve within each isolated stratum. 该配置参数决定了在每个隔离的层中要保留的行样本的最大整型阈值。

### Schema Transformation Process Schema 转换过程
* Phase 1: Window Partitioning and Sequence Generation 阶段 1：窗口分区与序列生成
  * A Spark `Window` specification is dynamically defined, partitioning the data by the value of `strata_column` and ordering rows using a non-deterministic `F.monotonically_increasing_id()` index. 一个 Spark `Window` 规范被动态定义，通过 `strata_column` 的值对数据进行分区，并使用非确定性的 `F.monotonically_increasing_id()` 索引对行进行排序。
  * The `.withColumn()` operator appends an intermediate tracking field named `"__row_num__"` into the active DataFrame schema footprint. `.withColumn()` 算子向当前 DataFrame schema 蓝图中追加一个名为 `"__row_num__"` 的中间追踪字段。
  * Data Type 数据类型: `IntegerType()` 整型. The newly appended intermediate field holds sequential 1-based indices calculated independently within each grouped partition boundary via the `F.row_number()` expression. 新追加的中间字段包含通过 `F.row_number()` 表达式在每个分组分区边界内独立计算出的、以 1 开始的顺序索引。
* Phase 2: Threshold Pruning and Temporary Purging 阶段 2：阈值裁剪与临时清除
  * A `.filter()` transformation is executed to evaluate the sequence indices, systematically discarding rows whose `"__row_num__"` values exceed the configured integer cap defined by `samples_per_stratum`. 执行 `.filter()` 转换来评估序列索引，系统性地丢弃 `"__row_num__"` 值超过由 `samples_per_stratum` 定义的配置整型上限的行。
  * The `.drop()` operator is subsequently invoked to strip the temporary `"__row_num__"` metadata tracking column out of the schema layout entirely. 随后调用 `.drop()` 算子，将临时的 `"__row_num__"` 元数据追踪列从 schema 布局中完全剥离。

### Output Schema Final State 输出 Schema 最终态
* Original Columns 原始列: All schema columns existing in the initial input DataFrame are fully retained with their original names, relative ordering, and exact data types. 初始输入 DataFrame 中存在的所有 Schema 列都将完整保留其原始名称、相对顺序和准确的数据类型。
  * No external permanent field columns are structurally injected, mutated, or deleted; the output schema structure remains identical to the input state, despite the aggregate row count reduction. 没有结构性地注入、变异或删除任何外部永久字段列；尽管总行数有所减少，但输出 schema 结构与输入状态保持完全一致。

🏡 Back to [operator market 算子市场](../ops_market.md)