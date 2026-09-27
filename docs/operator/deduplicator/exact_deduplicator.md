# `ExactDeduplicator` Operator

---

Deduplicate exactly same value in a field column from a whole dataset. 从整个数据集中删除字段列中完全相同的重复值

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.deduplicator.exact_deduplicator`
- Class name 类名: `ExactDeduplicator`
- Inherit from 继承于: [Deduplicator](../meta_operator/deduplicator.md)
- Test code 测试代码: [test code](../../../tests/ops/deduplicator/test_exact_deduplicator.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`comparison_function`|`Optional[str]`|`None`|`False`|A comparison function used to resolve duplicates by deciding which record to keep. 用于在重复数据之间决定保留哪条记录的比较函数||
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`field`|`<class 'str'>`|`None`|`True`|The field name (column name in dataframe) to deduplicate. 要去重的字段名（DataFrame 列名）||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`mode`|`<class 'str'>`|`dedup`|`False`|The mode of deduplication. 去重模式|- `dedup`<br/>remove duplicates 去重<br/>- `dup`<br/>keep duplicates only 仅保留重复数据<br/>- `dedup_with_dup`<br/>deduplicate and keep duplicates separately 去重并单独保留重复数据<br/>|
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* The input DataFrame must contain the specific column designated by `field`. 输入 DataFrame 必须包含由 `field` 指定的特定列。
* The column specified by `field` must be of `StringType` data type. 由 `field` 指定的列必须为 `StringType` 数据类型。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Column 源列: This parameter identifies the target column used to calculate the unique identifier for deduplication. 该参数确定用于计算去重唯一标识的目标列。
  * The values in `field` are passed into a SHA256 hashing function to generate a new key column named `gid`. `field` 中的值被传入 SHA256 哈希函数中，以生成名为 `gid` 的新键列。
* `mode` -> Deduplication Mode 控制模式: This parameter dictates the structural behavior of the schema transformation and the data rows retained in the final output. 该参数决定了 Schema 转换的结构行为以及最终输出中保留的数据行。
  * When `mode` is `DeduplicatorMode.DEDUP` 当 `mode` 为 `DeduplicatorMode.DEDUP` 时: The operator retains unique rows via RDD reduce operations. 算子通过 RDD reduce 操作保留唯一行。
  * When `mode` is `DeduplicatorMode.DUP` 当 `mode` 为 `DeduplicatorMode.DUP` 时: The operator appends a temporary tracking column to filter out unique records and isolate duplicates. 算子追加一个临时追踪列以过滤出唯一记录并隔离重复数据。
  * When `mode` is `DeduplicatorMode.DEDUP_WITH_DUP` 当 `mode` 为 `DeduplicatorMode.DEDUP_WITH_DUP` 时: The operator introduces a tracking column and executes an anti-join operation to separate unique and duplicate data streams. 算子引入一个追踪列并执行反连接操作以分离唯一与重复的数据流。

### Schema Transformation Process Schema 转换过程
* Phase 1: Key Generation 阶段 1：键生成: The component applies a SHA256 UDF to the column specified by `field`, adding a temporary intermediate column named `gid` of type `StringType` to the DataFrame. 组件对 `field` 指定的列应用 SHA256 UDF，向 DataFrame 中添加一个名为 `gid` 且类型为 `StringType` 的临时中间列。
* Phase 2: Structural Branching 阶段 2：结构分支:
  * Under `DeduplicatorMode.DEDUP` 在 `DeduplicatorMode.DEDUP` 模式下: The DataFrame is converted to an RDD to perform a key-based reduction and then converted back to a DataFrame, maintaining the `gid` column. DataFrame 被转换为 RDD 以执行基于键的归约，随后转换回 DataFrame，保留 `gid` 列。
  * Under `DeduplicatorMode.DUP` 在 `DeduplicatorMode.DUP` 模式下: A temporary unique identifier column named `__id__` of type `LongType` is added via `monotonically_increasing_id()`. An anti-join operation is subsequently performed on `__id__`, which is then dropped from the final output schema. 通过 `monotonically_increasing_id()` 添加一个名为 `__id__` 且类型为 `LongType` 的临时唯一标识列。随后在 `__id__` 上执行反连接操作，该列最终会从输出 Schema 中移除。
  * Under `DeduplicatorMode.DEDUP_WITH_DUP` 在 `DeduplicatorMode.DEDUP_WITH_DUP` 模式下: Similar to the duplicate mode, `__id__` of type `LongType` is appended. An intermediate dataframe with `__id__` and an extra `__is_dup__` column of type `BooleanType` is created for anti-join mapping. 与重复模式类似，追加 `LongType` 类型的 `__id__` 列。同时创建一个包含 `__id__` 以及额外的 `BooleanType` 类型 `__is_dup__` 列的中间 DataFrame 用于反连接映射。

### Output Schema Final State 输出 Schema 最终态
* The output DataFrame retains all original input columns with their data types unaltered. 输出 DataFrame 保留所有原始输入列，且其数据类型未发生改变。
* A permanent structural addition is made to the schema: a column named `gid` of type `StringType` is included in the final output. Schema 中新增了一个永久性的结构变化：最终输出中包含一个名为 `gid` 且类型为 `StringType` 的列。

🏡 Back to [operator market 算子市场](../ops_market.md)