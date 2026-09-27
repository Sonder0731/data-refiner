# `MinhashLSHDeduplicator` Operator

---

Deduplicate records using MinHash and Locality Sensitive Hashing (LSH) algorithm. 使用 MinHash 和局部敏感哈希 (LSH) 算法对记录进行去重

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.deduplicator.minhash_lsh_deduplicator`
- Class name 类名: `MinhashLSHDeduplicator`
- Inherit from 继承于: [Deduplicator](../meta_operator/deduplicator.md)
- Test code 测试代码: [test code](../../../tests/ops/deduplicator/test_minhash_lsh_deduplicator.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`b`|`Optional[int]`|`None`|`False`|The number of bands to be used for LSH.||
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`comparison_function`|`Optional[str]`|`None`|`False`|A comparison function used to resolve duplicates by deciding which record to keep. 用于在重复数据之间决定保留哪条记录的比较函数||
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`ev_partitions`|`<class 'int'>`|`10000`|`False`|The number of partitions to be used for generating MinHash signatures.||
|`field`|`<class 'str'>`|`None`|`True`|The field name (column name in dataframe) to deduplicate. 要去重的字段名（DataFrame 列名）||
|`index_field`|`<class 'str'>`|`None`|`False`|The name of the field to be used as the index for LSH.||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`mode`|`<class 'str'>`|`dedup`|`False`|The mode of deduplication. 去重模式|- `dedup`<br/>remove duplicates 去重<br/>- `dup`<br/>keep duplicates only 仅保留重复数据<br/>- `dedup_with_dup`<br/>deduplicate and keep duplicates separately 去重并单独保留重复数据<br/>|
|`ngram_size`|`<class 'int'>`|`5`|`False`|The size of the n-grams to be used for generating MinHash signatures.||
|`num_perm`|`<class 'int'>`|`300`|`False`|The number of permutations to be used for generating MinHash signatures.||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`r`|`Optional[int]`|`None`|`False`|The number of rows to be used for LSH.||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||
|`text_min_length`|`<class 'int'>`|`20`|`False`|The minimum length of the text to be considered for generating MinHash signatures.||
|`threshold`|`<class 'float'>`|`0.7`|`False`|The threshold for the Jaccard similarity between two records to be considered as duplicates.||


## Constraint 约束

### Input Column Constraints 输入列约束
* The input DataFrame must contain the specific target column designated by `field`. 输入 DataFrame 必须包含由 `field` 指定的特定目标列。
* The column specified by `field` must be of either `StringType` or `ArrayType(StringType)` data type. 由 `field` 指定的列必须为 `StringType` 或 `ArrayType(StringType)` 数据类型。
* If the configuration parameter `index_field` is specified, a column with that matching name must exist in the input DataFrame. 如果指定了配置参数 `index_field`，则输入 DataFrame 中必须存在具有该匹配名称的列。

### Argument and Column Mapping 参数与列的映射
* `field` -> Text Tokenization Column 文本分词列: This parameter points to the raw text or token array used to generate n-grams and MinHash signatures. 该参数指向用于生成 n-grams 和 MinHash 签名的高维原始文本或标量数组。
* `index_field` -> Primary Identifier Column 主键标识列: This parameter defines the tracking key used to perform graph structural grouping and dataframe joins. 该参数定义了用于执行图结构分组和数据帧连接的追踪主键。
  * When `index_field` is omitted or `None` 当 `index_field` 省略或为 `None` 时: The operator maps and automatically generates an index named `__id__` using a monotonic sequence. 算子映射并使用单调递增序列自动生成名为 `__id__` 的索引。
* `mode` -> Structural Processing Mode 结构化处理模式: This parameter governs how rows are combined and what columns are structural elements in the final dataframe pipeline. 该参数决定了行的合并方式以及最终数据帧流水线中哪些列将作为结构化元素。
  * When `mode` is `DeduplicatorMode.DEDUP` 当 `mode` 为 `DeduplicatorMode.DEDUP` 时: The operator unions the un-clustered records with the priority records resolved within each connected component. 算子将未聚类的记录与在每个连通分量中解析出的优先记录进行联合。
  * When `mode` is `DeduplicatorMode.DUP` 当 `mode` 为 `DeduplicatorMode.DUP` 时: The operator returns exclusively the duplicate records by executing an anti-join mapping. 算子通过执行反连接映射，仅返回重复的记录。
  * When `mode` is `DeduplicatorMode.DEDUP_WITH_DUP` 当 `mode` 为 `DeduplicatorMode.DEDUP_WITH_DUP` 时: The operator appends a boolean retention flag to indicate whether a record is kept or discarded. 算子追加一个布尔类型的保留标志，以指示记录是被保留还是丢弃。

### Schema Transformation Process Schema 转换过程
* Phase 1: Indexing and Component Clustering 阶段 1：索引建制与分量聚类:
  * If the specified tracking column does not exist, an intermediate column (defaulting to `__id__`) of type `LongType` is created via `monotonically_increasing_id()`. 如果指定的追踪列不存在，则通过 `monotonically_increasing_id()` 创建一个类型为 `LongType` 的中间列（默认为 `__id__`）。
  * An intermediate GraphFrame mapping creates a transient column named `component` of type `LongType`, which is then joined back to the primary schema as `__component__`. 一个中间 GraphFrame 映射会创建一个名为 `component` 且类型为 `LongType` 的瞬态列，随后作为 `__component__` 连接回主 Schema。
* Phase 2: Behavioral Branching 阶段 2：行为分支转换:
  * Under `DeduplicatorMode.DEDUP` 在 `DeduplicatorMode.DEDUP` 模式下: The intermediate column `__component__` is consumed during the RDD `reduceByKey` stage and is omitted from the final row unions. 中间列 `__component__` 在 RDD `reduceByKey` 阶段被消耗，并在最终的行联合操作中被排除。
  * Under `DeduplicatorMode.DUP` 在 `DeduplicatorMode.DUP` 模式下: The pipeline performs an anti-join using the tracking key, removing `__component__` automatically and preserving the original schema shape. 流水线使用追踪键执行反连接，自动移除 `__component__` 并保留原始的 Schema 形状。
  * Under `DeduplicatorMode.DEDUP_WITH_DUP` 在 `DeduplicatorMode.DEDUP_WITH_DUP` 模式下: A new structural indicator column named `__stay__` of type `BooleanType` is appended via a left join, and all missing rows are populated via a fallback fill value. 通过左连接追加一个名为 `__stay__` 且类型为 `BooleanType` 的新结构指示列，并且所有缺失行通过兜底填充值进行填补。

### Output Schema Final State 输出 Schema 最终态
* When `mode` is `DeduplicatorMode.DEDUP` or `DeduplicatorMode.DUP` 当 `mode` 为 `DeduplicatorMode.DEDUP` 或 `DeduplicatorMode.DUP` 时:
  * The output DataFrame schema matches the input DataFrame schema exactly, except for the possible inclusion of the generated `__id__` column of type `LongType` if no prior index field was present. 输出 DataFrame Schema 与输入 DataFrame Schema 完全匹配，唯一的例外是：若此前不存在索引字段，则可能包含自动生成的 `LongType` 类型的 `__id__` 列。
* When `mode` is `DeduplicatorMode.DEDUP_WITH_DUP` 当 `mode` 为 `DeduplicatorMode.DEDUP_WITH_DUP` 时:
  * The output schema contains all columns from the input schema (plus the optional `__id__` column of type `LongType`). 输出 Schema 包含输入 Schema 的所有列（加上可选的 `LongType` 类型的 `__id__` 列）。
  * An additional indicator column named `__stay__` of type `BooleanType` is permanently appended to the final schema layout. 一个名为 `__stay__` 且类型为 `BooleanType` 的附加指示列将被永久追加到最终的 Schema 布局中。

🏡 Back to [operator market 算子市场](../ops_market.md)