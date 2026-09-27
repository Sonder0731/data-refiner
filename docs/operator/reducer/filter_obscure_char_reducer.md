# `FilterObscureCharReducer` Operator

---

Filter out the characters that appear under a certain cumsum frequency percentage of the data. 筛选出在数据中出现频率低于特定累积频率百分比的字符

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.reducer.filter_obscure_char_reducer`
- Class name 类名: `FilterObscureCharReducer`
- Inherit from 继承于: [Reducer](../meta_operator/reducer.md)
- Test code 测试代码: [test code](../../../tests/ops/reducer/test_filter_obscure_char_reducer.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`cumsum_rate`|`<class 'float'>`|`0.99`|`False`|The cumsum rate of the characters to be filtered out.||
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`field`|`<class 'str'>`|`None`|`True`|The field name used for reduction. 用于归并计算的字段名||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束
## Constraint 约束

### Input Column Constraints 输入列约束
* `field` -> Target Text Column 目标文本列: The input DataFrame must contain this specified text column. 输入 DataFrame 必须包含此指定的文本列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column must contain string data whose individual characters will be extracted and analyzed for cumulative frequency. 该列必须包含字符串数据，其单个字符将被提取并进行累积频次分析。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Token Source Column 源词项源列: This parameter defines the name of the input string column used as the raw character source. 该参数指定用作原始字符源的输入字符串列名称。
* `cumsum_rate` -> Threshold Percent Parameter 阈值百分比参数: This configuration parameter determines the cumulative frequency percentage cutoff used to filter obscure characters. 该配置参数决定了用于过滤冷门字符的累积频次百分比截断阈值。

### Schema Transformation Process Schema 转换过程
* Phase 1: Character Extraction and RDD Frequency Grouping 阶段 1：字符提取与 RDD 频次分组
  * The internal engine executes an RDD `.flatMap()` operation to unnest every string in the column specified by `field` into individual character tokens. 内部引擎执行 RDD `.flatMap()` 操作，将 `field` 指定列中的每个字符串展开为独立的字符标记。
  * A subsequent `.map()` and `.reduceByKey()` chain counts occurrences, and the resulting structure is explicitly loaded into a temporary schema footprint via `.toDF(["char", "frequency"])`. 随后的 `.map()` 和 `.reduceByKey()` 链计算出现次数，生成结构通过 `.toDF(["char", "frequency"])` 被显式加载到临时 schema 蓝图中。
* Phase 2: Windowed Accumulation and Mathematical Projection 阶段 2：开窗累加与数学投影
  * A Spark `Window` specification is established, ordering rows by descending frequency from the unbounded beginning to the current row index. 建立一个 Spark `Window` 规范，按频次降序对行进行排序，范围从无边界起始到当前行索引。
  * The first `.withColumn()` operator appends an intermediate aggregation column named `"cumsum"` containing the running total of character occurrences. 第一个 `.withColumn()` 算子追加一个名为 `"cumsum"` 的中间聚合列，包含字符出现次数的运行累计总数。
  * A second `.withColumn()` operator divides the `"cumsum"` column by a broadcasted scalar total frequency, projecting a new evaluation field named `"cumsum_rate"`. 第二个 `.withColumn()` 算子将 `"cumsum"` 列除以广播的标量总频次，投影一个名为 `"cumsum_rate"` 的新评估字段。
* Phase 3: Threshold Filtering 阶段 3：阈值过滤
  * A `.filter()` transformation is applied based on the configured `cumsum_rate` value, selecting only the character records that fall within the specified cumulative boundary. 基于配置的 `cumsum_rate` 值应用 `.filter()` 转换，仅保留符合指定累积边界的字符记录。

### Output Schema Final State 输出 Schema 最终态
* `char` -> Character Key Column 字符键列: The first output field holding the extracted character token. 第一个输出列，存放提取的字符标记。
  * Data Type 数据类型: `StringType()` 字符串类型. This column holds the unique character strings extracted from the input text collection. 该列存放从输入文本集合中提取的唯一字符字符串。
* `frequency` -> Occurrence Count Column 出现次数列: The second output field holding the raw occurrence count. 第二个输出列，存放原始出现次数。
  * Data Type 数据类型: `LongType()` 长整型. This column represents the total absolute count of the corresponding character within the dataset. 该列代表数据集中对应字符的总绝对计数。
* `cumsum` -> Cumulative Sum Column 累积和列: The third output field holding the running aggregate frequency. 第三个输出列，存放运行累计频次。
  * Data Type 数据类型: `LongType()` 长整型. This column tracks the descending cumulative frequency sum calculated down to the current character record. 该列记录按降序计算至当前字符记录的累计频次和。
* `cumsum_rate` -> Cumulative Percentage Column 累积百分比列: The fourth output field holding the calculated ratio score. 第四个输出列，存放计算出的比率得分。
  * Data Type 数据类型: `DoubleType()` 双精度浮点型. This column contains the mathematical ratio representing the character's ranked cumulative position relative to the global frequency pool. 该列包含代表该字符相对于全局频次池的排序累积位置的数学比率。

🏡 Back to [operator market 算子市场](../ops_market.md)