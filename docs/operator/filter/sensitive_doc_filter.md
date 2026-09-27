# `SensitiveDocFilter` Operator

---

Filter the documents by the sensitive keywords. 按敏感关键词筛选文档

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.filter.sensitive_doc_filter`
- Class name 类名: `SensitiveDocFilter`
- Inherit from 继承于: [Filter](../meta_operator/filter.md)
- Test code 测试代码: [test code](../../../tests/ops/filter/test_sensitive_doc_filter.py)
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
|`keyword_limits`|`<class 'int'>`|`1`|`False`|The maximum number of sensitive keywords in a document.||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`mode`|`<class 'str'>`|`filter`|`False`|The filter mode. 过滤模式|- `tag`<br/>tag only 仅打标<br/>- `filter`<br/>filter rows 过滤数据行<br/>- `tag_and_filter`<br/>tag and filter 打标并过滤<br/>|
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`sensitive_keyword_paths`|`List[pathlib.Path]`|`[PosixPath('data-refiner-runtime-resources/data/SensitiveLexicon.json')]`|`False`|Keyword files. Cluster-relative paths resolve inside the `data-refiner-runtime-resources` archive.||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`tag_field`|`<class 'str'>`|`tag`|`False`|The tag field name (column name). 标签字段名（列名）||
|`targeted_keywords_field`|`Optional[str]`|`None`|`False`|The field name of the targeted keywords.||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operation depends on the source text column defined by the `field` configuration parameter. 该操作依赖于由 `field` 配置参数定义的源文本列。
* Required Data Types 要求的数据类型: The column specified by `field` must be of type `StringType()`, which is verified by the `check_column_schema` validator before processing. 由 `field` 指定的列必须为 `StringType()` 类型，并在处理前通过 `check_column_schema` 校验器进行验证。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Text Column 源文本列: This parameter identifies the text column parsed by the Aho-Corasick automaton to search for sensitive keywords. 该参数指定由 Aho-Corasick 自动机解析以搜索敏感词的文本列。
* `targeted_keywords_field` -> Extracted Detail Column 提取详情列: This parameter maps to an optional array column storing structural hit details. 该参数映射到一个可选的数组列，用于存储结构化的命中详情。
  * When `targeted_keywords_field` is specified 当指定 `targeted_keywords_field` 时: An explicit column is created containing tuples of matches (keyword, start index, end index). 将创建一个显式列，其中包含匹配项的三元组（关键字、起始索引、结束索引）。
  * When `targeted_keywords_field` is `None` 当 `targeted_keywords_field` 为 `None` 时: No detailed extraction column is added to the data pipeline. 数据流水线中不会添加任何详细的提取列。
* `keyword_limits` -> Threshold Limiter 阈值限制器: This parameter influences the condition of `tag_field`. If the detected keyword count exceeds this value, the text is flagged for exclusion. 该参数影响 `tag_field` 的条件状态。如果检测到的关键字数量超过此值，该文本将被标记为排除。
* `tag_field` -> Filter Indicator Column 过滤指示列: This parameter specifies the name of the intermediate boolean column determining the validation status. 该参数指定决定验证状态的中间布尔列的名称。
  * When sensitive keywords count <= `keyword_limits` 当敏感词数量 <= `keyword_limits` 时: The row value in `tag_field` is set to `True`. `tag_field` 中的行值被设置为 `True`。
  * When sensitive keywords count > `keyword_limits` 当敏感词数量 > `keyword_limits` 时: The row value in `tag_field` is set to `False`. `tag_field` 中的行值被设置为 `False`。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: The Schema dynamically alters based on the provided parameters. Schema 根据提供的参数进行动态改变。
  * Under conditional targeting 满足指定详情列条件时: Two columns are sequentially generated: an extraction column named after `targeted_keywords_field` with type `ArrayType(StructType([...]))` via `match_sensitive_keyword_udf`, followed by a `BooleanType` column named after `tag_field`. 依次生成两个列：通过 `match_sensitive_keyword_udf` 创建的以 `targeted_keywords_field` 命名且类型为 `ArrayType(StructType([...]))` 的提取列，以及随后创建的以 `tag_field` 命名的 `BooleanType` 列。
  * Under direct filtering 满足直接过滤条件时: Only one intermediate `BooleanType` column named after `tag_field` is appended via `tag_sensitive_doc_udf`. 仅通过 `tag_sensitive_doc_udf` 追加一个以 `tag_field` 命名的中间 `BooleanType` 列。
* Type Modifications 类型改变: Preexisting columns from the input DataFrame are unmodified, and their structural definitions remain intact. 输入 DataFrame 中原有的列未被修改，其结构定义保持原样。
* Elimination of Intermediate Columns 中间列的消除: The intermediate `tag_field` column is processed by the `return_df_by_filter_level` routine, which may filter rows and conditionally drop this tag before delivering the final schema. 中间 `tag_field` 列由 `return_df_by_filter_level` 例程处理，该例程可能会在交付最终 Schema 之前过滤行并条件性地删除此标记。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: If `targeted_keywords_field` was specified, it remains preserved within the schema as a permanent metadata column. The intermediate `tag_field` is stripped or retained based on the execution logic of the filter utility. 如果指定了 `targeted_keywords_field`，它将作为永久元数据列保留在 Schema 中。中间的 `tag_field` 则根据过滤工具的执行逻辑被剥离或保留。
* Final Column Data Types 最终列数据类型: Retained baseline columns preserve their incoming types. If stored, the structural field type for `targeted_keywords_field` evaluates exactly as `ArrayType(StructType([StructField("keyword", StringType()), StructField("start_index", IntegerType()), StructField("end_index", IntegerType())]))`. 保留的基线列保持其输入类型。如果被存储，`targeted_keywords_field` 的结构化列类型精确解析为 `ArrayType(StructType([StructField("keyword", StringType()), StructField("start_index", IntegerType()), StructField("end_index", IntegerType())]))`。

🏡 Back to [operator market 算子市场](../ops_market.md)
