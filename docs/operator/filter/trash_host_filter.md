# `TrashHostFilter` Operator

---

Filter the text from the trash host. 从过滤来自垃圾域名的文本

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.filter.trash_host_filter`
- Class name 类名: `TrashHostFilter`
- Inherit from 继承于: [Filter](../meta_operator/filter.md)
- Test code 测试代码: [test code](../../../tests/ops/filter/test_trash_host_filter.py)
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
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`tag_field`|`<class 'str'>`|`tag`|`False`|The tag field name (column name). 标签字段名（列名）||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||
|`trash_host_paths`|`List[Union[str, pathlib.Path]]`|`[PosixPath('/data/projects/data-refiner/data-refiner-runtime-resources/data/KADhosts.txt'), PosixPath('/data/projects/data-refiner/data-refiner-runtime-resources/data/FadeMindhosts.txt')]`|`False`|A list of paths to the trash host files.||


## Constraint 约束

### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operator directly relies on the specific source URL or host string column specified by `field`. 该算子直接依赖于由 `field` 指定的特定源 URL 或主机名字符串列。
* Required Data Types 要求的数据类型: The column specified by `field` must be of `StringType()`, which is verified by the `check_column_schema` method before any logical evaluation. 由 `field` 指定的列必须为 `StringType()`，在进行任何逻辑评估前由 `check_column_schema` 方法进行验证。

### Argument and Column Mapping 参数与列的映射
* `field` -> Checked Host Column 被检查主机列: This parameter designates the target column containing host values to be checked against the loaded Bloom filter. 该参数指定包含主机名元素的目标列，用于对照加载的布隆过滤器进行检查。
* `trash_host_paths` -> Blacklist Sources 黑名单数据源: This parameter defines the file paths used to dynamically construct the in-memory Bloom filter, directly influencing the evaluation result of `tag_field`. 该参数定义了用于动态构建内存中布隆过滤器的文件路径，直接影响 `tag_field` 的评估结果。
* `tag_field` -> Filter Result Column 过滤结果列: This parameter names the appended intermediate boolean column that stores the matching status of the Bloom filter check. 该参数命名了追加的中间布尔列，用于存储布隆过滤器检查的匹配状态。
  * When the host value in `field` is not found in the Bloom filter blacklist 当 `field` 中的主机名值未在布隆过滤器黑名单中找到时: The row value in `tag_field` evaluates to `True`. `tag_field` 中的行值解析为 `True`。
  * When the host value in `field` exists in the Bloom filter blacklist 当 `field` 中的主机名值存在于布隆过滤器黑名单中时: The row value in `tag_field` evaluates to `False`. `tag_field` 中的行值解析为 `False`。
* `mode` -> Dataset Pruning Strategy 数据集剪枝策略: This parameter dictates the row-filtering action applied within `return_df_by_filter_level` based on the validation states in `tag_field`. 该参数决定了在 `return_df_by_filter_level` 中基于 `tag_field` 中的验证状态所执行的行过滤操作。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: A user-defined function `bloom_filter_udf` returning `BooleanType` is executed via the `withColumn` operator, which appends a single intermediate indicator column named after `tag_field` to the processing schema. 一个返回 `BooleanType` 的用户自定义函数 `bloom_filter_udf` 通过 `withColumn` 算子被执行，向处理 Schema 中追加一个以 `tag_field` 命名的中间指示列。
* Type Modifications 类型改变: The structural layout and data types of preexisting columns within the incoming DataFrame remain fully unmodified. 输入 DataFrame 中原有列的结构布局和数据类型保持完全未修改状态。
* Elimination of Intermediate Columns 中间列的消除: The intermediate `tag_field` column is processed inside `return_df_by_filter_level`, which may drop this temporary operational column prior to returning the final schema state. 中间 `tag_field` 列在 `return_df_by_filter_level` 内部被处理，该函数可能会在返回最终 Schema 状态之前删除此临时业务列。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: The final DataFrame outputs the original structural baseline columns, whereas the intermediate condition column `tag_field` is dynamically decoupled or dropped based on the execution mode of the downstream filtering wrapper. 最终的 DataFrame 输出原始的结构基线列，而中间条件列 `tag_field` 则根据下游过滤外壳的执行模式被动态解耦或删除。
* Final Column Data Types 最终列数据类型: Every baseline column perfectly preserves its incoming operational type (e.g., `field` strictly remains `StringType()`) ensuring structural schema compliance for subsequent processing steps. 每一个基线列都完美地保留了其输入的业务类型（例如，`field` 严格保持为 `StringType()`），确保了后续处理步骤的结构 Schema 合规性。

🏡 Back to [operator market 算子市场](../ops_market.md)