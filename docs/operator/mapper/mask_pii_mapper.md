# `MaskPiiMapper` Operator

---

Mask personal identifiable information (PII) in text. 在文本中屏蔽个人身份信息 (PII)

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.mask_pii_mapper`
- Class name 类名: `MaskPiiMapper`
- Inherit from 继承于: [SimpleMapper](../meta_operator/simple_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_mask_pii_mapper.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`engine_name`|`<class 'pathlib.Path'>`|`en_core_web_sm-3.8.0`|`False`|The name of the engine model.||
|`field`|`<class 'str'>`|`None`|`True`|The field name to map on. 要进行映射的字段名||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`mask_map`|`Dict`|`None`|`True`|A mapping from entity type to Presidio configuration. Each non-null value must contain `type`; operator parameters are sibling keys, for example `PHONE_NUMBER: {type: replace, new_value: <PHONE>}`.||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`output_field`|`<class 'str'>`|`None`|`True`|The field name to store the mapped value. 用于存储映射结果的字段名||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `mask_map` -> PII Masking Configurations PII 掩码配置: This parameter specifies a dictionary mapping target entity types to their concrete masking operational parameters, directly controlling how text fragments within the source column are substituted. 该参数指定一个字典，将目标实体类型映射到具体的掩码操作参数，直接控制源列中的文本片段如何被替换。
* `engine_path` -> NLP Model Path NLP 模型路径: This parameter specifies the location of the Spacy NLP model utilized inside each partition executor to tokenize and detect base entities. 该参数指定在每个分区执行器内部使用的 Spacy NLP 模型路径，用于对文本进行分词并检测基础实体。
* `field` -> Input Column 输入列: This parameter specifies the source text column that contains the sensitive samples to be evaluated and anonymized. 该参数指定包含敏感样本以进行评估和脱敏的源文本列。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the fully anonymized and masked text string will be stored. 该参数定义了存储完全脱敏和掩码后文本字符串的新列的列名。

### Schema Transformation Process Schema 转换过程
* The component parses and overrides default masking strategies with user configurations defined in `mask_map` before distributed execution. 组件在分布式执行前，使用 `mask_map` 中定义的客户配置解析并覆盖默认的掩码策略。
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to facilitate node-level analyzer and anonymizer object initialization using the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便使用 `mapPartitions` 算子促进节点级分析器和脱敏器对象的初始化。
* Inside the partition loop, the `init_analyzer` method registers both default English models and customized Chinese recognizers, appending an updated text sequence to a new key-value pair represented by `output_field` for every row. 在分区循环内部，`init_analyzer` 方法同时注册默认的英文模型和自定义的中文识别器，并将更新后的文本序列追加到由 `output_field` 表示的每行新键值对中。
* The processed RDD structure is re-converted back into a DataFrame representation via the `toDF()` method, embedding the newly generated column alongside the original intact fields. 处理后的 RDD 结构通过 `toDF()` 方法重新转换为 DataFrame 表示，将新生成的列嵌入到保持完好的原始字段旁。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `StringType`。

🏡 Back to [operator market 算子市场](../ops_market.md)
