# `LanguageIdentificationMapper` Operator

---

This mapper uses the fasttext model to identify the language of a given text. 使用 fasttext 模型来识别给定文本的语言

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.language_identification_mapper`
- Class name 类名: `LanguageIdentificationMapper`
- Inherit from 继承于: [SimpleMapper](../meta_operator/simple_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_language_identification_mapper.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`field`|`<class 'str'>`|`None`|`True`|The field name to map on. 要进行映射的字段名||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`label_prefix`|`<class 'str'>`|`__label__`|`False`|Remove what prefix string in language label, default is __label__||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`output_field`|`<class 'str'>`|`None`|`True`|The field name to store the mapped value. 用于存储映射结果的字段名||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`prob_field`|`<class 'str'>`|`language_prob`|`False`|The column name for showing probability of language.||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `model_path` -> Model File Path 模型文件路径: This parameter specifies the localization path of the fasttext language identification model used to analyze the text data. 该参数指定用于分析文本数据的 fasttext 语言识别模型的本地化路径。
* `label_prefix` -> Label Prefix 标签前缀: This parameter defines the prefix string used by the fasttext model to isolate and clean the predicted language code. 该参数定义了 fasttext 模型使用的标签前缀字符串，用于隔离和清洗预测的语言代码。
* `field` -> Input Column 输入列: This parameter specifies the source text column whose multiline string content will be stripped of newlines and evaluated for language detection. 该参数指定源文本列，其多行字符串内容将被去除换行符并进行语言检测评估。
* `output_field` -> Predicted Language Column 预测语言列: This parameter defines the name of the new column where the identified language label will be stored. 该参数定义了存储识别出的语言标签的新列的列名。
* `prob_field` -> Probability Column 概率列: This parameter defines the name of the new column where the confidence score of the predicted language will be stored. 该参数定义了存储预测语言置信度分数的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is transformed into a Resilient Distributed Dataset (`RDD`) to facilitate row-by-row language identification inside partitions via the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便通过 `mapPartitions` 算子在分区内部促进逐行的语言识别。
* Within the partition function, text fields are preprocessed to remove newline characters before being evaluated by the loaded fasttext model. 在分区函数内部，文本字段在由加载的 fasttext 模型评估之前会被预处理以移除换行符。
* Two distinct key-value pairs represented by `output_field` and `prob_field` are dynamically appended to each row dictionary within the iterator. 在迭代器内部，由 `output_field` 和 `prob_field` 表示的两个不同的键值对被动态追加到每个行字典中。
* The mutated RDD structure is re-converted back into a DataFrame representation via the `toDF()` method, upgrading the schema layout to incorporate both new columns. 改变后的 RDD 结构通过 `toDF()` 方法重新转换为 DataFrame 表示，从而升级 Schema 布局以合并这两个新列。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are retained in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `StringType`。
* `prob_field`: A secondary new column is appended to the final DataFrame, and its data type is explicitly determined as `DoubleType` or `FloatType`. 最终 DataFrame 中追加了第二个新列，其数据类型被明确确定为 `DoubleType` 或 `FloatType`。

🏡 Back to [operator market 算子市场](../ops_market.md)