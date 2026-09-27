# `FastTextModelMapper` Operator

---

A common fasttext model mapper that takes a text field and applies a fasttext model to it. 一个常用的 fasttext 模型映射器，它接收一个文本字段并为其应用 fasttext 模型

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.fasttext_model_mapper`
- Class name 类名: `FastTextModelMapper`
- Inherit from 继承于: [SimpleMapper](../meta_operator/simple_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_fast_text_model_mapper.py)
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
|`label_prefix`|`<class 'str'>`|`__label__`|`False`|The prefix of the label in the fasttext model.||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`model_path`|`<class 'str'>`|`None`|`True`|The path to the fasttext model file.||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`output_field`|`<class 'str'>`|`None`|`True`|The field name to store the mapped value. 用于存储映射结果的字段名||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`prob_field`|`<class 'str'>`|`None`|`True`|The name of the field to store the probability of each label.||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be either `StringType` or `ArrayType(StringType())`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType` 或 `ArrayType(StringType())`。

### Argument and Column Mapping 参数与列的映射
* `model_path` -> Model File Path 模型文件路径: This parameter specifies the file path of the fasttext model, which is broadcasted to worker nodes to execute predictions on the text data. 该参数指定 fasttext 模型的物理路径，该模型会被广播至工作节点以对文本数据执行预测。
* `label_prefix` -> Label Prefix 标签前缀: This parameter defines the prefix string used by the fasttext model to filter and clean the predicted label outputs. 该参数定义了 fasttext 模型使用的标签前缀字符串，用于过滤和清洗预测输出的标签。
* `field` -> Input Column 输入列: This parameter specifies the source text column that contains the samples to be fed into the fasttext model for prediction. 该参数指定包含要输入至 fasttext 模型进行预测的样本的源文本列。
* `output_field` -> Predicted Label Column 预测标签列: This parameter defines the name of the new column where the predicted label text will be stored. 该参数定义了存储预测标签文本的新列的列名。
* `prob_field` -> Probability Column 概率列: This parameter defines the name of the new column where the probability scores of the predicted labels will be stored. 该参数定义了存储预测标签概率分数的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to perform distributed prediction inside partitions using the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便使用 `mapPartitions` 算子在分区内部执行分布式预测。
* Within each partition, the fasttext model is loaded via `FastTextModelLoader`, and two distinct key-value pairs represented by `output_field` and `prob_field` are dynamically appended to each row dictionary. 在每个分区内部，通过 `FastTextModelLoader` 加载 fasttext 模型，并向每个行字典中动态追加由 `output_field` 和 `prob_field` 表示的两个不同的键值对。
* The transformed RDD is re-converted back into a structured DataFrame using the `toDF()` method, mapping the expanded row dictionaries to an upgraded schema structure. 转换后的 RDD 通过 `toDF()` 方法重新转换为结构化的 DataFrame，将扩展后的行字典映射到升级后的 Schema 结构中。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `StringType`。
* `prob_field`: A secondary new column is appended to the final DataFrame, and its data type is explicitly determined as `DoubleType` or `FloatType` depending on the model's output precision. 最终 DataFrame 中追加了第二个新列，其数据类型根据模型的输出精度被明确确定为 `DoubleType` 或 `FloatType`。

🏡 Back to [operator market 算子市场](../ops_market.md)