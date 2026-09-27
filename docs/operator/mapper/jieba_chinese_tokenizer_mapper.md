# `JiebaNormalTokenizerMapper` Operator

---

Tokenize the input text using jieba library. 使用 jieba 库对输入文本进行分词

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.jieba_chinese_tokenizer_mapper`
- Class name 类名: `JiebaNormalTokenizerMapper`
- Inherit from 继承于: [SimpleMapper](../meta_operator/simple_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_jieba_normal_tokenizer_mapper.py)
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
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`output_field`|`<class 'str'>`|`None`|`True`|The field name to store the mapped value. 用于存储映射结果的字段名||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`punctuation_filter`|`<class 'bool'>`|`True`|`False`|No description 无描述||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `punctuation_filter` -> Punctuation Filter Toggle 标点符号过滤开关: This parameter determines whether punctuation tokens should be removed from the generated token list within the partition processor. 该参数决定了在分区处理器内部是否应从生成的词标记列表中移除标点符号标记。
* `field` -> Input Column 输入列: This parameter specifies the source text column that will be segmented into tokens using the jieba library. 该参数指定将使用 jieba 库进行分词处理的源文本列。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the extracted token array will be stored. 该参数定义了存储提取出的词标记数组的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to implement custom tokenization logic inside partitions via the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便通过 `mapPartitions` 算子在分区内部实现自定义的分词逻辑。
* Within the `_tokenize` iterator, conditional logic evaluates `punctuation_filter` to filter out punctuation matching Unicode category 'P', and a new key-value pair defined by `output_field` is dynamically added to each row dictionary. 在 `_tokenize` 迭代器内部，条件逻辑评估 `punctuation_filter` 以过滤掉匹配 Unicode 属性为 'P' 的标点符号，并向每个行字典中动态添加一个由 `output_field` 定义的新键值对。
* The modified RDD is transformed back into a DataFrame format using the `toDF()` method, mapping the expanded row layout into an updated schema. 修改后的 RDD 通过 `toDF()` 方法重新转换为 DataFrame 格式，将扩展后的行布局映射到更新后的 Schema 中。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `ArrayType(StringType())`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `ArrayType(StringType())`。

🏡 Back to [operator market 算子市场](../ops_market.md)