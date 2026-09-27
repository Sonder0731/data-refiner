# `TfIdfMapper` Operator

---

calculates the TF-IDF score for each word in a given text field. 计算语料库中某篇文档内每个词的 TF-IDF 分数

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.tfidf_mapper`
- Class name 类名: `TfIdfMapper`
- Inherit from 继承于: [SimpleMapper](../meta_operator/simple_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_tf_idf_mapper.py)
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
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `field` -> Target Source Column 目标源列: The input DataFrame must contain this specified column. 输入 DataFrame 必须包含此指定的列。
  * Data Type 数据类型: `ArrayType(StringType())` 数组字符串类型. This column must contain an array of strings representing tokenized words. 该列必须包含代表分词结果的字符串数组。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Token Column 源分词列: This parameter defines the name of the input column containing tokenized text to calculate Term Frequency (TF). 该参数指定包含已分词文本的输入列名称，用于计算词频 (TF)。
* `output_field` -> Result Column 结果列: This parameter defines the name of the newly generated column holding the sorted TF-IDF results. 该参数指定新生成的列名称，用于存放排序后的 TF-IDF 结果。

### Schema Transformation Process Schema 转换过程
* Phase 1: Intermediate Term Frequency Calculation 阶段 1：中间词频计算
  * An intermediate key `"tf"` of type `MapType(StringType(), IntegerType())` is injected into each row dictionary via an RDD map operation. 通过 RDD map 操作，一个类型为 `MapType(StringType(), IntegerType())` 的中间键 `"tf"` 被注入到每行字典中。
  * This intermediate data structure is used to build a global Inverse Document Frequency (IDF) dictionary via a flatMap action and a broadcast variable. 该中间数据结构通过 flatMap 算子和广播变量用于构建全局逆文档频率 (IDF) 字典。
* Phase 2: TF-IDF Calculation and Schema Evolution 阶段 2：TF-IDF 计算与 Schema 演变
  * The intermediate `"tf"` key is removed from each row during the second RDD map operation. 在第二个 RDD map 操作期间，中间键 `"tf"` 从每行中被移除。
  * A new column defined by `output_field` is appended to the DataFrame. 一个由 `output_field` 指定的新列被追加到 DataFrame 中。

### Output Schema Final State 输出 Schema 最终态
* Original Columns 原始列: All schema columns existing in the input DataFrame are retained with their original data types. 输入 DataFrame 中存在的所有 Schema 列都将以其原始数据类型保留。
* `output_field` -> TF-IDF Results Column TF-IDF 结果列: The final generated output column containing scored and sorted terms. 最终生成的包含评分和排序词项的输出列。
  * Data Type 数据类型: `ArrayType(StructType([StructField('_1', StringType()), StructField('_2', IntegerType()), StructField('_3', IntegerType()), StructField('_4', DoubleType())]))` 结构数组类型. Each element in the array represents a struct containing the term string, term frequency, document frequency, and the final calculated TF-IDF score rounded to 4 decimal places. 数组中的每个元素代表一个结构体，包含词项字符串、词频、文档频率以及四舍五入保留 4 位小数的最终 TF-IDF 分数。

🏡 Back to [operator market 算子市场](../ops_market.md)