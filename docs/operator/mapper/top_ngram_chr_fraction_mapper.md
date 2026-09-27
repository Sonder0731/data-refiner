# `TopNgramChrFractionMapper` Operator

---

Calculate the fraction of the document's total characters that are accounted for by the most frequently occurring N-gram. 计算文档中出现频率最高的 N-gram 所占的字符数，占整个文档总字符数的比例

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.top_ngram_chr_fraction_mapper`
- Class name 类名: `TopNgramChrFractionMapper`
- Inherit from 继承于: [SingleInMultiOutMapper](../meta_operator/single_in_multi_out_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_top_ngram_chr_fraction_mapper.py)
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
|`ngram_range`|`Tuple[int, int]`|`(3, 5)`|`False`|ngram range||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### 1. Input Column Constraints 输入列约束
* `field` -> Source Column 源列: This column must exist in the input DataFrame and its DataType must be `StringType`. 该列必须存在于输入 DataFrame 中，且其数据类型必须为 `StringType`。
* `df.schema.fields` -> Retained Columns 保留列: All existing columns in the input DataFrame will be retained and their original DataTypes will be preserved. 输入 DataFrame 中的所有现有列将被保留，且其原始数据类型将保持不变。

### 2. Argument and Column Mapping 参数与列的映射
* `ngram_range` -> Generated Feature Columns 生成特征列: This parameter defines the inclusive range of N-gram lengths used to calculate the top frequent N-gram character fractions. 该参数定义了用于计算最高频 N-gram 字符比例的 N-gram 长度的闭区间范围。
  * For each integer `n` in `range(ngram_range[0], ngram_range[1] + 1)` 对于 `range(ngram_range[0], ngram_range[1] + 1)` 中的每个整数 `n`: A specific column named `top_n_gram_chr_fraction` will be dynamically mapped and generated. 将动态映射并生成一个名为 `top_n_gram_chr_fraction` 的特定列。

### 3. Schema Transformation Process Schema 转换过程
* Initial State 初始状态: The DataFrame consists of all original input columns, with `field` validated to ensure it contains string data. DataFrame 由所有原始输入列组成，且 `field` 已通过验证以确保其包含字符串数据。
* Transformation Step 转换步骤: The operator calculates the output schema dynamically by iterating through the `ngram_range` and appending new fields via `output_schema.add`. The underlying data is processed in parallel batches using `df.mapInPandas`, where the internal `top_ngram_range_chr_fraction` logic computes values for each row and concatenates the results along the column axis. 算子通过遍历 `ngram_range` 并通过 `output_schema.add` 追加新字段来动态计算输出 Schema。底层数据使用 `df.mapInPandas` 进行并行批处理，其中内部的 `top_ngram_range_chr_fraction` 逻辑计算每行的值并在列轴上拼接结果。
* Intermediate Columns 中间列: Temporary Pandas DataFrames and list objects are created within the streaming iterator to hold batch calculations, but no intermediate columns are written back to the persistent Spark Schema. 在流式迭代器内部创建了临时 Pandas DataFrame 和列表对象以保存批次计算结果，但没有中间列被写回到持久的 Spark Schema 中。

### 4. Output Schema Final State 输出 Schema 最终态
* Retained Columns 保留列: All original columns from the input DataFrame are preserved with their names and DataTypes unchanged. 输入 DataFrame 的所有原始列均予以保留，其名称和数据类型保持不变。
* New Columns Generated 新生成列: 
  * `top_n_gram_chr_fraction` -> Fraction Columns 比例列: Multiple columns will be appended, where `n` spans from the lower bound to the upper bound of `ngram_range`. The DataType for all these newly generated columns is `DoubleType`. 将追加多个列，其中 `n` 跨越 `ngram_range` 的下界到上界。所有这些新生成的列的数据类型均为 `DoubleType`。

🏡 Back to [operator market 算子市场](../ops_market.md)