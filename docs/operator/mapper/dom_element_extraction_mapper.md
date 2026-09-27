# `DomElementExtractionMapper` Operator

---

Extracts specified DOM elements and their subtrees from HTML content using CSS selectors or XPath, outputting the serialized HTML string of the matched elements. 使用 CSS 选择器或 XPath 从 HTML 内容中提取指定的 DOM 元素及其子树，并输出匹配元素的序列化 HTML 字符串

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.dom_element_extraction_mapper`
- Class name 类名: `DomElementExtractionMapper`
- Inherit from 继承于: [SimpleMapper](../meta_operator/simple_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_dom_element_extraction_mapper.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`css_selector`|`<class 'str'>`|``|`False`|CSS selector to extract DOM elements.||
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
|`xpath_selector`|`<class 'str'>`|``|`False`|XPath selector to extract DOM elements.||


## Constraint 约束

### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `css_selector` -> CSS Extraction Rules CSS 提取规则: When this parameter is provided, the partition operator evaluates this specific CSS path against the source HTML column specified by `field`. 当提供该参数时，分区算子会针对由 `field` 指定的源 HTML 列评估该特定的 CSS 路径。
* `xpath_selector` -> XPath Extraction Rules XPath 提取规则: When this parameter is provided, the partition operator evaluates this specific XPath expression against the source HTML column specified by `field`. 当提供该参数时，分区算子会针对由 `field` 指定的源 HTML 列评估该特定的 XPath 表达式。
* `field` -> Input Column 输入列: This parameter specifies the target column containing the raw HTML strings from which DOM elements will be extracted. 该参数指定包含原始 HTML 字符串的目标列，将从该列中提取 DOM 元素。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the serialized HTML string of the matched DOM elements will be stored. 该参数定义了存储匹配到的 DOM 元素序列化 HTML 字符串的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to execute the `extract_elements` method across partitions via the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便通过 `mapPartitions` 算子在分区之间执行 `extract_elements` 方法。
* Within the partition function, conditional logic switches between `selector.css()` and `selector.xpath()` based on the defined parameters, and a new key-value pair represented by `output_field` is dynamically added to each row dictionary. 在分区函数内部，条件逻辑根据定义的参数在 `selector.css()` 和 `selector.xpath()` 之间切换，并向每个行字典中动态添加了一个由 `output_field` 表示的新键值对。
* The transformed RDD is re-converted back into a DataFrame structural format using the `toDF()` method, which maps the updated dictionary schemas onto a new DataFrame representation. 转换后的 RDD 通过 `toDF()` 方法重新转换为 DataFrame 结构格式，该方法将更新后的字典 Schema 映射到新的 DataFrame 表示中。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `StringType`。

🏡 Back to [operator market 算子市场](../ops_market.md)