# `LiteLLMMapper` Operator

---

Ask LLm for help and return json object. 请求 LLM API，并返回 JSON 的对象

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.litellm_mapper`
- Class name 类名: `LiteLLMMapper`
- Inherit from 继承于: [SimpleMapper](../meta_operator/simple_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_lite_llm_mapper.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`answer_flatten`|`<class 'bool'>`|`False`|`False`|Whether to flatten the answer, aka whether use a field to contain the whole answer, valid only when return_format=='json'||
|`api_key`|`<class 'str'>`|`None`|`True`|API key or executor environment reference such as `${LITELLM_API_KEY}`.||
|`base_url`|`<class 'str'>`|`None`|`False`|No description 无描述||
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`field`|`<class 'str'>`|`None`|`True`|The field name to map on. 要进行映射的字段名||
|`frequency_penalty`|`<class 'float'>`|`None`|`False`|No description 无描述||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`max_retries`|`<class 'int'>`|`3`|`False`|No description 无描述||
|`max_tokens`|`Optional[int]`|`None`|`False`|No description 无描述||
|`model_name`|`<class 'str'>`|`None`|`True`|No description 无描述||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`output_field`|`<class 'str'>`|`None`|`True`|The field name to store the mapped value. 用于存储映射结果的字段名||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`prompt_template`|`Optional[str]`|`None`|`True`|Prompt template for LLM||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`return_format`|`Literal['json', 'text']`|`json`|`False`|Return format of the LLM output||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||
|`temperature`|`<class 'float'>`|`0.2`|`False`|No description 无描述||


## Constraint 约束

### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `prompt_template` -> Prompt Formatting Template 提示词格式化模板: This parameter defines the template string populated by mapping existing row field keys to their corresponding row dictionary values. 该参数定义了通过将现有行字段键映射到其对应的行字典值来填充的模板字符串。
* `return_format` -> Response Structural Format 响应结构格式: This parameter determines whether the Large Language Model (LLM) structural client response returns an unstructured `text` string or a parsed `json` dictionary. 该参数决定了大型语言模型（LLM）结构化客户端响应返回的是非结构化 `text` 字符串还是解析后的 `json` 字典。
* `answer_flatten` -> Dictionary Flattening Toggle 字典展平开关: This parameter dictates whether the parsed dictionary elements should be expanded directly into top-level row properties, which is valid only when `return_format` is set to `json`. 该参数指示是否应将解析后的字典元素直接扩展为顶级行属性，仅在 `return_format` 设置为 `json` 时有效。
* `output_field` -> Output Column 输出列: This parameter defines the target column name where the raw model response string or unflattened dictionary is stored when flattening is bypassed or inapplicable. 该参数定义了当绕过或不适用展平逻辑时，存储原始模型响应字符串或未展平字典的目标列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is transformed into a Resilient Distributed Dataset (`RDD`) to handle API invocation state management across cluster workers via the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便通过 `mapPartitions` 算子在集群工作节点之间处理 API 调用状态管理。
* Inside the partition loop, each row dictionary dynamically formats the `prompt_template` string using its internal schema fields as parameters. 在分区循环内部，每个行字典使用其内部 Schema 字段作为参数来动态格式化 `prompt_template` 字符串。
* When `answer_flatten` is true, `return_format` is `json`, and the client payload output evaluates to a valid dictionary, the schema shifts dynamically by appending all root-level keys of that JSON object directly into the row dictionary. 当 `answer_flatten` 为真、`return_format` 为 `json` 且客户端负载输出评估为有效字典时，Schema 通过将该 JSON 对象的所有根级键直接追加到行字典中来进行动态转变。
* If the flattening prerequisites are not met, a single column specified by `output_field` is appended to the current row key collection. 如果不满足展平前提条件，则将由 `output_field` 指定的单个列追加到当前行键集合中。
* The mutated RDD is converted back into a DataFrame structural layout via the `toDF()` operator, standardizing the dynamic schema state. 改变后的 RDD 通过 `toDF()` 算子重新转换为 DataFrame 结构化布局，从而标准化动态 Schema 状态。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* Under standard or non-flattened execution (`answer_flatten` is False or `return_format` is `text`): A single column designated by `output_field` is appended to the final DataFrame, carrying a data type of `StringType`. 在标准或未展平的执行下（`answer_flatten` 为 False 或 `return_format` 为 `text`）：最终 DataFrame 中追加了一个由 `output_field` 指定的新列，其数据类型为 `StringType`。
* Under flattened JSON execution (`answer_flatten` is True and `return_format` is `json`): No specific `output_field` is guaranteed; instead, multiple dynamic columns are appended to the final DataFrame corresponding to the root keys of the returned JSON structure, with data types inferred as `StringType`. 在展平 JSON 的执行下（`answer_flatten` 为 True 且 `return_format` 为 `json`）：不保证生成特定的 `output_field`；相反，最终 DataFrame 中会追加多个对应于返回 JSON 结构根键的动态列，其数据类型被推断为 `StringType`。

🏡 Back to [operator market 算子市场](../ops_market.md)
