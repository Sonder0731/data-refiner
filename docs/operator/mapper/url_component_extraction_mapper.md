# `UrlComponentExtractionMapper` Operator

---

Extract url components from a url field. 从 URL 字段中提取 URL 组件

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.mapper.url_component_extraction_mapper`
- Class name 类名: `UrlComponentExtractionMapper`
- Inherit from 继承于: [SingleInMultiOutMapper](../meta_operator/single_in_multi_out_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/mapper/test_url_component_extraction_mapper.py)
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
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`required_components`|`Set[Literal['args', 'fragment', 'host', 'origin', 'path', 'port', 'query', 'scheme']]`|`{'path', 'query', 'host', 'scheme', 'port'}`|`False`|The list of URL components to extract. Elements must be chosen from: 'scheme', 'netloc', 'path', 'params', 'query', 'fragment'. 需要提取的 URL 组件列表。元素必须从以下集合中选择：'scheme', 'netloc', 'path', 'params', 'query', 'fragment'。||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `field` -> Input Column 输入列: The component requires a specific source column containing URL strings to perform the extraction logic. 组件需要一个包含 URL 字符串的特定源列来执行提取逻辑。
  * Data Type 数据类型: `StringType` 字符串类型. The schema validation explicitly checks that this column must be a string type. Schema 校验明确检查该列必须为字符串类型。

### Argument and Column Mapping 参数与列的映射
* `required_components` -> Output Struct Fields 输出结构体字段: This parameter defines a set of targeted URL components to be extracted from the source URL. 该参数定义了需要从源 URL 中提取的目标 URL 组件集合。
  * When `required_components` contains specific keys 当 `required_components` 包含特定键时: Each selected component string (e.g., `scheme`, `host`, `path`, `port`, `query`, `fragment`, `args`, `origin`) directly determines a corresponding field name in the temporary Struct column and the final expanded column names. 每个选定的组件字符串（例如 `scheme`, `host`, `path`, `port`, `query`, `fragment`, `args`, `origin`）直接决定了临时结构体列中的对应字段名以及最终炸裂展开后的列名。

### Schema Transformation Process Schema 转换过程
* Intermediate Column Creation 中间列创建: A temporary column named `"1"` is created using a User Defined Function (UDF). 使用用户自定义函数（UDF）创建了一个名为 `"1"` 的临时列。
  * The column `"1"` is of `StructType`, containing fields dynamically defined by `required_components`. 列 `"1"` 的类型为 `StructType`，其中包含由 `required_components` 动态定义的字段。
  * Each field inside the `StructType` is explicitly typed as `StringType` and is nullable. `StructType` 内部的每个字段都被明确定义为 `StringType` 类型且允许为空。
* Column Expansion and Deletion 列展开与删除: The component applies the star expansion operator (`"1.*"`) on the temporary struct column. 组件在临时结构体列上应用星号展开算子（`"1.*"`）。
  * The temporary struct container column `"1"` itself is flattened and removed from the selection. 临时结构体容器列 `"1"` 本身被扁平化展开并从选择结果中移除。
  * All fields within the struct are promoted to top-level columns in the resulting dataset. 结构体内部的所有字段都被提升为结果数据集中的顶级列。

### Output Schema Final State 输出 Schema 最终态
* Preserved Columns 保留列: All columns originally present in the input DataFrame (`original_columns`) are preserved unchanged. 输入 DataFrame 中原本存在的所有列（`original_columns`）都将原封不动地保留。
* Newly Added Columns 新增列: New columns are appended to the DataFrame based on the elements specified in `required_components`. 根据 `required_components` 中指定的元素，新列将被追加到 DataFrame 中。
  * Each newly added column will have the exact name as the component identifier (e.g., `scheme`, `host`, etc.). 每个新增列的列名将与组件标识符完全一致（例如 `scheme`, `host` 等）。
  * Data Type 数据类型: `StringType` 字符串类型. All extracted component columns are strictly typed as string format. 所有提取出的组件列都严格限制为字符串格式。

🏡 Back to [operator market 算子市场](../ops_market.md)