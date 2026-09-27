# `Explode` Operator

---

Wrap Spark’s built-in explode function. 封装 Spark 内置的 explode 函数

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.builtin.explode`
- Class name 类名: `Explode`
- Inherit from 继承于: [SingleInMultiOutMapper](../meta_operator/single_in_multi_out_mapper.md)
- Test code 测试代码: [test code](../../../tests/ops/builtin/test_explode.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`exploded_cols`|`List[str]`|`None`|`True`|The list of column names generated after explosion. If the input field is an Array, this list should contain 1 element (the exploded value); if it is a Map, it should contain 2 elements (corresponding to the key and value respectively). 拆解后生成的列名列表。若输入字段为 Array 类型，此列表应包含 1 个元素（即拆解后的值）；若为 Map 类型，此列表应包含 2 个元素（分别对应 key 和 value）。||
|`field`|`<class 'str'>`|`None`|`True`|The field name to map on. 要进行映射的字段名||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### 1. Input Column Constraints 输入列约束
* `field` -> Source Column 源列: This column must exist in the input DataFrame and its DataType must be either `ArrayType` or `MapType`. 该列必须存在于输入 DataFrame 中，且其数据类型必须为 `ArrayType` 或 `MapType`。
* `columns` -> Retained Columns 保留列: All existing columns in the input DataFrame will be retained during the transformation. 输入 DataFrame 中的所有现有列将在转换过程中予以保留。

### 2. Argument and Column Mapping 参数与列的映射
* `exploded_cols` -> Output Columns 输出列: This parameter specifies the names of the new columns generated after the explosion. 该参数指定炸裂后生成的新列的列名。
  * When `field` is `ArrayType` 当 `field` 为数组类型时: `exploded_cols` must contain exactly 1 element, which maps to the exploded value column. `exploded_cols` 必须恰好包含 1 个元素，映射为炸裂后的值列。
  * When `field` is `MapType` 当 `field` 为映射类型时: `exploded_cols` must contain exactly 2 elements, which map to the exploded key column and value column respectively. `exploded_cols` 必须恰好包含 2 个元素，分别映射为炸裂后的键列和值列。

### 3. Schema Transformation Process Schema 转换过程
* Initial State 初始状态: The DataFrame contains all original columns defined in `columns`. DataFrame 包含 `columns` 中定义的所有原始列。
* Transformation Step 转换步骤: The `F.explode` function is applied to the column specified by `field`. The resulting elements are aliased using the names provided in `exploded_cols` and appended to the DataFrame via a `select` operation. 对 `field` 指定的列应用 `F.explode` 函数。转换后的元素使用 `exploded_cols` 中提供的名称进行重命名，并通过 `select` 操作追加到 DataFrame 中。
* Intermediate Columns 中间列: No temporary or intermediate columns are created or deleted; the operation directly appends the final exploded columns. 没有创建或删除任何临时或中间列；该操作直接追加最终的炸裂列。

### 4. Output Schema Final State 输出 Schema 最终态
* Retained Columns 保留列: All original columns from the input DataFrame remain unchanged in both name and DataType. 输入 DataFrame 的所有原始列在名称和数据类型上均保持不变。
* New Columns Generated 新生成列: 
  * If `field` was `ArrayType` 如果 `field` 为数组类型: One new column named `exploded_cols[0]` is added. Its DataType matches the element type of the original array. 新增一个名为 `exploded_cols[0]` 的新列。其数据类型与原数组的元素类型一致。
  * If `field` was `MapType` 如果 `field` 为映射类型: Two new columns named `exploded_cols[0]` and `exploded_cols[1]` are added. Their DataTypes match the key type and value type of the original map respectively. 新增两个名为 `exploded_cols[0]` 和 `exploded_cols[1]` 的新列。它们的数据类型分别与原映射的键类型和值类型一致。

🏡 Back to [operator market 算子市场](../ops_market.md)