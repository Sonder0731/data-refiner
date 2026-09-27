# `Sample` Operator

---

Wrap Spark’s built-in sample function. 封装 Spark 内置的 sample 函数

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.builtin.sample`
- Class name 类名: `Sample`
- Inherit from 继承于: [Sampler](../meta_operator/sampler.md)
- Test code 测试代码: [test code](../../../tests/ops/builtin/test_sample.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`sample`|`Union[int, float]`|`None`|`True`|Sample size or ratio||
|`seed`|`<class 'int'>`|`777`|`False`|No description 无描述||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束
## Constraint 约束

### Input Column Constraints 输入列约束
* The component operates on any generic DataFrame without restricting specific input columns. 该组件在任意通用 DataFrame 上运行，不限制特定的输入列。
* The initial schema of the input DataFrame is fully preserved during the execution. 输入 DataFrame 的初始 Schema 在执行期间被完全保留。

### Argument and Column Mapping 参数与列的映射
* `sample` -> Sampling Control 采样控制: This parameter defines either the exact count or the percentage of rows to be sampled from the input DataFrame. 该参数定义了从输入 DataFrame 中采样的确切行数或百分比。
  * When `sample` is a `float` between 0 and 1 当 `sample` 为 0 到 1 之间的浮点数时: It maps to the direct sampling fraction parameter in the Spark sample operator. 它映射为 Spark 采样算子中直接使用的抽样比例参数。
  * When `sample` is an `int` or a `float` greater than 1 当 `sample` 为整数或大于 1 的浮点数时: It represents the targeted exact row count, which is mapped to a dynamically calculated fraction based on the total row count. 它表示目标确切行数，该行数被映射为一个基于总行数动态计算出的比例。
* `seed` -> Randomization Control 随机化控制: This parameter determines the random seed for the sampling operator to ensure reproducibility. 该参数决定采样算子的随机种子以确保可复现性。

### Schema Transformation Process Schema 转换过程
* Initial State 初始状态: The DataFrame retains its original schema with all existing columns and their respective data types unchanged. DataFrame 保持其原始 Schema，所有现有列及其各自的数据类型均未改变。
* Intermediate Operations 中间操作: The component invokes temporary persistence and row counting operations without introducing any intermediate columns or altering existing data types. 组件调用临时持久化和行数统计操作，不引入任何中间列，也不改变现有的数据类型。
* Type Mutation 类型变更: No column data types are mutated or cast during the processing lifecycle. 在处理生命周期中，没有任何列的数据类型被变更或转换。

### Output Schema Final State 输出 Schema 最终态
* The output DataFrame contains identical columns and identical data types as the input DataFrame. 输出 DataFrame 包含与输入 DataFrame 完全相同的列和完全相同的数据类型。
* The schema structure remains strictly unchanged, while only the total number of rows is reduced according to the sampling logic. Schema 结构严格保持不变，仅总行数根据采样逻辑减少。

🏡 Back to [operator market 算子市场](../ops_market.md)