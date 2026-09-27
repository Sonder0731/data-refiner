# `PathWriter` Operator

---

Write data to a file system, format: "csv", "parquet", "json", "orc", "text". 将数据写入文件系统, 支持格式"csv", "parquet", "json", "orc", "text"

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.writer.path_writer`
- Class name 类名: `PathWriter`
- Inherit from 继承于: [Writer](../meta_operator/writer.md)
- Test code 测试代码: [test code](../../../tests/ops/writer/test_path_writer.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`format`|`<class 'str'>`|`None`|`True`|The format of the data to write.||
|`input_df`|`<class 'str'>`|`None`|`True`|The input dataframe name. 输入 DataFrame 的名称||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`mode`|`<class 'str'>`|`None`|`True`|The mode to write the data in.||
|`output_df`|`Optional[str]`|`None`|`False`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`path`|`<class 'str'>`|`None`|`True`|The path to write the data to.||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `input_df` -> Input Dataset Structure 输入数据集结构: The input DataFrame serves as the source layout to be flushed onto the file system. 输入 DataFrame 充当要刷新到文件系统中的源结构布局。
  * Data Type 数据类型: `Any` 任意类型. The dataset can contain any valid Spark SQL compliant data types, provided they are supported by the target format writer (e.g., standard formats like Parquet, ORC, JSON, CSV, or Text). 该数据集可以包含任何有效的、兼容 Spark SQL 的数据类型，前提是它们受到目标格式写入器（例如 Parquet, ORC, JSON, CSV 或 Text 等标准格式）的支持。

### Argument and Column Mapping 参数与列的映射
* `path` -> Destination Storage Path 目标存储路径: This configuration parameter defines the destination URI block where the dataframe's structural layout is serialized. 该配置参数定义了序列化 DataFrame 结构布局的目标 URI 块。
* `format` -> Data Layout Format 结构布局格式: This parameter determines the underlying storage driver (such as `"parquet"`, `"orc"`, `"json"`, `"csv"`, or `"text"`) used to map columns into disk blocks. 该参数决定了将列映射到磁盘块中的底层存储驱动程序（例如 `"parquet"`, `"orc"`, `"json"`, `"csv"` 或 `"text"`）。
* `mode` -> I/O Transaction Mode 输入输出事务模式: This parameter governs the file system overwrite or append behaviors when acting on the physical storage location. 该参数在对物理存储位置进行操作时，控制文件系统的覆盖或追加行为。

### Schema Transformation Process Schema 转换过程
* Phase 1: Storage Driver Binding and Options Setup 阶段 1：存储驱动绑定与选项设置
  * The operator passes the configuration tokens to `df.write.format(format).mode(mode)`, dynamically initializing a Spark `DataFrameWriter` instance. 算子将配置标记传递给 `df.write.format(format).mode(mode)`，动态初始化一个 Spark `DataFrameWriter` 实例。
  * This phase establishes downstream storage properties but does not change the programmatic column mapping sequence or row properties within the memory layout. 此阶段建立下游存储属性，但不改变内存布局中的程序化列映射顺序或行属性。
* Phase 2: Action Execution and File Serialization 阶段 2：Action 算子执行与文件序列化
  * The `.save(path)` method is triggered, executing a Spark Action that marshals the schema architecture and flushes row datasets into file formats matching the `format` configuration. 触发 `.save(path)` 方法，执行一个 Spark Action 算子，该算子编组 schema 架构并将行数据集刷新为与 `format` 配置匹配的文件格式。
  * No internal columns are created, renamed, dropped, or cast during this execution pipeline. 在该执行管道期间，没有内部列被创建、重命名、丢弃或进行类型转换。

### Output Schema Final State 输出 Schema 最终态
* Original Passthrough Schema 原始透传 Schema: The operator strictly returns the identical input DataFrame instance untouched, retaining the exact structural names, sequence order, nullability settings, and primitive data types. 算子严格返回未触动的相同输入 DataFrame 实例，保留准确的结构名称、排列顺序、可空性设置和基础数据类型。
  * Although data records are exported and formatted onto physical disks, the continuous memory pipeline's structural footprint remains entirely unchanged. 尽管数据记录被导出并格式化到物理磁盘上，但连续内存管道的结构蓝图完全保持不变。

🏡 Back to [operator market 算子市场](../ops_market.md)