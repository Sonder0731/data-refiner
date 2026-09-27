# `WholeTextFileReader` Operator

---

Reads the whole text file as a string. Useful for reading large text files like .logs, .html files, etc. 将整个文本文件读取为字符串。适用于读取大型文本文件，例如 .log 文件、.html 文件等

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.reader.whole_text_file_reader`
- Class name 类名: `WholeTextFileReader`
- Inherit from 继承于: [PathReader](../meta_operator/path_reader.md)
- Test code 测试代码: [test code](../../../tests/ops/reader/test_whole_text_file_reader.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`input_path`|`<class 'str'>`|`None`|`True`|The input data location path. 输入数据路径||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束

### Input Column Constraints 输入列约束
* `input_path` -> Target External Storage 目标外部存储: The directory or file path residing on the file system serves as the raw input to the reader engine. 位于文件系统上的目录或文件路径充当读取器引擎的原始输入。
  * Data Type 数据类型: `Any` 任意类型. The target source points to physical files (e.g., `.logs`, `.html`, `.txt`) stored on disk rather than an existing structural dataset. 目标源指向存储在磁盘上的物理文件（例如 `.logs`, `.html`, `.txt`），而不是现有的结构化数据集。

### Argument and Column Mapping 参数与列的映射
* `input_path` -> Ingestion Directory String 导入目录字符串: This parameter defines the file URI or directory path passed down to the underlying Spark core execution context. 该参数定义了传递给底层 Spark 核心执行上下文的文件 URI 或目录路径。
  * No individual configuration arguments are mapped to specific inline columns; instead, the entire structural mapping of the output DataFrame is statically preset by the operator logic. 没有单独的配置参数映射到特定的行内列；相反，输出 DataFrame 的整个结构映射由算子逻辑静态预设。

### Schema Transformation Process Schema 转换过程
* Phase 1: Low-Level Pair RDD Generation 阶段 1：低级键值对 RDD 生成
  * The Spark context invokes the `spark.sparkContext.wholeTextFiles()` method using the directory specified by `input_path`. Spark 上下文使用 `input_path` 指定的目录调用 `spark.sparkContext.wholeTextFiles()` 方法。
  * This initializes a specialized low-level Pair RDD where each individual file is treated as a single undivided record. The key represents the absolute file path, and the value represents the entire text payload of that file. 这将初始化一个特化的低级键值对 RDD，其中每个单独的文件都被视为一条未分割的记录。键代表绝对文件路径，值代表该文件的整个文本净荷。
* Phase 2: Schema Enforcement and DataFrame Creation 阶段 2：Schema 强制结合与 DataFrame 创建
  * A fixed metadata structure `schema` is explicitly instantiated via `StructType`, containing exactly two `StringType()` fields named `"file_path"` and `"text"`. 一个固定的结构化元数据 `schema` 通过 `StructType` 被显式实例化，包含且仅包含两个名为 `"file_path"` 和 `"text"` 的 `StringType()` 字段。
  * The operator invokes `spark.createDataFrame(rdd, schema)` to bind the compiled structure directly onto the low-level Pair RDD, generating a strongly-typed structured dataset. 算子调用 `spark.createDataFrame(rdd, schema)` 将编译好的结构直接绑定到低级键值对 RDD 上，从而生成一个强类型的结构化数据集。

### Output Schema Final State 输出 Schema 最终态
* `file_path` -> Absolute Source Path Column 绝对源路径列: The first output field holding the location identifier. 第一个输出列，存放位置标识符。
  * Data Type 数据类型: `StringType()` 字符串类型. This column stores the absolute file URI string generated by the file system reader for each file processed. 该列存储由文件系统读取器为每个已处理文件生成的绝对文件 URI 字符串。
* `text` -> Entire File Content Column 完整文件内容列: The second output field holding the full text string. 第二个输出列，存放完整的文本字符串。
  * Data Type 数据类型: `StringType()` 字符串类型. This column contains the entire raw text block loaded from the corresponding file, read completely as a single continuous string payload. 该列包含从对应文件中加载的整个原始文本块，完全作为一个连续的单一字符串净荷读取。

🏡 Back to [operator market 算子市场](../ops_market.md)