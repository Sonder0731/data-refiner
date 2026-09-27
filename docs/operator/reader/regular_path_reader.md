# `RegularPathReader` Operator

---

Read files with a Spark DataFrameReader.<br><br>    Supported formats include csv, json, parquet, orc, text, image, and<br>    binaryFile. Use `options` to configure format-specific behavior such as<br>    CSV headers, delimiters, quoting, escaping, multiline records,<br>    encoding,<br>    schema inference, and malformed-record handling.<br><br>    使用 Spark DataFrameReader 读取文件。支持 csv、json、parquet、orc、<br>    text、image 和 binaryFile 等格式。可以通过 `options` 配置表头、分隔符、<br>    引号、转义、多行记录、编码、Schema 推断和坏记录处理等格式相关行为。

## Basic Information 基本信息
- Module path 模块路径: `data_refiner.ops.reader.regular_path_reader`
- Class name 类名: `RegularPathReader`
- Inherit from 继承于: [PathReader](../meta_operator/path_reader.md)
- Test code 测试代码: [test code](../../../tests/ops/reader/test_regular_path_reader.py)
- Operator type 算子类型: `processing operator`
- Pipeline applicability 流水线适用性: `Yes`
- Example 示例: `None`
## Specific Parameters 具体参数 

| Parameter 参数 | Type 类型 | Default 默认值 | Required 必填 | Description 描述| Options 选项 |
|:--:|:--------:|:------------:|:------------:|:---------------------------------------|:--|
|`cache`|`Literal['disk', 'memory', 'memory_disk']`|`disk`|`False`|Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`|- `disk`<br/>disk cache 仅磁盘缓存<br/>- `memory`<br/>memory cache 仅内存缓存<br/>- `memory_disk`<br/>memory and disk cache 内存和磁盘缓存<br/>|
|`count`|`<class 'bool'>`|`False`|`False`|Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数|- `True`<br/>count rows 统计行数<br/>- `False`<br/>not count rows 不统计行数<br/>|
|`drop`|`Optional[List[str]]`|`None`|`False`|Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列||
|`format`|`<class 'str'>`|`None`|`True`|The format of the file to be read: csv, json, parquet, orc, text, image, binaryFile.||
|`input_path`|`<class 'str'>`|`None`|`True`|The input data location path. 输入数据路径||
|`limit`|`Optional[int]`|`None`|`False`|Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数||
|`options`|`<class 'dict'>`|`{}`|`False`|Additional Spark DataFrameReader options. Supported options depend on the selected file format. For example, CSV supports header, multiLine, delimiter, quote, escape, encoding, and mode. User-provided options override the operator's default options. 传递给 Spark DataFrameReader 的附加读取选项。支持的选项取决于文件格式；例如 CSV 支持 header、multiLine、delimiter、quote、escape、encoding 和 mode。用户配置会覆盖算子的默认配置。||
|`output_df`|`<class 'str'>`|`None`|`True`|The output dataframe name. 输出 DataFrame 的名称||
|`partitions`|`Optional[int]`|`None`|`False`|Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量||
|`renames`|`Optional[Dict[str, str]]`|`None`|`False`|Rename the specified columns. 重命名指定的列||
|`select`|`Optional[List[str]]`|`None`|`False`|Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列||
|`show`|`boolean or object {"truncate": boolean}`|`False`|`False`|Controls DataFrame display. JSON examples: {"show": false} disables display; {"show": true} or {"show": {"truncate": true}} displays truncated values; {"show": {"truncate": false}} displays full values. 控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；{"show": {"truncate": false}} 表示展示完整内容。||
|`temp_view_name`|`Optional[str]`|`None`|`False`|Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。||


## Constraint 约束
### Input Source Constraints 输入源约束

  * `input_path`: Path of the source dataset.
    数据源所在的文件或目录路径。
    * The path must be accessible to Spark.
      该路径必须能够被 Spark 访问。
    * The files must be compatible with the selected `format`.
      文件内容必须与所选 `format` 兼容。

  ### Reader Arguments 读取参数

  * `format`: Spark data source format used by `spark.read.format()`.
    传递给 `spark.read.format()` 的数据源格式。
    * Supported formats include `csv`, `json`, `parquet`, `orc`, `text`,
      `image`, and `binaryFile`.
      支持的格式包括 `csv`、`json`、`parquet`、`orc`、`text`、`image`
      和 `binaryFile`。

  * `options`: Format-specific options passed directly to
    `Spark DataFrameReader.options()`.
    直接传递给 `Spark DataFrameReader.options()` 的格式相关配置。
    * Supported option names and values depend on the selected format.
      支持的配置项及取值由所选格式决定。
    * When an option is omitted, Spark's default behavior is used.
      未提供某个配置项时，使用 Spark 对应数据源的默认行为。
    * Invalid or unsupported options are handled by the selected Spark data
      source and may be ignored or cause a runtime error.
      无效或不受支持的配置由对应 Spark 数据源处理，可能被忽略或在运行时失败。

  ### Common Option Examples 常用配置示例

  * CSV:
    `header`, `inferSchema`, `delimiter`, `quote`, `escape`, `multiLine`,
    `encoding`, `mode`, `columnNameOfCorruptRecord`, and
    `recursiveFileLookup`.

  * JSON:
    `multiLine`, `mode`, `encoding`, `samplingRatio`,
    `columnNameOfCorruptRecord`, and `recursiveFileLookup`.

  * Parquet and ORC:
    `mergeSchema`, `recursiveFileLookup`, and format-specific reader options.

  ### Reading Process 读取过程

  The operator constructs the DataFrame using:

  `spark.read.format(format).options(**options).load(input_path)`

  算子通过以下方式构建 DataFrame：

  `spark.read.format(format).options(**options).load(input_path)`

  No additional column casting, renaming, filtering, or schema transformation
  is
  performed by this operator after loading.

  该算子在读取完成后不会额外执行字段类型转换、重命名、过滤或 Schema 转换。

  ### Output Schema 输出 Schema

  The output is a Spark DataFrame. Its schema is determined by the selected
  format, source data, Spark defaults, and supplied `options`.

  输出为 Spark DataFrame，其 Schema 由数据格式、源数据、Spark 默认行为以及
  传入的 `options` 共同决定。

  * CSV columns are normally `StringType` when `inferSchema` is not enabled.
    When `header=true`, the first row is used as column names.
  * JSON schema is inferred from the input records.
  * Parquet and ORC schema comes from file metadata.
  * Text normally produces a single `value` column.
  * Image and binaryFile use Spark's predefined schemas.

  * CSV 未启用 `inferSchema` 时通常生成 `StringType` 字段；
    `header=true` 时首行用于生成字段名。
  * JSON 根据输入记录推断 Schema。
  * Parquet 和 ORC 从文件元数据读取 Schema。
  * Text 通常生成单个 `value` 字段。
  * Image 和 binaryFile 使用 Spark 预定义的 Schema。

🏡 Back to [operator market 算子市场](../ops_market.md)