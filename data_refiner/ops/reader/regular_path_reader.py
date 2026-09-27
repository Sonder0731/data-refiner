from pydantic import BaseModel, Field
from pyspark.sql import DataFrame

from data_refiner.core.meta_operator import PathReader, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_params


@processing_operator
class RegularPathReader(PathReader, OperatorConstraint):
    """
    Read files with a Spark DataFrameReader.

    Supported formats include csv, json, parquet, orc, text, image, and
    binaryFile. Use `options` to configure format-specific behavior such as
    CSV headers, delimiters, quoting, escaping, multiline records,
    encoding,
    schema inference, and malformed-record handling.

    使用 Spark DataFrameReader 读取文件。支持 csv、json、parquet、orc、
    text、image 和 binaryFile 等格式。可以通过 `options` 配置表头、分隔符、
    引号、转义、多行记录、编码、Schema 推断和坏记录处理等格式相关行为。
    """

    CONSTRAINT = """### Input Source Constraints 输入源约束

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
  * Image 和 binaryFile 使用 Spark 预定义的 Schema。"""

    class PathReaderParams(BaseModel):
        format: str = Field(
            ...,
            description="The format of the file to be read: csv, json, parquet, orc, text, image, binaryFile.",
        )
        options: dict = Field(
            default=dict(),
            description=(
                "Additional Spark DataFrameReader options. Supported options "
                "depend on the selected file format. For example, CSV supports "
                "header, multiLine, delimiter, quote, escape, encoding, and mode. "
                "User-provided options override the operator's default options. "
                "传递给 Spark DataFrameReader 的附加读取选项。支持的选项取决于文件格式；"
                "例如 CSV 支持 header、multiLine、delimiter、quote、escape、"
                "encoding 和 mode。用户配置会覆盖算子的默认配置。"
            ),
        )

    config = PathReaderParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.format = params.format
        self.options = params.options

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        spark = kwargs.get("spark")
        if self.format == "csv":
            self.options.update({"header": True})
        return spark.read.format(self.format).options(**self.options).load(self.input_path)
