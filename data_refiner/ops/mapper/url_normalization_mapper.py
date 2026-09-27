import posixpath
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema


@processing_operator
class UrlNormalizationMapper(SimpleMapper):
    """Normalize a URL field: lowercase scheme/host, remove default ports, sort query params, drop empty params and fragments, resolve path. 规范化 URL 字段：将协议/主机名转换为小写，移除默认端口，对查询参数进行排序，丢弃片段，解析路径"""

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field` -> Target Source Column 目标源列: The input DataFrame must contain this specified URL column. 输入 DataFrame 必须包含此指定的 URL 列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column must contain raw URL strings to be processed by the normalization routine. 该列必须包含待由规范化程序处理的原始 URL 字符串。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source URL Column 源 URL 列: This parameter defines the name of the input column containing the unnormalized URL strings. 该参数指定包含未规范化 URL 字符串的输入列名称。
* `output_field` -> Normalized URL Column 规范化后 URL 列: This parameter defines the name of the generated column holding the standardized URL strings. 该参数指定存放标准化后 URL 字符串的生成列名称。

### Schema Transformation Process Schema 转换过程
* Phase 1: UDF Evaluation and URL Standardization 阶段 1：UDF 求值与 URL 标准化
  * A Spark User-Defined Function (UDF) is instantiated with a declared return type of `StringType()` to encapsulate the URL standardization logic. 一个显式声明返回类型为 `StringType()` 的 Spark 用户自定义函数 (UDF) 被实例化，用以封装 URL 标准化逻辑。
  * The internal logic performs string manipulations including downcasing the scheme/host, removing default network ports (80/443), sorting query strings while filtering empty parameters, discarding URL fragments, and resolving path trajectories. 内部逻辑执行字符串操作，包括将协议/主机名转换为小写、移除默认网络端口 (80/443)、在过滤空参数的同时对查询字符串进行排序、丢弃 URL 片段以及解析路径轨迹。
* Phase 2: Schema Expansion via Column Append 阶段 2：通过列追加进行 Schema 扩展
  * The Catalyst optimizer evaluates the `.withColumn()` operator to safely append the new field to the active DataFrame schema. Catalyst 优化器求值 `.withColumn()` 算子，将新字段安全地追加到当前 DataFrame schema 中。
  * The column specified by `output_field` is formally registered in the schema metadata as a standard nullable `StringType()` field. 由 `output_field` 指定的列作为标准的、可空的 `StringType()` 字段正式注册到 schema 元数据中。

### Output Schema Final State 输出 Schema 最终态
* Original Columns 原始列: All schema columns present in the initial input DataFrame are fully preserved in their original sequence and data types. 初始输入 DataFrame 中存在的所有 Schema 列都将完整保留其原始顺序和数据类型。
* `output_field` -> Normalized URL Column 规范化后 URL 列: The final generated output column containing the fully transformed and uniform URL text. 最终生成的包含完全转换且统一的 URL 文本的输出列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column holds the structurally normalized, clean URL strings. 该列存放结构规范化、清洗后的 URL 字符串。"""

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:

        def _normalize(url: str) -> str:
            if not url:
                return url
            p = urlparse(url.strip())
            scheme = p.scheme.lower()
            host = p.hostname.lower() if p.hostname else ""
            port = p.port
            if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
                port = None
            netloc = host if port is None else f"{host}:{port}"
            path = posixpath.normpath(p.path) if p.path else "/"
            if p.path.endswith("/") and path != "/":
                path += "/"
            query = urlencode(sorted((k, v) for k, v in parse_qsl(p.query) if v))
            return urlunparse((scheme, netloc, path, p.params, query, ""))

        normalize_udf = F.udf(_normalize, StringType())
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        return df.withColumn(self.output_field, normalize_udf(F.col(self.field)))
