from dateutil import parser as dateutil_parser
from dateutil import tz
from loguru import logger
from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, LongType
from pydantic import BaseModel, Field

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema, check_params


@processing_operator
class TimestampMapper(SimpleMapper, OperatorConstraint):
    """
    Converts a datetime string column to a Unix timestamp (seconds since epoch). 将日期时间字符串转换为 Unix 时间戳（自 Unix 纪元以来的秒数）
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field` -> Target Source Column 目标源列: The input DataFrame must contain this specified datetime string column. 输入 DataFrame 必须包含此指定的日期时间字符串列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column must contain standard or custom datetime formatted strings parseable by the dateutil parser. 该列必须包含可被 dateutil 解析器解析的标准或自定义日期时间格式字符串。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Datetime Column 源日期时间列: This parameter defines the name of the input column containing datetime strings to be converted. 该参数指定包含待转换日期时间字符串的输入列名称。
* `output_field` -> Result Timestamp Column 结果时间戳列: This parameter defines the name of the newly generated column holding the converted Unix timestamp. 该参数指定新生成的列名称，用于存放转换后的 Unix 时间戳。
* `tz` -> Timezone Argument 时区参数: This configuration parameter determines the target timezone applied to the parsed datetime object before extracting the epoch timestamp. 该配置参数决定在提取自历元以来的时间戳之前，应用于已解析日期时间对象的目标时区。

### Schema Transformation Process Schema 转换过程
* Phase 1: Partition-Level RDD Transformation 阶段 1：分区级 RDD 转换
  * The transformation executes via an RDD mapPartitions operation, creating an intermediate state where a new dictionary key specified by `output_field` is dynamically injected into each row dictionary. 转换通过 RDD mapPartitions 操作执行，创建一个中间状态，在此状态下，由 `output_field` 指定的新字典键被动态注入到每行字典中。
  * If parsing succeeds, the key maps to an `IntegerType()` representing the Unix timestamp; if parsing fails or the input value is missing, it maps to `None` (Null). 如果解析成功，该键映射为代表 Unix 时间戳的 `IntegerType()`；如果解析失败或输入值缺失，则映射为 `None` (空值)。
* Phase 2: Schema Evolution with explicit Typing 阶段 2：带有显式类型的 Schema 演变
  * The final DataFrame Schema is explicitly derived by invoking the `.add()` method on the original input schema. 最终的 DataFrame Schema 是通过对原始输入 schema 调用 `.add()` 方法显式派生出来的。
  * The column mapping defined by `output_field` is strictly registered as a `LongType()` field within the Spark engine. 由 `output_field` 定义的列映射在 Spark 引擎中被严格注册为 `LongType()` 字段。

### Output Schema Final State 输出 Schema 最终态
* Original Columns 原始列: All schema columns existing in the input DataFrame are retained with their original positions and data types. 输入 DataFrame 中存在的所有 Schema 列都将保留其原始位置和数据类型。
* `output_field` -> Epoch Timestamp Column 历元时间戳列: The final appended output column containing the calculated Unix timestamp. 最终追加的包含计算出的 Unix 时间戳的输出列。
  * Data Type 数据类型: `LongType()` 长整型. This column represents the standard 64-bit integer Unix timestamp denoting seconds elapsed since Jan 01 1970 (UTC). 该列代表标准的 64 位整型 Unix 时间戳，表示自 1970 年 1 月 1 日 (UTC) 以来流逝的秒数。"""

    class TimestampMapperParams(BaseModel):
        tz: str = Field(default="UTC")

    config = TimestampMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.tz = tz.gettz(params.tz)

    def _convert_partition(self, partition):
        for row in partition:
            row_dict = row.asDict()
            value = row_dict.get(self.field)
            ts = None
            if value and isinstance(value, str):
                try:
                    dt = dateutil_parser.parse(value)
                    dt = dt.replace(tzinfo=self.tz)
                    ts = int(dt.timestamp())
                except Exception as e:
                    logger.warning(f"Failed to parse datetime string '{value}': {e}")
            row_dict[self.output_field] = ts
            yield Row(**row_dict)

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df: DataFrame = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())

        result_rdd = df.rdd.mapPartitions(self._convert_partition)
        output_schema = df.schema.add(self.output_field, LongType())
        return result_rdd.toDF(output_schema)
