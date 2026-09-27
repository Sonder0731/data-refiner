from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pydantic import BaseModel, Field

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema, check_params


@processing_operator
class UnprintableCharRemoveMapper(SimpleMapper, OperatorConstraint):
    """
    Remove unprintable characters from the input text. 从输入文本中移除不可打印字符
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field` -> Target Source Column 目标源列: The input DataFrame must contain this specified source text column. 输入 DataFrame 必须包含此指定的源文本列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column must contain string data that potentially includes unprintable characters requiring cleaning. 该列必须包含可能含有需要清洗的不可打印字符的字符串数据。

### Argument and Column Mapping 参数与列的映射
* `field` -> Raw Text Column 原始文本列: This parameter defines the name of the input column containing the raw string data to be filtered. 该参数指定包含待过滤原始字符串数据的输入列名称。
* `output_field` -> Cleaned Text Column 清洗后文本列: This parameter defines the name of the generated column holding the cleaned string data. 该参数指定存放清洗后字符串数据的生成列名称。
* `exclude_chars` -> Exclusion Characters Argument 排除字符参数: This configuration parameter defines a set of specific characters that must be preserved even if they are non-printable. 该配置参数定义了一组特定字符，即使它们属于不可打印字符也必须予以保留。

### Schema Transformation Process Schema 转换过程
* Phase 1: UDF Registration and Internal Evaluation 阶段 1：UDF 注册与内部求值
  * A Spark User-Defined Function (UDF) is dynamically instantiated with an explicit return type of `StringType()`. 一个显式返回类型为 `StringType()` 的 Spark 用户自定义函数 (UDF) 被动态实例化。
  * The UDF evaluates each character within the column defined by `field`, filtering out characters based on Python's `.isprintable()` logic while exempting characters defined in `exclude_chars`. 该 UDF 对由 `field` 定义的列中的每个字符进行求值，根据 Python 的 `.isprintable()` 逻辑过滤掉字符，同时豁免 `exclude_chars` 中定义的字符。
* Phase 2: Schema Expansion via column addition 阶段 2：通过列追加进行 Schema 扩展
  * The catalyst optimizer processes the `.withColumn()` operator to append the new field to the DataFrame schema. Catalyst 优化器处理 `.withColumn()` 算子，将新字段追加到 DataFrame schema 中。
  * The column specified by `output_field` is explicitly integrated into the schema definition as a `StringType()` column. 由 `output_field` 指定的列作为 `StringType()` 列被显式整合到 schema 定义中。

### Output Schema Final State 输出 Schema 最终态
* Original Columns 原始列: All schema columns existing in the input DataFrame are retained with their original positions, schemas, and data types. 输入 DataFrame 中存在的所有 Schema 列都将保留其原始位置、结构和数据类型。
* `output_field` -> Cleaned Text Column 清洗后文本列: The final generated output column containing the processed text. 最终生成的包含已处理文本的输出列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column represents the final sanitized string where unprintable characters have been stripped out. 该列代表已去除不可打印字符的最终净化后的字符串。"""

    class UnprintableCharRemoveMapperParams(BaseModel):
        exclude_chars: str = Field(default="\n", description="Characters to exclude from removal.")

    config = UnprintableCharRemoveMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.exclude_chars = params.exclude_chars

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        exclude_chars = self.exclude_chars

        def clean(text):
            if text is None:
                return None
            return "".join(chr for chr in text if chr.isprintable() or chr in exclude_chars)

        clean_udf = F.udf(clean, StringType())
        res_df = df.withColumn(self.output_field, clean_udf(F.col(self.field)))
        return res_df
