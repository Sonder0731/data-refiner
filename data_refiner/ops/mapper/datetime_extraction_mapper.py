import datefinder
from loguru import logger
from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, ArrayType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema


@processing_operator
class DatetimeExtractionMapper(SimpleMapper, OperatorConstraint):
    """
    Extracts all datetime-related substrings from a text string using the datefinder library. 使用 datefinder 库从文本字符串中提取所有与日期时间相关的子字符串
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `field` -> Input Column 输入列: This parameter specifies the source text column from which datetime-related substrings will be extracted. 该参数指定需要从中提取日期时间相关子字符串的源文本列。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the extracted datetime strings will be stored as an array. 该参数定义了存储提取出的日期时间字符串数组的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to perform custom row processing via the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便通过 `mapPartitions` 算子进行自定义的行级处理。
* Within the `_extract_datetimes` partition processing, an empty or populated list of extracted datetime strings is assigned to a new key-value pair defined by `output_field` for each row. 在 `_extract_datetimes` 分区处理内部，一个包含提取出的日期时间字符串的列表（可为空）被赋值给由 `output_field` 定义的每行新键值对。
* An explicit schema object named `output_schema` is programmatically cloned from the original DataFrame schema and updated by appending the `output_field` with an explicit `ArrayType(StringType())` type declaration. 一个名为 `output_schema` 的显式 Schema 对象通过程序从原始 DataFrame Schema 克隆，并通过追加由 `output_field` 指定且类型声明为 `ArrayType(StringType())` 的列进行更新。
* The transformed RDD is re-converted into a DataFrame using the `toDF(output_schema)` operator, enforcing the updated schema layout structure. 转换后的 RDD 使用 `toDF(output_schema)` 算子重新转换为 DataFrame，从而强制应用更新后的 Schema 布局结构。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial ordering and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly structured as `ArrayType(StringType())`. 最终 DataFrame 中追加了一个新列，其数据类型被明确构建为 `ArrayType(StringType())`。"""

    def _extract_datetimes(self, partition):
        """Extract datetime strings from each text in the partition."""
        for row in partition:
            row_dict = row.asDict()
            text = row_dict[self.field]

            datetime_strings = []
            if text is None:
                row_dict[self.output_field] = None
            else:
                matches = datefinder.find_dates(text)
                for match in matches:
                    datetime_strings.append(str(match))
                row_dict[self.output_field] = datetime_strings
            yield Row(**row_dict)

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())

        result_rdd = df.rdd.mapPartitions(self._extract_datetimes)

        output_schema = df.schema
        output_schema = output_schema.add(self.output_field, ArrayType(StringType()))

        result_df = result_rdd.toDF(output_schema)
        return result_df
