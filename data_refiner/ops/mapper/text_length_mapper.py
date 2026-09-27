from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pydantic import BaseModel

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.utils.tools import check_column_schema
from data_refiner.ops.common.tools import resonance


@processing_operator
class TextLengthMapper(SimpleMapper, OperatorConstraint):
    """
    Calculate the length of a given text field and add it as a new field. 计算文本长度
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `field` -> Input Column 输入列: This parameter specifies the source text column whose character length needs to be calculated. 该参数指定需要计算字符长度的源文本列。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the calculated character length integers will be stored. 该参数定义了存储计算出的字符长度整数值的新列的列名。

### Schema Transformation Process Schema 转换过程
* The Spark built-in function `F.length` is applied to the column designated by `field` to calculate the character count of each string element. 对由 `field` 指定的列应用 Spark 内置函数 `F.length`，以计算每个字符串元素的字符数。
* The `withColumn` operator is executed on the input DataFrame to append a new column defined by `output_field` without altering or removing any existing columns. 在输入 DataFrame 上执行 `withColumn` 算子以追加由 `output_field` 定义的新列，而不改变或删除任何现有列。
* No intermediate or temporary columns are created or dropped during this execution phase. 在此执行阶段，没有创建或删去任何中间或临时列。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly structured as `IntegerType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确构建为 `IntegerType`。"""

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df: DataFrame = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        res_df = df.withColumn(self.output_field, F.length(self.field))
        return res_df
