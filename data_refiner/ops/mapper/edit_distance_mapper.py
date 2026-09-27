import editdistance
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import (
    MultiInSingleOutMapper,
    OperatorConstraint,
    processing_operator,
)
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema


def calculate_edit_distance(s1, s2):
    return editdistance.eval(s1, s2) if s1 and s2 else None


@processing_operator
class EditDistanceMapper(MultiInSingleOutMapper, OperatorConstraint):
    """
    Calculate the edit distance between two strings. 计算两个字符串之间的编辑距离
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `fields`: The input DataFrame must contain all columns specified within this parameters list, and every individual column's data type must be `StringType`. 输入 DataFrame 必须包含该参数列表中指定的所有列，且每个单独列的数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `fields` -> Input Columns 输入列: This parameter specifies an ordered collection of exactly two source string columns used as arguments to compute the edit distance. 该参数指定了一个包含恰好两个源字符串列的有序集合，用作计算编辑距离的参数。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the calculated edit distance integer values will be stored. 该参数定义了存储计算出的编辑距离整数值的新列的列名。

### Schema Transformation Process Schema 转换过程
* A loop iterates through the collection defined by `fields` to validate that each target column strictly matches the `StringType` requirement. 循环遍历由 `fields` 定义的集合，以验证每个目标列 sampled 是否严格符合 `StringType` 要求。
* A User Defined Function (UDF) named `calc_edit_distance_udf` is registered, which encapsulates the `calculate_edit_distance` evaluation logic and explicitly sets its return type to `IntegerType()`. 注册了一个名为 `calc_edit_distance_udf` 的用户自定义函数（UDF），该函数封装了 `calculate_edit_distance` 评估逻辑，并将其返回类型明确设置为 `IntegerType()`。
* The `withColumn` operator is invoked on the DataFrame, passing the unpacked columns from `fields` into the UDF to append the new column designated by `output_field`. 在 DataFrame 上调用 `withColumn` 算子，将 `fields` 中解包的列传入 UDF 中，以追加由 `output_field` 指定的新列。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly structured as `IntegerType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确构建为 `IntegerType`。"""

    @resonance
    def process(self, *args, **kwargs):
        # load the input dataframe
        df = recorder.load(self.input_df)
        for field in self.fields:
            # check the input column schema, if the schema is complicated, you do not need to do this.
            check_column_schema(df, field, StringType())
        calc_edit_distance_udf = F.udf(
            lambda s1, s2: calculate_edit_distance(s1, s2), IntegerType()
        )
        res_df = df.withColumn(self.output_field, calc_edit_distance_udf(*self.fields))
        return res_df
