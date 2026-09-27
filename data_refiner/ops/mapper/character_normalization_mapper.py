import unicodedata
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema


@processing_operator
class CharacterNormalizationMapper(SimpleMapper, OperatorConstraint):
    """
    Normalize the characters in a string. 对字符串中的字符进行规范化
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `field` -> Input Column 输入列: This parameter specifies the source column containing the strings that need to be normalized. 该参数指定包含需要进行归一化处理的字符串的源列。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the normalized strings will be stored. 该参数定义了存储归一化后字符串的新列的列名。

### Schema Transformation Process Schema 转换过程
* During the internal execution, a User Defined Function (UDF) named `nfkc_udf` is registered to perform Unicode NFKC normalization. 在内部执行过程中，注册了一个名为 `nfkc_udf` 的用户自定义函数（UDF）以执行 Unicode NFKC 归一化。
* A new column, specified by `output_field`, is appended to the DataFrame using the `withColumn` operator while preserving all existing columns. 使用 `withColumn` 算子向 DataFrame 追加由 `output_field` 指定的新列，同时保留所有现有列。
* No intermediate columns are deleted, and no existing column data types are altered during this process. 在此过程中，没有删除任何中间列，也没有改变任何现有列的数据类型。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are retained with their initial data types. 输入 DataFrame 中的所有原始列均被保留，并保持其初始数据类型。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly set to `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确指定为 `StringType`。"""

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        nfkc_udf = F.udf(lambda x: unicodedata.normalize("NFKC", x) if x else None, StringType())
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        res_df = df.withColumn(self.output_field, nfkc_udf(F.col(self.field)))
        return res_df
