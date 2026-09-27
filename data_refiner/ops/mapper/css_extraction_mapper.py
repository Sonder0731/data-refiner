from parsel import Selector
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType
from pydantic import BaseModel

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema


def get_css(html):
    if html is None:
        return None
    doc = Selector(text=html)
    classes = set(doc.xpath("//*[@class]/@class").extract())
    atom_classes = set()
    for cls in classes:
        for _cls in cls.split():
            atom_classes.add(_cls)
    atom_classes = list(atom_classes)
    class_dict = {i: 1 for i in atom_classes}
    classes_page = list(class_dict.keys())
    classes_page.sort()
    return classes_page


@processing_operator
class CssFeaturesExtractionMapper(SimpleMapper, OperatorConstraint):
    """
    Extract CSS classes from HTML string. 从 HTML 字符串中提取 CSS
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `field` -> Input Column 输入列: This parameter specifies the source column containing the HTML strings from which CSS classes will be extracted. 该参数指定包含 HTML 字符串的源列，将从该列中提取 CSS 类名。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the extracted and sorted unique CSS classes will be stored. 该参数定义了存储提取并排序后的唯一 CSS 类名列表的新列的列名。

### Schema Transformation Process Schema 转换过程
* A User Defined Function (UDF) named `get_css_udf` is registered, which wraps the `get_css` logic and explicitly declares its return type as `ArrayType(StringType())`. 注册了一个名为 `get_css_udf` 的用户自定义函数（UDF），该函数封装了 `get_css` 逻辑，并明确声明其返回类型为 `ArrayType(StringType())`。
* The `withColumn` operator is applied to the input DataFrame to append a new column defined by `output_field`, while maintaining all existing columns in their original state. 对输入 DataFrame 应用 `withColumn` 算子以逃加由 `output_field` 定义的新列，同时保持所有现有列的原始状态不变。
* No intermediate columns are deleted, and no existing schema properties are modified during this transformation. 在此转换过程中，没有删除任何中间列，也没有修改任何现有的 Schema 属性。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved with their initial data types and schema structures. 输入 DataFrame 中的所有原始列均被保留，并保持其初始数据类型和 Schema 结构。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly set to `ArrayType(StringType())`. 最终 DataFrame 中追加了一个新列，其数据类型被明确指定为 `ArrayType(StringType())`。"""

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        get_css_udf = F.udf(get_css, ArrayType(StringType()))
        res_df = df.withColumn(self.output_field, get_css_udf(F.col(self.field)))
        return res_df
