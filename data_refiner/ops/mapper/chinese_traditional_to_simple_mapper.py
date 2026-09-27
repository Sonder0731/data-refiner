from typing import Iterable

from opencc import OpenCC
from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pydantic import BaseModel

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema


@processing_operator
class ChineseTraditional2SimpleMapper(SimpleMapper, OperatorConstraint):
    """
    Convert Chinese Traditional to Simple Chinese. 将繁体中文转换为简体中文
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `field` -> Input Column 输入列: This parameter specifies the target column containing traditional Chinese text to be converted. 该参数指定包含需要被转换的繁体中文文本的目标列。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the converted simplified Chinese text will be stored. 该参数定义了存储转换后的简体中文文本的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to perform partition-level processing via the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便通过 `mapPartitions` 算子进行分区级处理。
* Within the `_t2s` method, an `OpenCC` instance is initialized for each partition, and a new key-value pair represented by `output_field` is added to each row dictionary with the converted text. 在 `_t2s` 方法内部，为每个分区初始化一个 `OpenCC` 实例，并向每个行字典中添加一个由 `output_field` 表示的新键值对，其值为转换后的文本。
* The transformed RDD is reconstructed into a DataFrame using the `toDF()` method, which maps the updated row dictionary structures to a new Schema. 转换后的 RDD 通过 `toDF()` 方法重新构建为 DataFrame，该方法将更新后的行字典结构映射到新的 Schema 中。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `StringType`。"""

    def _t2s(self, partition) -> Iterable[Row]:
        opencc = OpenCC("t2s")
        for line in partition:
            dict_ = line.asDict()
            dict_[self.output_field] = (
                opencc.convert(dict_[self.field]) if dict_[self.field] else None
            )
            yield Row(**dict_)

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        res_df = df.rdd.mapPartitions(self._t2s).toDF()
        return res_df
