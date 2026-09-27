from pydantic import BaseModel, Field
from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_params, check_column_schema


@processing_operator
class CharacterRemovalMapper(SimpleMapper, OperatorConstraint):
    """
    Removes specified characters from text samples. 从文本样本中删除指定的字符
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `characters_to_remove` -> Character Removal Set 待删除字符集: This parameter defines a string containing all individual characters that need to be stripped from the target text. 该参数定义了一个字符串，包含所有需要从目标文本中清除的单个字符。
* `field` -> Input Column 输入列: This parameter specifies the target column from which characters will be removed during partition processing. 该参数指定在分区处理过程中需要执行字符删除操作的目标列。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the cleaned text will be stored. 该参数定义了存储清洗后文本的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to apply custom row-by-row transformations using the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便使用 `mapPartitions` 算子应用自定义的逐行转换逻辑。
* Within each partition, a Python translation table is constructed, and a new key-value pair represented by `output_field` is dynamically added to each row dictionary. 在每个分区内部，构建了一个 Python 转换表，并向每个行字典中动态添加了一个由 `output_field` 表示的新键值对。
* The transformed RDD is converted back into a DataFrame using the `toDF()` method, which maps the updated row dictionary structures to a new Schema. 转换后的 RDD 通过 `toDF()` 方法重新转换为 DataFrame，该方法将更新后的行字典结构映射到新的 Schema 中。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are retained in their initial order and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `StringType`。"""

    class CharacterRemovalMapperParams(BaseModel):
        characters_to_remove: str = Field(
            default="",
            description="A string containing all characters to be removed from the input text.",
        )

    config = CharacterRemovalMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(
            self.config,
            kwargs,
        )
        self.characters_to_remove = params.characters_to_remove

    def _remove_characters(self, partition):
        translation_table = str.maketrans("", "", self.characters_to_remove)
        for row in partition:
            row_dict = row.asDict()
            text = row_dict[self.field]
            if text is not None:
                cleaned_text = text.translate(translation_table)
            else:
                cleaned_text = None
            row_dict[self.output_field] = cleaned_text
            yield Row(**row_dict)

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        res_df = df.rdd.mapPartitions(self._remove_characters).toDF()
        return res_df
