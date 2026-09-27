import jieba
from pydantic import BaseModel, Field
from pyspark import Row
from pyspark.sql.types import StringType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema, check_params
import unicodedata


def is_punctuation(text):
    return any(unicodedata.category(char).startswith('P') for char in text)


@processing_operator
class JiebaNormalTokenizerMapper(SimpleMapper, OperatorConstraint):
    """
    Tokenize the input text using jieba library. 使用 jieba 库对输入文本进行分词
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `punctuation_filter` -> Punctuation Filter Toggle 标点符号过滤开关: This parameter determines whether punctuation tokens should be removed from the generated token list within the partition processor. 该参数决定了在分区处理器内部是否应从生成的词标记列表中移除标点符号标记。
* `field` -> Input Column 输入列: This parameter specifies the source text column that will be segmented into tokens using the jieba library. 该参数指定将使用 jieba 库进行分词处理的源文本列。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the extracted token array will be stored. 该参数定义了存储提取出的词标记数组的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to implement custom tokenization logic inside partitions via the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便通过 `mapPartitions` 算子在分区内部实现自定义的分词逻辑。
* Within the `_tokenize` iterator, conditional logic evaluates `punctuation_filter` to filter out punctuation matching Unicode category 'P', and a new key-value pair defined by `output_field` is dynamically added to each row dictionary. 在 `_tokenize` 迭代器内部，条件逻辑评估 `punctuation_filter` 以过滤掉匹配 Unicode 属性为 'P' 的标点符号，并向每个行字典中动态添加一个由 `output_field` 定义的新键值对。
* The modified RDD is transformed back into a DataFrame format using the `toDF()` method, mapping the expanded row layout into an updated schema. 修改后的 RDD 通过 `toDF()` 方法重新转换为 DataFrame 格式，将扩展后的行布局映射到更新后的 Schema 中。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `ArrayType(StringType())`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `ArrayType(StringType())`。"""

    class JiebaNormalTokenizerMapperParams(BaseModel):
        punctuation_filter: bool = Field(default=True)

    config = JiebaNormalTokenizerMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.punctuation_filter = params.punctuation_filter

    def _tokenize(self, partition):
        for row in partition:
            row_dict = row.asDict()
            tokens = None
            if row_dict[self.field]:
                tokens = jieba.lcut(row_dict[self.field])
                if self.punctuation_filter:
                    tokens = [token for token in tokens if not is_punctuation(token)]
            row_dict[self.output_field] = tokens
            yield Row(**row_dict)

    @resonance
    def process(self, *args, **kwargs):
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        res_df = df.rdd.mapPartitions(self._tokenize).toDF()
        return res_df
