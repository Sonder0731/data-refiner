from pyspark import Row
from pyspark.sql.types import StringType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema


@processing_operator
class NltkTokenizerMapper(SimpleMapper, OperatorConstraint):
    """
    This class is used to tokenize the text data using NLTK library. 使用 NLTK 库对文本数据进行分词
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `field` -> Input Column 输入列: This parameter specifies the source text column that will be segmented into tokens using the NLTK library. 该参数指定将使用 NLTK 库进行分词处理的源文本列。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the extracted token array will be stored. 该参数定义了存储提取出的词标记数组的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to execute partition-level tokenization via the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便通过 `mapPartitions` 算子执行分区级的分词处理。
* Within the `_tokenize` method, an instance of `TreebankWordTokenizer` is initialized per partition, and a new key-value pair represented by `output_field` is dynamically assigned to each row dictionary with the tokenized array. 在 `_tokenize` 方法内部，每个分区初始化一个 `TreebankWordTokenizer` 实例，并向每个行字典中动态分配一个由 `output_field` 表示的新键值对，其值为分词后的数组。
* The mutated RDD structure is re-converted back into a structured DataFrame using the `toDF()` method, mapping the expanded row layout into an updated schema. 改变后的 RDD 结构通过 `toDF()` 方法重新转换为结构化的 DataFrame，将扩展后的行布局映射到更新后的 Schema 中。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `ArrayType(StringType())`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `ArrayType(StringType())`。"""

    def _tokenize(self, partition):
        from nltk.tokenize import TreebankWordTokenizer

        tokenizer = TreebankWordTokenizer()
        for row in partition:
            row_dict = row.asDict()
            row_dict[self.output_field] = (
                tokenizer.tokenize(row_dict[self.field])
                if row_dict[self.field] is not None
                else None
            )
            yield Row(**row_dict)

    @resonance
    def process(self, *args, **kwargs):
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        res_df = df.rdd.mapPartitions(self._tokenize).toDF()
        return res_df
