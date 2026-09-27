from pydantic import BaseModel, Field
from pyspark import Row, SparkFiles
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, ArrayType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.model_loader.fasttext_model_loader import (
    FastTextModelLoader,
)
from data_refiner.utils.tools import check_params, check_column_schema


@processing_operator
class FastTextModelMapper(SimpleMapper, OperatorConstraint):
    """
    A common fasttext model mapper that takes a text field and applies a fasttext model to it. 一个常用的 fasttext 模型映射器，它接收一个文本字段并为其应用 fasttext 模型
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be either `StringType` or `ArrayType(StringType())`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType` 或 `ArrayType(StringType())`。

### Argument and Column Mapping 参数与列的映射
* `model_path` -> Model File Path 模型文件路径: This parameter specifies the file path of the fasttext model, which is broadcasted to worker nodes to execute predictions on the text data. 该参数指定 fasttext 模型的物理路径，该模型会被广播至工作节点以对文本数据执行预测。
* `label_prefix` -> Label Prefix 标签前缀: This parameter defines the prefix string used by the fasttext model to filter and clean the predicted label outputs. 该参数定义了 fasttext 模型使用的标签前缀字符串，用于过滤和清洗预测输出的标签。
* `field` -> Input Column 输入列: This parameter specifies the source text column that contains the samples to be fed into the fasttext model for prediction. 该参数指定包含要输入至 fasttext 模型进行预测的样本的源文本列。
* `output_field` -> Predicted Label Column 预测标签列: This parameter defines the name of the new column where the predicted label text will be stored. 该参数定义了存储预测标签文本的新列的列名。
* `prob_field` -> Probability Column 概率列: This parameter defines the name of the new column where the probability scores of the predicted labels will be stored. 该参数定义了存储预测标签概率分数的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to perform distributed prediction inside partitions using the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便使用 `mapPartitions` 算子在分区内部执行分布式预测。
* Within each partition, the fasttext model is loaded via `FastTextModelLoader`, and two distinct key-value pairs represented by `output_field` and `prob_field` are dynamically appended to each row dictionary. 在每个分区内部，通过 `FastTextModelLoader` 加载 fasttext 模型，并向每个行字典中动态追加由 `output_field` 和 `prob_field` 表示的两个不同的键值对。
* The transformed RDD is re-converted back into a structured DataFrame using the `toDF()` method, mapping the expanded row dictionaries to an upgraded schema structure. 转换后的 RDD 通过 `toDF()` 方法重新转换为结构化的 DataFrame，将扩展后的行字典映射到升级后的 Schema 结构中。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `StringType`。
* `prob_field`: A secondary new column is appended to the final DataFrame, and its data type is explicitly determined as `DoubleType` or `FloatType` depending on the model's output precision. 最终 DataFrame 中追加了第二个新列，其数据类型根据模型的输出精度被明确确定为 `DoubleType` 或 `FloatType`。"""

    class FastTextModelMapperParams(BaseModel):
        model_path: str = Field(..., description="The path to the fasttext model file.")
        prob_field: str = Field(
            ...,
            description="The name of the field to store the probability of each label.",
        )
        label_prefix: str = Field(
            default="__label__",
            description="The prefix of the label in the fasttext model.",
        )

    config = FastTextModelMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(
            self.config,
            kwargs,
        )
        self.model_path = params.model_path
        self.prob_field = params.prob_field
        self.label_prefix = params.label_prefix

    def _predict(self, partition):
        model = FastTextModelLoader(SparkFiles.get(str(self.model_path)))
        for row in partition:
            row_dict = row.asDict()
            text = row_dict[self.field]
            label, prob = (
                model.predict(text, self.label_prefix) if text is not None else (None, None)
            )
            row_dict[self.output_field] = label
            row_dict[self.prob_field] = prob
            yield Row(**row_dict)

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, [StringType(), ArrayType(StringType())])
        res_df = df.rdd.mapPartitions(self._predict).toDF()
        return res_df
