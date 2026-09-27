from pydantic import BaseModel, Field
from pyspark import Row, SparkFiles
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pathlib import Path

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.model_loader import FastTextModelLoader
from data_refiner.utils.path_set import LocalPath, ClusterPath
from data_refiner.utils.tools import check_params, check_column_schema


@processing_operator
class LanguageIdentificationMapper(SimpleMapper, OperatorConstraint):
    """
    This mapper uses the fasttext model to identify the language of a given text. 使用 fasttext 模型来识别给定文本的语言
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `model_path` -> Model File Path 模型文件路径: This parameter specifies the localization path of the fasttext language identification model used to analyze the text data. 该参数指定用于分析文本数据的 fasttext 语言识别模型的本地化路径。
* `label_prefix` -> Label Prefix 标签前缀: This parameter defines the prefix string used by the fasttext model to isolate and clean the predicted language code. 该参数定义了 fasttext 模型使用的标签前缀字符串，用于隔离和清洗预测的语言代码。
* `field` -> Input Column 输入列: This parameter specifies the source text column whose multiline string content will be stripped of newlines and evaluated for language detection. 该参数指定源文本列，其多行字符串内容将被去除换行符并进行语言检测评估。
* `output_field` -> Predicted Language Column 预测语言列: This parameter defines the name of the new column where the identified language label will be stored. 该参数定义了存储识别出的语言标签的新列的列名。
* `prob_field` -> Probability Column 概率列: This parameter defines the name of the new column where the confidence score of the predicted language will be stored. 该参数定义了存储预测语言置信度分数的新列的列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is transformed into a Resilient Distributed Dataset (`RDD`) to facilitate row-by-row language identification inside partitions via the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便通过 `mapPartitions` 算子在分区内部促进逐行的语言识别。
* Within the partition function, text fields are preprocessed to remove newline characters before being evaluated by the loaded fasttext model. 在分区函数内部，文本字段在由加载的 fasttext 模型评估之前会被预处理以移除换行符。
* Two distinct key-value pairs represented by `output_field` and `prob_field` are dynamically appended to each row dictionary within the iterator. 在迭代器内部，由 `output_field` 和 `prob_field` 表示的两个不同的键值对被动态追加到每个行字典中。
* The mutated RDD structure is re-converted back into a DataFrame representation via the `toDF()` method, upgrading the schema layout to incorporate both new columns. 改变后的 RDD 结构通过 `toDF()` 方法重新转换为 DataFrame 表示，从而升级 Schema 布局以合并这两个新列。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are retained in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `StringType`。
* `prob_field`: A secondary new column is appended to the final DataFrame, and its data type is explicitly determined as `DoubleType` or `FloatType`. 最终 DataFrame 中追加了第二个新列，其数据类型被明确确定为 `DoubleType` 或 `FloatType`。"""

    # Using internal class to define and check the processing operator parameters
    class LanguageIdentificationMapperParams(BaseModel):
        prob_field: str = Field(
            default="language_prob",
            description="The column name for showing probability of language.",
        )
        label_prefix: str = Field(
            default="__label__",
            description="Remove what prefix string in language label, default is __label__",
        )

    config = LanguageIdentificationMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        # initialize the parent class
        super().__init__(*args, **kwargs)
        # check the processing operator parameters
        params = check_params(
            self.config,
            kwargs,
        )
        # initialize the processing operator parameters
        self.prob_field = params.prob_field
        self.label_prefix = params.label_prefix

    # define other methods if needed
    def _predict(self, partition):
        model_path = LocalPath.model_root().joinpath("lid.176.bin")
        print(f"model_path: {model_path}")
        model_path_exist = model_path.exists()
        print(f"model_path exist: {model_path}")

        if model_path_exist:
            model = FastTextModelLoader(model_path)
        else:
            model_path = ClusterPath.model_root().joinpath("lid.176.bin")
            model = FastTextModelLoader(SparkFiles.get(str(model_path)))

        for row in partition:
            row_dict = row.asDict()
            text = row_dict[self.field]
            label, prob = (
                model.predict(text.replace("\n", ""), self.label_prefix)
                if text is not None
                else (None, None)
            )
            row_dict[self.output_field] = label
            row_dict[self.prob_field] = prob
            yield Row(**row_dict)

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        # load the input dataframe
        df = recorder.load(self.input_df)
        # check the input column schema, if the schema is complicated, you do not need to do this.
        check_column_schema(df, self.field, StringType())
        # process the input dataframe
        res_df = df.rdd.mapPartitions(self._predict).toDF()
        return res_df
