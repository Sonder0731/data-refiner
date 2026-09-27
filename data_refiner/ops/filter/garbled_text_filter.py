from pydantic import BaseModel, Field
from pyspark import Row, SparkFiles
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pathlib import Path

from data_refiner.core import recorder
from data_refiner.core.meta_operator import Filter, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.model_loader.fasttext_model_loader import (
    FastTextModelLoader,
)
from data_refiner.utils.path_set import ClusterPath, LocalPath
from data_refiner.utils.tools import (
    check_params,
    check_column_schema,
    return_df_by_filter_level,
)


@processing_operator
class GarbledTextFilter(Filter, OperatorConstraint):
    """
    Identifying garbled text in a given field using a binary classification model trained by fasttext. 使用 fasttext 训练的二元分类模型判断乱码文本
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operation strictly depends on the specific source text column defined by `field`. 该操作严格依赖于由 `field` 指定的特定源文本列。
* Required Data Types 要求的数据类型: The column specified by `field` must be of `StringType()`, which is explicitly verified by the `check_column_schema` validator. 由 `field` 指定的列必须为 `StringType()`，这通过 `check_column_schema` 校验器进行了显式验证。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Text Column 源文本列: This parameter determines the target string column inside the Spark partition rows extracted for fasttext model inference. 该参数决定了在 Spark 分区行中提取并用于 fasttext 模型推理的目标字符串列。
* `tag_field` -> Identification Tag Column 识别标记列: This parameter defines the name of the newly injected boolean column that indicates whether the text is classified as garbled. 该参数定义了新注入的布尔列的名称，用于指示文本是否被分类为乱码。
  * When `grade < threshold` 当 `grade < threshold` 时: The corresponding row in `tag_field` is set to `True`. `tag_field` 中的对应行被设置为 `True`。
  * When `grade >= threshold` 当 `grade >= threshold` 时: The corresponding row in `tag_field` is set to `False`. `tag_field` 中的对应行被设置为 `False`。
* `mode` -> Filtering Execution Mode 过滤执行模式: This parameter dictates how rows are sliced or retained using `return_df_by_filter_level` based on the values in `tag_field`. 该参数决定了如何基于 `tag_field` 中的值，通过 `return_df_by_filter_level` 对行进行切片或保留。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: Within the `mapPartitions` RDD transformation, each row dictionary is modified to include a new boolean key-value pair under the name of `tag_field`, expanding the internal record structure. 在 `mapPartitions` RDD 转换 rows 中，每个行字典都被修改，以在 `tag_field` 的名称下包含一个新的布尔键值对，从而扩展了内部记录结构。
* Type Modifications 类型改变: The RDD is converted back to a DataFrame using `rdd.toDF()`, which implicitly dynamically infers the updated schema, changing the Schema status by appending `tag_field` as a `BooleanType` column. RDD 通过 `rdd.toDF()` 转换回 DataFrame，这隐式地动态推导了更新后的 Schema，通过追加 `tag_field` 作为 `BooleanType` 列改变了 Schema 状态。
* Elimination of Intermediate Columns 中间列的消除: The intermediate `tag_field` column is processed within `return_df_by_filter_level`, and depending on the filter strategy settings, it may be dropped from the final structural presentation. 中间 `tag_field` 列在 `return_df_by_filter_level` 中被处理，并且根据过滤策略设置，它可能会从最终的结构呈现中被删除。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: The final DataFrame outputs the original business columns, while the conditional `tag_field` is decoupled or stripped according to the operational architecture of the filter level utility. 最终的 DataFrame 输出原始业务列，而条件 `tag_field` 则根据过滤级别工具的运行架构被解耦或剥离。
* Final Column Data Types 最终列数据类型: All preexisting columns retain their strict input types (e.g., `field` remains `StringType()`), ensuring structural consistency for subsequent pipeline stages. 所有先前存在的列都保持其严格的输入类型（例如，`field` 保持为 `StringType()`），从而确保后续流水线阶段的结构一致性。"""

    class GarbledTextFilterParams(BaseModel):
        label_prefix: str = Field(
            default="__label__",
            description="The prefix of the label in the model output.",
        )
        threshold: float = Field(
            default=0.5,
            description="The threshold of the model output, if the output is lower than the threshold, the text is considered as garbled.",
        )
        tag_field: str = Field(
            default="is_garbled",
            description="The name of the new field to indicate if the text is garbled or not.",
        )

    config = GarbledTextFilterParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(
            self.config,
            kwargs,
        )
        self.threshold = params.threshold
        self.tag_field = params.tag_field
        self.label_prefix = params.label_prefix

    def is_garbled(self, partition):
        model_path = LocalPath.model_root().joinpath("garbled_text_identifier.bin")
        path_exist = model_path.exists()
        if path_exist:
            model = FastTextModelLoader(model_path)
        else:
            model_path = ClusterPath.model_root().joinpath("garbled_text_identifier.bin")
            model = FastTextModelLoader(SparkFiles.get(str(model_path)))
        for row in partition:
            row_dict = row.asDict()
            text = row_dict[self.field]
            label, grade = model.predict(text.replace("\n", ""), self.label_prefix) if text else ("pos", 1.0)
            if label == "neg":
                grade = 1 - grade
            if grade < self.threshold:
                row_dict[self.tag_field] = True
            else:
                row_dict[self.tag_field] = False
            yield Row(**row_dict)

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        rdd = df.rdd.mapPartitions(self.is_garbled)
        self.persist_tmps(rdd, "disk")
        df = rdd.toDF()
        res_df = return_df_by_filter_level(df, self.tag_field, self.mode, reverse=True)
        return res_df
