from typing import Union

from pydantic import BaseModel, Field, field_validator
from pyspark.sql import DataFrame

from data_refiner.core import recorder
from data_refiner.core.meta_operator import Sampler, processing_operator, OperatorConstraint
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import (
    check_params,
)


@processing_operator
class Sample(Sampler, OperatorConstraint):
    """
    Wrap Spark’s built-in sample function. 封装 Spark 内置的 sample 函数
    """

    CONSTRAINT = """## Constraint 约束

### Input Column Constraints 输入列约束
* The component operates on any generic DataFrame without restricting specific input columns. 该组件在任意通用 DataFrame 上运行，不限制特定的输入列。
* The initial schema of the input DataFrame is fully preserved during the execution. 输入 DataFrame 的初始 Schema 在执行期间被完全保留。

### Argument and Column Mapping 参数与列的映射
* `sample` -> Sampling Control 采样控制: This parameter defines either the exact count or the percentage of rows to be sampled from the input DataFrame. 该参数定义了从输入 DataFrame 中采样的确切行数或百分比。
  * When `sample` is a `float` between 0 and 1 当 `sample` 为 0 到 1 之间的浮点数时: It maps to the direct sampling fraction parameter in the Spark sample operator. 它映射为 Spark 采样算子中直接使用的抽样比例参数。
  * When `sample` is an `int` or a `float` greater than 1 当 `sample` 为整数或大于 1 的浮点数时: It represents the targeted exact row count, which is mapped to a dynamically calculated fraction based on the total row count. 它表示目标确切行数，该行数被映射为一个基于总行数动态计算出的比例。
* `seed` -> Randomization Control 随机化控制: This parameter determines the random seed for the sampling operator to ensure reproducibility. 该参数决定采样算子的随机种子以确保可复现性。

### Schema Transformation Process Schema 转换过程
* Initial State 初始状态: The DataFrame retains its original schema with all existing columns and their respective data types unchanged. DataFrame 保持其原始 Schema，所有现有列及其各自的数据类型均未改变。
* Intermediate Operations 中间操作: The component invokes temporary persistence and row counting operations without introducing any intermediate columns or altering existing data types. 组件调用临时持久化和行数统计操作，不引入任何中间列，也不改变现有的数据类型。
* Type Mutation 类型变更: No column data types are mutated or cast during the processing lifecycle. 在处理生命周期中，没有任何列的数据类型被变更或转换。

### Output Schema Final State 输出 Schema 最终态
* The output DataFrame contains identical columns and identical data types as the input DataFrame. 输出 DataFrame 包含与输入 DataFrame 完全相同的列和完全相同的数据类型。
* The schema structure remains strictly unchanged, while only the total number of rows is reduced according to the sampling logic. Schema 结构严格保持不变，仅总行数根据采样逻辑减少。"""

    class SampleParams(BaseModel):
        sample: Union[int, float] = Field(..., description="Sample size or ratio")
        seed: int = Field(default=777)

        @field_validator("sample")
        def check_sample(cls, v):
            if isinstance(v, float):
                if v > 1 and v != int(v):
                    raise ValueError("If greater than 1, must be an integer.")
                if v < 0:
                    raise ValueError("The sample cannot be negative.")
            return v

        def is_ratio(self) -> bool:
            return 0 < self.sample <= 1

        def is_count(self) -> bool:
            return self.sample > 1

    config = SampleParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: Sample.SampleParams = check_params(
            Sample.SampleParams,
            kwargs,
        )
        self.sample = params.sample
        self.seed = params.seed

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        self.persist_tmps(df, "disk")
        if isinstance(self.sample, int):
            res_df = df.sample(self.sample / df.count(), seed=self.seed)
            return res_df
        res_df = df.sample(self.sample, seed=self.seed)
        return res_df
