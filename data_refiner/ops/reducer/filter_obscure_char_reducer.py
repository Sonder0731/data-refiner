from loguru import logger
from pydantic import BaseModel, Field
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import Reducer, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema, check_params


@processing_operator
class FilterObscureCharReducer(Reducer, OperatorConstraint):
    """
    Filter out the characters that appear under a certain cumsum frequency percentage of the data. 筛选出在数据中出现频率低于特定累积频率百分比的字符
    """

    CONSTRAINT = """## Constraint 约束

### Input Column Constraints 输入列约束
* `field` -> Target Text Column 目标文本列: The input DataFrame must contain this specified text column. 输入 DataFrame 必须包含此指定的文本列。
  * Data Type 数据类型: `StringType()` 字符串类型. This column must contain string data whose individual characters will be extracted and analyzed for cumulative frequency. 该列必须包含字符串数据，其单个字符将被提取并进行累积频次分析。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Token Source Column 源词项源列: This parameter defines the name of the input string column used as the raw character source. 该参数指定用作原始字符源的输入字符串列名称。
* `cumsum_rate` -> Threshold Percent Parameter 阈值百分比参数: This configuration parameter determines the cumulative frequency percentage cutoff used to filter obscure characters. 该配置参数决定了用于过滤冷门字符的累积频次百分比截断阈值。

### Schema Transformation Process Schema 转换过程
* Phase 1: Character Extraction and RDD Frequency Grouping 阶段 1：字符提取与 RDD 频次分组
  * The internal engine executes an RDD `.flatMap()` operation to unnest every string in the column specified by `field` into individual character tokens. 内部引擎执行 RDD `.flatMap()` 操作，将 `field` 指定列中的每个字符串展开为独立的字符标记。
  * A subsequent `.map()` and `.reduceByKey()` chain counts occurrences, and the resulting structure is explicitly loaded into a temporary schema footprint via `.toDF(["char", "frequency"])`. 随后的 `.map()` 和 `.reduceByKey()` 链计算出现次数，生成结构通过 `.toDF(["char", "frequency"])` 被显式加载到临时 schema 蓝图中。
* Phase 2: Windowed Accumulation and Mathematical Projection 阶段 2：开窗累加与数学投影
  * A Spark `Window` specification is established, ordering rows by descending frequency from the unbounded beginning to the current row index. 建立一个 Spark `Window` 规范，按频次降序对行进行排序，范围从无边界起始到当前行索引。
  * The first `.withColumn()` operator appends an intermediate aggregation column named `"cumsum"` containing the running total of character occurrences. 第一个 `.withColumn()` 算子追加一个名为 `"cumsum"` 的中间聚合列，包含字符出现次数的运行累计总数。
  * A second `.withColumn()` operator divides the `"cumsum"` column by a broadcasted scalar total frequency, projecting a new evaluation field named `"cumsum_rate"`. 第二个 `.withColumn()` 算子将 `"cumsum"` 列除以广播的标量总频次，投影一个名为 `"cumsum_rate"` 的新评估字段。
* Phase 3: Threshold Filtering 阶段 3：阈值过滤
  * A `.filter()` transformation is applied based on the configured `cumsum_rate` value, selecting only the character records that fall within the specified cumulative boundary. 基于配置的 `cumsum_rate` 值应用 `.filter()` 转换，仅保留符合指定累积边界的字符记录。

### Output Schema Final State 输出 Schema 最终态
* `char` -> Character Key Column 字符键列: The first output field holding the extracted character token. 第一个输出列，存放提取的字符标记。
  * Data Type 数据类型: `StringType()` 字符串类型. This column holds the unique character strings extracted from the input text collection. 该列存放从输入文本集合中提取的唯一字符字符串。
* `frequency` -> Occurrence Count Column 出现次数列: The second output field holding the raw occurrence count. 第二个输出列，存放原始出现次数。
  * Data Type 数据类型: `LongType()` 长整型. This column represents the total absolute count of the corresponding character within the dataset. 该列代表数据集中对应字符的总绝对计数。
* `cumsum` -> Cumulative Sum Column 累积和列: The third output field holding the running aggregate frequency. 第三个输出列，存放运行累计频次。
  * Data Type 数据类型: `LongType()` 长整型. This column tracks the descending cumulative frequency sum calculated down to the current character record. 该列记录按降序计算至当前字符记录的累计频次和。
* `cumsum_rate` -> Cumulative Percentage Column 累积百分比列: The fourth output field holding the calculated ratio score. 第四个输出列，存放计算出的比率得分。
  * Data Type 数据类型: `DoubleType()` 双精度浮点型. This column contains the mathematical ratio representing the character's ranked cumulative position relative to the global frequency pool. 该列包含代表该字符相对于全局频次池的排序累积位置的数学比率。"""

    class FilterObscureCharReducerParams(BaseModel):
        cumsum_rate: float = Field(
            default=0.99,
            description="The cumsum rate of the characters to be filtered out.",
        )

    config = FilterObscureCharReducerParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.cumsum_rate = params.cumsum_rate

    @resonance
    def process(self, *args, **kwargs):
        df = recorder.load(self.input_df)
        # check the input column schema, if the schema is complicated, you do not need to do this.
        check_column_schema(df, self.field, StringType())
        freq_df = (
            df.rdd.flatMap(lambda x: [char for char in x[self.field]])
            .map(lambda x: (x, 1))
            .reduceByKey(lambda x, y: x + y)
            .toDF(["char", "frequency"])
        )
        self.persist_tmps(freq_df)
        total_freq = freq_df.select(F.sum(F.col("frequency")).alias("sum_freq")).collect()[0][
            "sum_freq"
        ]
        logger.info(f"{self.__class__.__name__} get total frequency: {total_freq}")
        window = Window.orderBy(F.desc("frequency")).rowsBetween(Window.unboundedPreceding, 0)
        res_df = (
            freq_df.withColumn("cumsum", F.sum("frequency").over(window))
            .withColumn("cumsum_rate", F.col("cumsum") / F.lit(total_freq))
            .filter(F.col("cumsum_rate") >= self.cumsum_rate)
        )
        return res_df
