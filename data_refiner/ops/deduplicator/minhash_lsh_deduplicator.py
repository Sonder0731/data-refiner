import re
import sys
from itertools import tee
from typing import List, Tuple, Optional, Set, Union

import numpy as np
import numpy.typing as npt
import xxhash
from graphframes import GraphFrame
from loguru import logger
from pydantic import BaseModel, Field
from pyspark import RDD, Row
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, StringType, BooleanType
from scipy.integrate import quad as integrate

from data_refiner.core import recorder
from data_refiner.core.dependency import DeduplicatorMode
from data_refiner.core.dependency import PERSIST_LEVEL
from data_refiner.core.meta_operator import Deduplicator, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_params, check_column_schema


@processing_operator
class MinhashLSHDeduplicator(Deduplicator):
    """
    Deduplicate records using MinHash and Locality Sensitive Hashing (LSH) algorithm. 使用 MinHash 和局部敏感哈希 (LSH) 算法对记录进行去重
    """

    SEED = 19980731
    RNG = np.random.RandomState(SEED)
    NON_ALPHA = re.compile(r"\W", re.UNICODE)
    DTYPE = np.uint32
    MAX_HASH = 4_294_967_295  # maximum 32-bit unsigned integer
    MOD_PRIME = 4_294_967_291  # maximum 32-bit prime number

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* The input DataFrame must contain the specific target column designated by `field`. 输入 DataFrame 必须包含由 `field` 指定的特定目标列。
* The column specified by `field` must be of either `StringType` or `ArrayType(StringType)` data type. 由 `field` 指定的列必须为 `StringType` 或 `ArrayType(StringType)` 数据类型。
* If the configuration parameter `index_field` is specified, a column with that matching name must exist in the input DataFrame. 如果指定了配置参数 `index_field`，则输入 DataFrame 中必须存在具有该匹配名称的列。

### Argument and Column Mapping 参数与列的映射
* `field` -> Text Tokenization Column 文本分词列: This parameter points to the raw text or token array used to generate n-grams and MinHash signatures. 该参数指向用于生成 n-grams 和 MinHash 签名的高维原始文本或标量数组。
* `index_field` -> Primary Identifier Column 主键标识列: This parameter defines the tracking key used to perform graph structural grouping and dataframe joins. 该参数定义了用于执行图结构分组和数据帧连接的追踪主键。
  * When `index_field` is omitted or `None` 当 `index_field` 省略或为 `None` 时: The operator maps and automatically generates an index named `__id__` using a monotonic sequence. 算子映射并使用单调递增序列自动生成名为 `__id__` 的索引。
* `mode` -> Structural Processing Mode 结构化处理模式: This parameter governs how rows are combined and what columns are structural elements in the final dataframe pipeline. 该参数决定了行的合并方式以及最终数据帧流水线中哪些列将作为结构化元素。
  * When `mode` is `DeduplicatorMode.DEDUP` 当 `mode` 为 `DeduplicatorMode.DEDUP` 时: The operator unions the un-clustered records with the priority records resolved within each connected component. 算子将未聚类的记录与在每个连通分量中解析出的优先记录进行联合。
  * When `mode` is `DeduplicatorMode.DUP` 当 `mode` 为 `DeduplicatorMode.DUP` 时: The operator returns exclusively the duplicate records by executing an anti-join mapping. 算子通过执行反连接映射，仅返回重复的记录。
  * When `mode` is `DeduplicatorMode.DEDUP_WITH_DUP` 当 `mode` 为 `DeduplicatorMode.DEDUP_WITH_DUP` 时: The operator appends a boolean retention flag to indicate whether a record is kept or discarded. 算子追加一个布尔类型的保留标志，以指示记录是被保留还是丢弃。

### Schema Transformation Process Schema 转换过程
* Phase 1: Indexing and Component Clustering 阶段 1：索引建制与分量聚类:
  * If the specified tracking column does not exist, an intermediate column (defaulting to `__id__`) of type `LongType` is created via `monotonically_increasing_id()`. 如果指定的追踪列不存在，则通过 `monotonically_increasing_id()` 创建一个类型为 `LongType` 的中间列（默认为 `__id__`）。
  * An intermediate GraphFrame mapping creates a transient column named `component` of type `LongType`, which is then joined back to the primary schema as `__component__`. 一个中间 GraphFrame 映射会创建一个名为 `component` 且类型为 `LongType` 的瞬态列，随后作为 `__component__` 连接回主 Schema。
* Phase 2: Behavioral Branching 阶段 2：行为分支转换:
  * Under `DeduplicatorMode.DEDUP` 在 `DeduplicatorMode.DEDUP` 模式下: The intermediate column `__component__` is consumed during the RDD `reduceByKey` stage and is omitted from the final row unions. 中间列 `__component__` 在 RDD `reduceByKey` 阶段被消耗，并在最终的行联合操作中被排除。
  * Under `DeduplicatorMode.DUP` 在 `DeduplicatorMode.DUP` 模式下: The pipeline performs an anti-join using the tracking key, removing `__component__` automatically and preserving the original schema shape. 流水线使用追踪键执行反连接，自动移除 `__component__` 并保留原始的 Schema 形状。
  * Under `DeduplicatorMode.DEDUP_WITH_DUP` 在 `DeduplicatorMode.DEDUP_WITH_DUP` 模式下: A new structural indicator column named `__stay__` of type `BooleanType` is appended via a left join, and all missing rows are populated via a fallback fill value. 通过左连接追加一个名为 `__stay__` 且类型为 `BooleanType` 的新结构指示列，并且所有缺失行通过兜底填充值进行填补。

### Output Schema Final State 输出 Schema 最终态
* When `mode` is `DeduplicatorMode.DEDUP` or `DeduplicatorMode.DUP` 当 `mode` 为 `DeduplicatorMode.DEDUP` 或 `DeduplicatorMode.DUP` 时:
  * The output DataFrame schema matches the input DataFrame schema exactly, except for the possible inclusion of the generated `__id__` column of type `LongType` if no prior index field was present. 输出 DataFrame Schema 与输入 DataFrame Schema 完全匹配，唯一的例外是：若此前不存在索引字段，则可能包含自动生成的 `LongType` 类型的 `__id__` 列。
* When `mode` is `DeduplicatorMode.DEDUP_WITH_DUP` 当 `mode` 为 `DeduplicatorMode.DEDUP_WITH_DUP` 时:
  * The output schema contains all columns from the input schema (plus the optional `__id__` column of type `LongType`). 输出 Schema 包含输入 Schema 的所有列（加上可选的 `LongType` 类型的 `__id__` 列）。
  * An additional indicator column named `__stay__` of type `BooleanType` is permanently appended to the final schema layout. 一个名为 `__stay__` 且类型为 `BooleanType` 的附加指示列将被永久追加到最终的 Schema 布局中。"""

    class MinhashLSHDeduplicatorParams(BaseModel):
        ev_partitions: int = Field(
            default=10000,
            description="The number of partitions to be used for generating MinHash signatures.",
        )
        threshold: float = Field(
            default=0.7,
            description="The threshold for the Jaccard similarity between two records to be considered as duplicates.",
        )
        ngram_size: int = Field(
            default=5,
            description="The size of the n-grams to be used for generating MinHash signatures.",
        )
        text_min_length: int = Field(
            default=20,
            description="The minimum length of the text to be considered for generating MinHash signatures.",
        )
        num_perm: int = Field(
            default=300,
            description="The number of permutations to be used for generating MinHash signatures.",
        )
        b: Optional[int] = Field(
            default=None, description="The number of bands to be used for LSH."
        )
        r: Optional[int] = Field(default=None, description="The number of rows to be used for LSH.")
        index_field: str = Field(
            default=None, description="The name of the field to be used as the index for LSH."
        )

    config = MinhashLSHDeduplicatorParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.threshold = params.threshold
        self.ngram_size = params.ngram_size
        self.text_min_length = params.text_min_length
        self.num_perm = params.num_perm
        self.b = params.b
        self.r = params.r
        self.index_field = params.index_field
        self.ev_partitions = params.ev_partitions

    @classmethod
    def generate_edges(cls, nodes: List[int]) -> List[Tuple[int, int]]:
        if len(nodes) <= 1:
            return []

        min_node = min(nodes)
        return [(n, min_node) for n in nodes if n != min_node]

    @classmethod
    def ngrams(cls, sequence: List[str], n: int, min_length: int = 5):

        if len(sequence) < min_length:
            return []
        if len(sequence) < n:
            return [tuple(sequence)]
        iterables = tee(iter(sequence), n)
        for i, sub_iterable in enumerate(iterables):
            for _ in range(i):
                next(sub_iterable, None)
        return zip(*iterables)

    @classmethod
    def ngram_hashes(cls, content: Union[List[str], str], n: int, min_length: int = 5) -> Set[int]:
        tokens: List[str] = (
            cls.NON_ALPHA.split(content.lower()) if isinstance(content, str) else content
        )
        ng: set[bytes] = {
            bytes(" ".join(t).lower(), "utf-8") for t in cls.ngrams(tokens, n, min_length)
        }
        return {xxhash.xxh32_intdigest(n) for n in ng}

    @classmethod
    def ngrams_length_check(cls, content: Union[List[str], str], min_length: int = 5) -> bool:
        tokens: List[str] = (
            cls.NON_ALPHA.split(content.lower()) if isinstance(content, str) else content
        )
        return len(tokens) >= min_length

    @classmethod
    def generate_hash_values(
        cls,
        content: str,
        idx: int,
        num_perm: int,
        ngram_size: int,
        min_length: int,
        hashranges: List[Tuple[int, int]],
        permutations: Tuple[npt.NDArray[DTYPE], npt.NDArray[DTYPE]],
    ) -> List[Tuple[int, bytes, int]]:
        a, b = permutations
        hashes = np.array(list(cls.ngram_hashes(content, ngram_size, min_length)), dtype=cls.DTYPE)
        p_hashes = ((np.outer(hashes, a) + b) % cls.MOD_PRIME) & cls.MAX_HASH
        min_hashes = np.vstack([p_hashes, np.full(num_perm, cls.MAX_HASH, dtype=cls.DTYPE)]).min(
            axis=0
        )
        return [
            (band_idx, min_hashes[start:end].data.tobytes(), idx)
            for band_idx, (start, end) in enumerate(hashranges)
        ]

    # endregion

    # region: MinHashLSH
    @classmethod
    def optimal_param(
        cls,
        threshold: float,
        num_perm: int,
        false_positive_weight: float = 0.5,
        false_negative_weight: float = 0.5,
    ):
        def false_positive_area(threshold: float, b: int, r: int):
            """Source: `datasketch.lsh`"""

            def area(s):
                return 1 - (1 - s ** float(r)) ** float(b)

            a, _ = integrate(area, 0.0, threshold)
            return a

        def false_negative_area(threshold: float, b: int, r: int):
            """Source: `datasketch.lsh`"""

            def area(s):
                return 1 - (1 - (1 - s ** float(r)) ** float(b))

            a, _ = integrate(area, threshold, 1.0)
            return a

        min_error = float("inf")
        opt = (0, 0)
        for b in range(1, num_perm + 1):
            max_r = int(num_perm / b)
            for r in range(1, max_r + 1):
                fp = false_positive_area(threshold, b, r)
                fn = false_negative_area(threshold, b, r)
                error = fp * false_positive_weight + fn * false_negative_weight
                if error < min_error:
                    min_error = error
                    opt = (b, r)
        return opt

    def dedup_by_reduce(self, df: DataFrame) -> DataFrame:
        index_field = self.index_field or "__id__"
        if self.mode == DeduplicatorMode.DEDUP:
            pair_rdd = df.rdd.map(lambda row: (row["__component__"], row))
            priority_df = pair_rdd.reduceByKey(self.comparison_function).map(lambda x: x[1]).toDF()
            return priority_df
        elif self.mode == DeduplicatorMode.DUP:
            pair_rdd = (
                df.rdd.map(lambda row: (row["__component__"], row))
                .reduceByKey(self.comparison_function)
                .map(lambda x: Row(**{index_field: x[1][index_field]}))
                .persist(PERSIST_LEVEL.get("disk"))
            )
            priority_df_ids = pair_rdd.toDF()
            dups_df = df.join(priority_df_ids, on=[index_field], how="left_anti")
            if self.cache:
                dups_df.persist(PERSIST_LEVEL.get("disk"))
                pair_rdd.unpersist()
            return dups_df
        elif self.mode == DeduplicatorMode.DEDUP_WITH_DUP:
            pair_rdd = (
                df.rdd.map(lambda row: (row["__component__"], row))
                .reduceByKey(self.comparison_function)
                .map(lambda x: Row(**{index_field: x[1][index_field]}))
                .persist(PERSIST_LEVEL.get("disk"))
            )
            priority_df_ids = pair_rdd.toDF().withColumn("__stay__", F.lit(True))
            dups_df = df.join(priority_df_ids, on=[index_field], how="left")
            dups_df = dups_df.fillna(False, subset=["__stay__"])
            if self.cache:
                dups_df.persist(PERSIST_LEVEL.get("disk"))
                pair_rdd.unpersist()
            return dups_df
        else:
            logger.error(f"Invalid duplication mode: {self.mode}")
            sys.exit(1)

    @resonance
    def process(self, *args, **kwargs):
        spark = kwargs.get("spark")
        #  init params b and r
        B, R = self.b, self.r
        if B is None or R is None:
            B, R = self.__class__.optimal_param(self.threshold, self.num_perm)

        HASH_RANGES: List[Tuple[int, int]] = [(i * R, (i + 1) * R) for i in range(B)]
        PERMUTATIONS: Tuple[
            npt.NDArray[MinhashLSHDeduplicator.DTYPE],
            npt.NDArray[MinhashLSHDeduplicator.DTYPE],
        ] = (
            MinhashLSHDeduplicator.RNG.randint(
                1,
                MinhashLSHDeduplicator.MOD_PRIME,
                size=(self.num_perm,),
                dtype=MinhashLSHDeduplicator.DTYPE,
            ),
            MinhashLSHDeduplicator.RNG.randint(
                0,
                MinhashLSHDeduplicator.MOD_PRIME,
                size=(self.num_perm,),
                dtype=MinhashLSHDeduplicator.DTYPE,
            ),
        )
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, [StringType(), ArrayType(StringType())])
        df = df.filter(
            F.udf(MinhashLSHDeduplicator.ngrams_length_check, BooleanType())(
                F.col(self.field), F.lit(self.text_min_length)
            )
        )
        index_field = self.index_field or "__id__"
        columns = df.columns
        if index_field not in columns:
            df = df.withColumn(index_field, F.monotonically_increasing_id())

        edges: RDD = (
            df.select(index_field, self.field)
            .rdd.flatMap(
                lambda x: MinhashLSHDeduplicator.generate_hash_values(
                    content=x[1],  # column
                    idx=x[0],  # __id__
                    num_perm=self.num_perm,
                    ngram_size=self.ngram_size,
                    min_length=self.text_min_length,
                    hashranges=HASH_RANGES,
                    permutations=PERMUTATIONS,
                )
            )  # (band_idx, band hash value, idx)
            .groupBy(
                lambda x: (x[0], x[1])
            )  # group by (band_idx, band hash value), potential bottleneck, band hash value是每条数据每个band的哈希值
            .flatMap(lambda x: MinhashLSHDeduplicator.generate_edges([ele[2] for ele in x[1]]))
            .distinct()
        ).persist(PERSIST_LEVEL.get("disk"))

        # if no dups
        if edges.isEmpty():
            logger.info("No duplicates found.")
            # df.unpersist()
            edges.unpersist()
            recorder.record(self.output_df, df)
            return df

        edges_df: DataFrame = (
            spark.createDataFrame(edges, schema=["src", "dst"])
            .repartition(self.ev_partitions)
            .persist(PERSIST_LEVEL.get("disk"))
        )
        edges.unpersist()
        vertices_df: DataFrame = (
            edges_df.select(F.col("src").alias("id"))
            .union(edges_df.select(F.col("dst").alias("id")))
            .distinct()
            .repartition(self.ev_partitions)
            .persist(PERSIST_LEVEL.get("disk"))
        )

        assignment: DataFrame = (
            GraphFrame(vertices_df, edges_df)
            .connectedComponents()
            .persist(PERSIST_LEVEL.get("disk"))
        )
        edges_df.unpersist()
        vertices_df.unpersist()

        df = df.join(
            assignment.select(
                F.col("id").alias(index_field),
                F.col("component").alias("__component__"),
            ),
            on=index_field,
            how="left",
        ).persist(PERSIST_LEVEL.get("disk"))
        assignment.unpersist()
        df_without_component = df.filter(F.col("__component__").isNull()).persist(
            PERSIST_LEVEL.get("disk")
        )

        df_with_component = df.filter(F.col("__component__").isNotNull()).persist(
            PERSIST_LEVEL.get("disk")
        )

        df_after_compare = self.dedup_by_reduce(df_with_component)
        if self.mode == DeduplicatorMode.DUP:
            if self.cache:
                df_after_compare.persist(PERSIST_LEVEL.get("disk"))
                df_with_component.unpersist()
                df_without_component.unpersist()
            return df_after_compare
        elif self.mode == DeduplicatorMode.DEDUP:
            df_after_compare.persist(PERSIST_LEVEL.get("disk"))
            df_with_component.unpersist()
            df_res = df_without_component.union(
                df_after_compare.select(*df_without_component.columns)
            )
            if self.cache:
                df_res.persist(PERSIST_LEVEL.get("disk"))
                df_with_component.unpersist()
                df_after_compare.unpersist()
            return df_res
        elif self.mode == DeduplicatorMode.DEDUP_WITH_DUP:
            df_after_compare.persist(PERSIST_LEVEL.get("disk"))
            df_res = df_without_component.withColumn("__stay__", F.lit(True)).union(
                df_after_compare
            )
            if self.cache:
                df_res.persist(PERSIST_LEVEL.get("disk"))
                df_with_component.unpersist()
                df_after_compare.unpersist()
            return df_res
        else:
            logger.error(f"Invalid duplication mode: {self.mode}")
            sys.exit(1)
