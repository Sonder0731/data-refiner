import json

from pathlib import Path
from typing import List, Tuple, Callable, Union,Optional

from pybloom_live import BloomFilter
from pydantic import BaseModel, Field
from pyspark import SparkFiles
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import BooleanType, StringType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import (
    Filter,
    OperatorConstraint,
    OperatorExample,
    processing_operator,
)
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.data_loader.file_loader import TextLoader
from data_refiner.utils.path_set import ClusterPath
from data_refiner.utils.tools import check_column_schema
from data_refiner.utils.tools import (
    check_params,
    return_df_by_filter_level,
)


@processing_operator
class TrashHostFilter(Filter, OperatorConstraint, OperatorExample):
    """
    Filter the text from the trash host. 从过滤来自垃圾域名的文本
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operator directly relies on the specific source URL or host string column specified by `field`. 该算子直接依赖于由 `field` 指定的特定源 URL 或主机名字符串列。
* Required Data Types 要求的数据类型: The column specified by `field` must be of `StringType()`, which is verified by the `check_column_schema` method before any logical evaluation. 由 `field` 指定的列必须为 `StringType()`，在进行任何逻辑评估前由 `check_column_schema` 方法进行验证。

### Argument and Column Mapping 参数与列的映射
* `field` -> Checked Host Column 被检查主机列: This parameter designates the target column containing host values to be checked against the loaded Bloom filter. 该参数指定包含主机名元素的目标列，用于对照加载的布隆过滤器进行检查。
* `trash_host_paths` -> Blacklist Sources 黑名单数据源: This parameter defines the file paths used to dynamically construct the in-memory Bloom filter, directly influencing the evaluation result of `tag_field`. 该参数定义了用于动态构建内存中布隆过滤器的文件路径，直接影响 `tag_field` 的评估结果。
* `tag_field` -> Filter Result Column 过滤结果列: This parameter names the appended intermediate boolean column that stores the matching status of the Bloom filter check. 该参数命名了追加的中间布尔列，用于存储布隆过滤器检查的匹配状态。
  * When the host value in `field` is not found in the Bloom filter blacklist 当 `field` 中的主机名值未在布隆过滤器黑名单中找到时: The row value in `tag_field` evaluates to `True`. `tag_field` 中的行值解析为 `True`。
  * When the host value in `field` exists in the Bloom filter blacklist 当 `field` 中的主机名值存在于布隆过滤器黑名单中时: The row value in `tag_field` evaluates to `False`. `tag_field` 中的行值解析为 `False`。
* `mode` -> Dataset Pruning Strategy 数据集剪枝策略: This parameter dictates the row-filtering action applied within `return_df_by_filter_level` based on the validation states in `tag_field`. 该参数决定了在 `return_df_by_filter_level` 中基于 `tag_field` 中的验证状态所执行的行过滤操作。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: A user-defined function `bloom_filter_udf` returning `BooleanType` is executed via the `withColumn` operator, which appends a single intermediate indicator column named after `tag_field` to the processing schema. 一个返回 `BooleanType` 的用户自定义函数 `bloom_filter_udf` 通过 `withColumn` 算子被执行，向处理 Schema 中追加一个以 `tag_field` 命名的中间指示列。
* Type Modifications 类型改变: The structural layout and data types of preexisting columns within the incoming DataFrame remain fully unmodified. 输入 DataFrame 中原有列的结构布局和数据类型保持完全未修改状态。
* Elimination of Intermediate Columns 中间列的消除: The intermediate `tag_field` column is processed inside `return_df_by_filter_level`, which may drop this temporary operational column prior to returning the final schema state. 中间 `tag_field` 列在 `return_df_by_filter_level` 内部被处理，该函数可能会在返回最终 Schema 状态之前删除此临时业务列。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: The final DataFrame outputs the original structural baseline columns, whereas the intermediate condition column `tag_field` is dynamically decoupled or dropped based on the execution mode of the downstream filtering wrapper. 最终的 DataFrame 输出原始的结构基线列，而中间条件列 `tag_field` 则根据下游过滤外壳的执行模式被动态解耦或删除。
* Final Column Data Types 最终列数据类型: Every baseline column perfectly preserves its incoming operational type (e.g., `field` strictly remains `StringType()`) ensuring structural schema compliance for subsequent processing steps. 每一个基线列都完美地保留了其输入的业务类型（例如，`field` 严格保持为 `StringType()`），确保了后续处理步骤的结构 Schema 合规性。"""

    REFERENCE = """- https://github.com/StevenBlack/hosts
    - https://github.com/AdguardTeam/AdGuardSDNSFilter
    - https://github.com/pi-hole/pi-hole
    - https://firebog.net/
    """.strip()

    class TrashHostFilterParams(BaseModel):
        trash_host_paths: List[Union[str, Path]] = Field(
            default=[
                ClusterPath.data_root().joinpath("KADhosts.txt"),
                ClusterPath.data_root().joinpath("FadeMindhosts.txt"),
            ],
            description="""A list of trash domain files. By default, trash domain records from data-refiner-runtime-resources/data/FadeMindhosts.txt and data-refiner-runtime-resources/data/KADhosts.txt are used. If additional trash domain files are provided, each file must contain an array of strings.
            垃圾域名文件列表。默认使用 data-refiner-runtime-resources/data/FadeMindhosts.txt 和 data-refiner-runtime-resources/data/KADhosts.txt 中的垃圾域名记录。如果传入额外的垃圾域名文件，则每个文件的内容必须为字符串数组。""",
        )
        bloom_error_rate: float = Field(
            default=0.001,
            gt=0,
            lt=1,
            description=(
                "Expected Bloom filter false-positive rate. Smaller values use "
                "more memory. 布隆过滤器预期误判率，值越小占用内存越多。"
            ),
        )
        bloom_capacity: Optional[int] = Field(
            default=None,
            gt=0,
            description=(
                "Optional Bloom filter capacity. Defaults to the number of loaded "
                "trash hosts and must not be smaller than it. 可选容量，默认使用加载的"
                "垃圾域名数量，不能小于实际数量。"
            ),
        )

    config = TrashHostFilterParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.trash_host_paths: TrashHostFilter.TrashHostFilterParams = params.trash_host_paths
        self.bloom_error_rate = params.bloom_error_rate
        self.bloom_capacity = params.bloom_capacity

    def normalization_preprocess(self) -> List[Tuple[str, Callable]]:
        """
        In case of the different trash host files has different formats, this function aims to normalize the final format that can be used.
        :return: A string list, each string is a host name.
        """

        def transform_func_1(text: str):
            """
            For each line, split by the space and take the last element which is the host
            :param text:
            :return:
            """
            return list(
                map(
                    lambda x: x.split(" ")[-1],
                    filter(lambda x: x.startswith("0.0.0.0"), text.split("\n")),
                )
            )

        def transform_func_2(text: str):
            """
            Get those lines which start with "0.0.0.0" and split by the space and take the last element which is the host
            :param text:
            :return:
            """
            lines = map(
                lambda x: x.split(" ")[-1],
                filter(lambda x: x.startswith("0.0.0.0"), text.split("\n")),
            )
            return list(lines)

        res = []
        trash_host_paths = []
        for p in self.trash_host_paths:
            rp = Path(p)
            if rp.is_file():
                trash_host_paths.append(rp)
            else:
                rp = Path(SparkFiles.get(p))
                if rp.is_file():
                    trash_host_paths.append(rp)
                else:
                    raise ValueError(f"Trash host path {p} does not exist")

        for trash_host_path in trash_host_paths:
            if Path(trash_host_path).name in ["FadeMindhosts.txt"]:
                text_loader = TextLoader(trash_host_path)
                res.extend(transform_func_1(text_loader.get_data()))
            elif Path(trash_host_path).name in ["KADhosts.txt"]:
                text_loader = TextLoader(trash_host_path)
                res.extend(transform_func_2(text_loader.get_data()))
            else:
                with open(trash_host_path, "r", encoding="utf-8") as fr:
                    trash_hosts = json.load(fr)
                res+=trash_hosts
        return res

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        trash_hosts = self.normalization_preprocess()
        required_capacity = len(trash_hosts)

        if required_capacity == 0:
            raise ValueError("No trash hosts were loaded")

        capacity = self.bloom_capacity or required_capacity
        if capacity < required_capacity:
            raise ValueError(
                f"bloom_capacity ({capacity}) cannot be smaller than "
                f"the number of loaded trash hosts ({required_capacity})"
            )

        bloom = BloomFilter(
            capacity=capacity,
            error_rate=self.bloom_error_rate,
        )
        for host in trash_hosts:
            bloom.add(host)

        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())

        spark = kwargs.get("spark")
        bloom_bc = spark.sparkContext.broadcast(bloom)

        def bloom_filter(x):
            return not (x in bloom_bc.value)

        bloom_filter_udf = F.udf(bloom_filter, BooleanType())
        df = df.withColumn(self.tag_field, bloom_filter_udf(F.col(self.field)))
        self.persist_tmps(df, "disk")
        res_df = return_df_by_filter_level(
            df,
            self.tag_field,
            self.mode,
        )
        return res_df
