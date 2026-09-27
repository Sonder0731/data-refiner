from pathlib import Path
from typing import Optional, List, Union

import ahocorasick
import orjson
from pydantic import BaseModel, Field
from pygments.styles import default
from pyspark import SparkFiles
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructType,
    StructField,
    IntegerType,
    ArrayType,
)

from data_refiner.core import recorder
from data_refiner.core.dependency import PERSIST_LEVEL
from data_refiner.core.meta_operator import (
    Filter,
    OperatorConstraint,
    OperatorExample,
    processing_operator,
)
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.path_set import ClusterPath
from data_refiner.utils.tools import (
    check_params,
    check_column_schema,
    return_df_by_filter_level,
)


@processing_operator
class SensitiveDocFilter(Filter, OperatorConstraint, OperatorExample):
    """
    Filter the documents by the sensitive keywords. 按敏感关键词筛选文档
    """

    REFERENCE = """chinese sensitive keywords you could find from here:
        https://github.com/konsheng/Sensitive-lexicon
    """.strip()

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* Dependent Initial Columns 依赖的初始列: The operation depends on the source text column defined by the `field` configuration parameter. 该操作依赖于由 `field` 配置参数定义的源文本列。
* Required Data Types 要求的数据类型: The column specified by `field` must be of type `StringType()`, which is verified by the `check_column_schema` validator before processing. 由 `field` 指定的列必须为 `StringType()` 类型，并在处理前通过 `check_column_schema` 校验器进行验证。

### Argument and Column Mapping 参数与列的映射
* `field` -> Source Text Column 源文本列: This parameter identifies the text column parsed by the Aho-Corasick automaton to search for sensitive keywords. 该参数指定由 Aho-Corasick 自动机解析以搜索敏感词的文本列。
* `targeted_keywords_field` -> Extracted Detail Column 提取详情列: This parameter maps to an optional array column storing structural hit details. 该参数映射到一个可选的数组列，用于存储结构化的命中详情。
  * When `targeted_keywords_field` is specified 当指定 `targeted_keywords_field` 时: An explicit column is created containing tuples of matches (keyword, start index, end index). 将创建一个显式列，其中包含匹配项的三元组（关键字、起始索引、结束索引）。
  * When `targeted_keywords_field` is `None` 当 `targeted_keywords_field` 为 `None` 时: No detailed extraction column is added to the data pipeline. 数据流水线中不会添加任何详细的提取列。
* `keyword_limits` -> Threshold Limiter 阈值限制器: This parameter influences the condition of `tag_field`. If the detected keyword count exceeds this value, the text is flagged for exclusion. 该参数影响 `tag_field` 的条件状态。如果检测到的关键字数量超过此值，该文本将被标记为排除。
* `tag_field` -> Filter Indicator Column 过滤指示列: This parameter specifies the name of the intermediate boolean column determining the validation status. 该参数指定决定验证状态的中间布尔列的名称。
  * When sensitive keywords count <= `keyword_limits` 当敏感词数量 <= `keyword_limits` 时: The row value in `tag_field` is set to `True`. `tag_field` 中的行值被设置为 `True`。
  * When sensitive keywords count > `keyword_limits` 当敏感词数量 > `keyword_limits` 时: The row value in `tag_field` is set to `False`. `tag_field` 中的行值被设置为 `False`。

### Schema Transformation Process Schema 转换过程
* Creation of Intermediate Columns 中间列的创建: The Schema dynamically alters based on the provided parameters. Schema 根据提供的参数进行动态改变。
  * Under conditional targeting 满足指定详情列条件时: Two columns are sequentially generated: an extraction column named after `targeted_keywords_field` with type `ArrayType(StructType([...]))` via `match_sensitive_keyword_udf`, followed by a `BooleanType` column named after `tag_field`. 依次生成两个列：通过 `match_sensitive_keyword_udf` 创建的以 `targeted_keywords_field` 命名且类型为 `ArrayType(StructType([...]))` 的提取列，以及随后创建的以 `tag_field` 命名的 `BooleanType` 列。
  * Under direct filtering 满足直接过滤条件时: Only one intermediate `BooleanType` column named after `tag_field` is appended via `tag_sensitive_doc_udf`. 仅通过 `tag_sensitive_doc_udf` 追加一个以 `tag_field` 命名的中间 `BooleanType` 列。
* Type Modifications 类型改变: Preexisting columns from the input DataFrame are unmodified, and their structural definitions remain intact. 输入 DataFrame 中原有的列未被修改，其结构定义保持原样。
* Elimination of Intermediate Columns 中间列的消除: The intermediate `tag_field` column is processed by the `return_df_by_filter_level` routine, which may filter rows and conditionally drop this tag before delivering the final schema. 中间 `tag_field` 列由 `return_df_by_filter_level` 例程处理，该例程可能会在交付最终 Schema 之前过滤行并条件性地删除此标记。

### Output Schema Final State 输出 Schema 最终态
* Output Columns Final List 输出列最终列表: If `targeted_keywords_field` was specified, it remains preserved within the schema as a permanent metadata column. The intermediate `tag_field` is stripped or retained based on the execution logic of the filter utility. 如果指定了 `targeted_keywords_field`，它将作为永久元数据列保留在 Schema 中。中间的 `tag_field` 则根据过滤工具的执行逻辑被剥离或保留。
* Final Column Data Types 最终列数据类型: Retained baseline columns preserve their incoming types. If stored, the structural field type for `targeted_keywords_field` evaluates exactly as `ArrayType(StructType([StructField("keyword", StringType()), StructField("start_index", IntegerType()), StructField("end_index", IntegerType())]))`. 保留的基线列保持其输入类型。如果被存储，`targeted_keywords_field` 的结构化列类型精确解析为 `ArrayType(StructType([StructField("keyword", StringType()), StructField("start_index", IntegerType()), StructField("end_index", IntegerType())]))`。"""

    class SensitiveDocFilterParams(BaseModel):
        sensitive_keyword_paths: List[Path] = Field(
            default_factory=lambda: [
                ClusterPath.data_root().joinpath("SensitiveLexicon.json")
            ],
            description="""Sensitive word files. By default, sensitive words from data-refiner-runtime-resources/data/SensitiveLexicon.json are used. If additional sensitive word files are provided, each file must contain an array of strings. 敏感词文件。默认使用 data-refiner-runtime-resources/data/SensitiveLexicon.json 中的敏感词汇。如果传入额外的敏感词文件，则每个文件的内容必须为字符串数组""",
        )
        keyword_limits: int = Field(
            default=1,
            description="The maximum number of sensitive keywords in a document. aka <=",
        )
        targeted_keywords_field: Optional[str] = Field(
            default=None, description="The field name of the targeted keywords."
        )

    config = SensitiveDocFilterParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: SensitiveDocFilter.SensitiveDocFilterParams = check_params(self.config, kwargs)
        self.sensitive_keyword_paths = params.sensitive_keyword_paths
        self.keyword_limits = params.keyword_limits
        self.targeted_keywords_field = params.targeted_keywords_field

    def _init_automaton(self):
        sensitive_keyword_list = []
        sensitive_keyword_paths = []
        aca = ahocorasick.Automaton()
        for p in self.sensitive_keyword_paths:
            rp = Path(p)
            if rp.is_file():
                sensitive_keyword_paths.append(rp)
            else:
                rp = Path(SparkFiles.get(p))
                if rp.is_file():
                    sensitive_keyword_paths.append(rp)
                else:
                    raise ValueError(f"Sensitive keywords file {p} is not exist.")

        assert len(self.sensitive_keyword_paths) == len(self.sensitive_keyword_paths)
        for sensitive_keyword_path in sensitive_keyword_paths:
            sensitive_keyword_path = str(sensitive_keyword_path)
            with open(sensitive_keyword_path, "rb") as fr:
                sensitive_keywords = orjson.loads(fr.read())
                if not isinstance(sensitive_keywords, List):
                    raise ValueError(
                        "The sensitive keyword file is not in the correct format. The json file should be in a list of strings."
                    )
            sensitive_keyword_list.extend(sensitive_keywords)
        for keyword in sensitive_keyword_list:
            aca.add_word(keyword, keyword)
        aca.make_automaton()
        return aca

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        ac_automaton = self._init_automaton()
        spark = kwargs.get("spark")
        ac_automaton_bc = spark.sparkContext.broadcast(ac_automaton)

        def match_sensitive_keyword(text):
            if text is None:
                return None
            matches = []
            for end_index, matched_pattern in ac_automaton_bc.value.iter(text):
                start_index = end_index - len(matched_pattern) + 1
                matches.append((matched_pattern, start_index, end_index))
            return matches

        def tag_sensitive_doc(text) -> bool:
            count = 0
            if text is None:
                return False
            for _ in ac_automaton_bc.value.iter(text):
                count += 1
                if count > self.keyword_limits:
                    return False
            return True

        check_column_schema(df, self.field, StringType())
        if self.targeted_keywords_field:
            match_sensitive_keyword_udf = F.udf(
                lambda x: match_sensitive_keyword(x),
                ArrayType(
                    StructType(
                        [
                            StructField("keyword", StringType()),
                            StructField("start_index", IntegerType()),
                            StructField("end_index", IntegerType()),
                        ]
                    )
                ),
            )
            df = df.withColumn(
                self.targeted_keywords_field,
                match_sensitive_keyword_udf(F.col(self.field)),
            )
            df = df.withColumn(
                self.tag_field,
                F.when(
                    F.size(F.col(self.targeted_keywords_field)) > self.keyword_limits,
                    False,
                ).otherwise(True),
            )
        else:
            tag_sensitive_doc_udf = F.udf(lambda x: tag_sensitive_doc(x), BooleanType())
            df = df.withColumn(self.tag_field, tag_sensitive_doc_udf(F.col(self.field)))

        self.persist_tmps(df, "disk")

        res_df = return_df_by_filter_level(
            df,
            self.tag_field,
            self.mode,
        )
        return res_df
