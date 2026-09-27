from typing import Dict, List
from pathlib import Path
import orjson
from pydantic import BaseModel, Field, field_validator
from pyspark import Row, SparkFiles
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from data_refiner.core import recorder
from data_refiner.core.meta_operator import (
    SimpleMapper,
    OperatorExample,
    OperatorConstraint,
    OperatorReference,
    processing_operator,
)
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.path_set import ClusterPath, LocalPath
from data_refiner.utils.tools import check_column_schema, check_params

@processing_operator
class MaskPiiMapper(SimpleMapper, OperatorConstraint, OperatorExample, OperatorReference):
    """
    Mask personal identifiable information (PII) in text. 在文本中屏蔽个人身份信息 (PII)
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `mask_map` -> PII Masking Configurations PII 掩码配置: This parameter specifies a dictionary mapping target entity types to their concrete masking operational parameters, directly controlling how text fragments within the source column are substituted. 该参数指定一个字典，将目标实体类型映射到具体的掩码操作参数，直接控制源列中的文本片段如何被替换。
* `engine_path` -> NLP Model Path NLP 模型路径: This parameter specifies the location of the Spacy NLP model utilized inside each partition executor to tokenize and detect base entities. 该参数指定在每个分区执行器内部使用的 Spacy NLP 模型路径，用于对文本进行分词并检测基础实体。
* `field` -> Input Column 输入列: This parameter specifies the source text column that contains the sensitive samples to be evaluated and anonymized. 该参数指定包含敏感样本以进行评估和脱敏的源文本列。
* `output_field` -> Output Column 输出列: This parameter defines the name of the new column where the fully anonymized and masked text string will be stored. 该参数定义了存储完全脱敏和掩码后文本字符串的新列的列名。

### Schema Transformation Process Schema 转换过程
* The component parses and overrides default masking strategies with user configurations defined in `mask_map` before distributed execution. 组件在分布式执行前，使用 `mask_map` 中定义的客户配置解析并覆盖默认的掩码策略。
* The input DataFrame is converted into a Resilient Distributed Dataset (`RDD`) to facilitate node-level analyzer and anonymizer object initialization using the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便使用 `mapPartitions` 算子促进节点级分析器和脱敏器对象的初始化。
* Inside the partition loop, the `init_analyzer` method registers both default English models and customized Chinese recognizers, appending an updated text sequence to a new key-value pair represented by `output_field` for every row. 在分区循环内部，`init_analyzer` 方法同时注册默认的英文模型和自定义的中文识别器，并将更新后的文本序列追加到由 `output_field` 表示的每行新键值对中。
* The processed RDD structure is re-converted back into a DataFrame representation via the `toDF()` method, embedding the newly generated column alongside the original intact fields. 处理后的 RDD 结构通过 `toDF()` 方法重新转换为 DataFrame 表示，将新生成的列嵌入到保持完好的原始字段旁。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* `output_field`: A new column is appended to the final DataFrame, and its data type is explicitly determined as `StringType`. 最终 DataFrame 中追加了一个新列，其数据类型被明确确定为 `StringType`。"""

    REFERENCE = """https://github.com/microsoft/presidio""".strip()

    ENTITIES = [
        # region: en_core_web default eneities
        "CREDIT_CARD",
        "US_BANK_NUMBER",
        "US_DRIVER_LICENSE",
        "US_ITIN",  # US Individual Taxpayer Identification Number
        "US_PASSPORT",
        "US_SSN",  # US Social Security Number
        "UK_NHS",  # UK National Health Service number
        "CRYPTO",  # wallet/address
        "DATE_TIME",
        "EMAIL_ADDRESS",
        "IBAN_CODE",  # International Bank Account Number
        "IP_ADDRESS",
        "MEDICAL_LICENSE",
        "PHONE_NUMBER",
        "URL",
        "PERSON",
        "DATE_TIME",
        "NRP",
        "LOCATION",
        # endregion
        # region: user defined entities
        "ZH_ID_CARD",
        "ZH_PHONE_NUMBER",
        # endregion
    ]

    class MaskPiiMapperParams(BaseModel):
        mask_map: Dict = Field(
            ...,
            description=(
                "A mapping from entity type to Presidio configuration. Each non-null "
                "value must contain 'type'; parameters are sibling keys, for example "
                "{'type': 'replace', 'new_value': '<PHONE>'}."
            ),
        )
        engine_name: Path = Field(
            default="en_core_web_sm-3.8.0",
            description="The name of the engine model.",
        )

        @field_validator("mask_map")
        @classmethod
        def validate_mask_map(cls, value: Dict) -> Dict:
            for entity, config in value.items():
                if config is None:
                    continue
                try:
                    parsed = orjson.loads(config) if isinstance(config, str) else config
                except orjson.JSONDecodeError as exc:
                    raise ValueError(f"mask_map[{entity!r}] must be valid JSON") from exc
                if not isinstance(parsed, dict) or not parsed.get("type"):
                    raise ValueError(
                        f"mask_map[{entity!r}] must contain a non-empty 'type'; "
                        "mask parameters belong beside 'type'"
                    )
            return value

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(self.config, kwargs)
        self.mask_map = params.mask_map
        self.engine_name = params.engine_name

    def init_analyzer(self):
        from presidio_analyzer import AnalyzerEngine
        from presidio_analyzer.nlp_engine.spacy_nlp_engine import SpacyNlpEngine

        engine_path = LocalPath.model_root().joinpath(self.engine_name)
        path_exits = engine_path.exists()
        if path_exits:
            engine = SpacyNlpEngine(models=[{"lang_code": "en", "model_name": str(engine_path)}])
        else:
            engine_path = ClusterPath.model_root().joinpath(self.engine_name)
            engine = SpacyNlpEngine(
                models=[{"lang_code": "en", "model_name": SparkFiles.get(str(engine_path))}]
            )

        analyzer = AnalyzerEngine(nlp_engine=engine)
        from data_refiner.ops.common.pattern import (
            zh_id_card_recognizer,
            zh_phone_number_recognizer,
            zh_landline_recognizer,
        )

        analyzer.registry.add_recognizer(zh_id_card_recognizer)
        analyzer.registry.add_recognizer(zh_phone_number_recognizer)
        analyzer.registry.add_recognizer(zh_landline_recognizer)
        return analyzer

    config = MaskPiiMapperParams
    __slots__ = list(config.model_fields.keys())

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        from presidio_analyzer import RecognizerResult
        from presidio_anonymizer.entities import OperatorConfig

        def init_operators() -> Dict:
            operators = {}
            operators["DEFAULT"] = OperatorConfig("replace", {"new_value": "<ANONYMIZED>"})
            operators["ZH_ID_CARD"] = OperatorConfig("replace", {"new_value": "<ZH_ID_CARD>"})
            operators["ZH_PHONE_NUMBER"] = OperatorConfig(
                "replace", {"new_value": "<ZH_PHONE_NUMBER>"}
            )
            operators["ZH_LANDLINE_NUMBER"] = OperatorConfig(
                "replace", {"new_value": "<ZH_LANDLINE_NUMBER>"}
            )
            return operators

        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        operators = init_operators()
        # update operators
        for entity, operator_config in self.mask_map.items():
            if not operator_config:
                continue
            if isinstance(operator_config, str):
                operators[entity] = OperatorConfig.from_json(orjson.loads(operator_config))
            else:
                operators[entity] = OperatorConfig.from_json(operator_config)

        def anonymize(analyzer, anonymizer, text):
            analyzer_results: List[RecognizerResult] = analyzer.analyze(
                text=text, entities=list(self.mask_map.keys()), language="en"
            )

            anonymized_results = anonymizer.anonymize(
                text=text,
                analyzer_results=analyzer_results,
                operators=operators,
            )
            return anonymized_results.text

        def partition_anonymize(partition):
            from presidio_anonymizer import AnonymizerEngine

            analyzer = self.init_analyzer()
            anonymizer = AnonymizerEngine()
            for row in partition:
                row_dict = row.asDict()
                text = row_dict[self.field]
                row_dict[self.output_field] = (
                    anonymize(analyzer, anonymizer, text) if text is not None else None
                )
                yield Row(**row_dict)

        output_schema = StructType(
            [
                *df.schema.fields,
                StructField(self.output_field, StringType(), nullable=True),
            ]
        )

        res_df = df.rdd.mapPartitions(partition_anonymize).toDF(output_schema)
        return res_df
