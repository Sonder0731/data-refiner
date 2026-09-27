import os

from pyspark import Row
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pydantic import BaseModel, Field
from typing import Literal, Optional

from data_refiner.core import recorder
from data_refiner.core.meta_operator import SimpleMapper, OperatorConstraint, processing_operator
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_column_schema, check_params


@processing_operator
class LiteLLMMapper(SimpleMapper, OperatorConstraint):
    """
    Ask LLm for help and return json object. 请求 LLM API，并返回 JSON 的对象
    """

    CONSTRAINT = """
### Input Column Constraints 输入列约束
* `field`: The input DataFrame must contain this specific column, and its data type must be `StringType`. 输入 DataFrame 必须包含该特定列，且其数据类型必须为 `StringType`。

### Argument and Column Mapping 参数与列的映射
* `prompt_template` -> Prompt Formatting Template 提示词格式化模板: This parameter defines the template string populated by mapping existing row field keys to their corresponding row dictionary values. 该参数定义了通过将现有行字段键映射到其对应的行字典值来填充的模板字符串。
* `return_format` -> Response Structural Format 响应结构格式: This parameter determines whether the Large Language Model (LLM) structural client response returns an unstructured `text` string or a parsed `json` dictionary. 该参数决定了大型语言模型（LLM）结构化客户端响应返回的是非结构化 `text` 字符串还是解析后的 `json` 字典。
* `answer_flatten` -> Dictionary Flattening Toggle 字典展平开关: This parameter dictates whether the parsed dictionary elements should be expanded directly into top-level row properties, which is valid only when `return_format` is set to `json`. 该参数指示是否应将解析后的字典元素直接扩展为顶级行属性，仅在 `return_format` 设置为 `json` 时有效。
* `output_field` -> Output Column 输出列: This parameter defines the target column name where the raw model response string or unflattened dictionary is stored when flattening is bypassed or inapplicable. 该参数定义了当绕过或不适用展平逻辑时，存储原始模型响应字符串或未展平字典的目标列名。

### Schema Transformation Process Schema 转换过程
* The input DataFrame is transformed into a Resilient Distributed Dataset (`RDD`) to handle API invocation state management across cluster workers via the `mapPartitions` operator. 输入 DataFrame 被转换为弹性分布式数据集（`RDD`），以便通过 `mapPartitions` 算子在集群工作节点之间处理 API 调用状态管理。
* Inside the partition loop, each row dictionary dynamically formats the `prompt_template` string using its internal schema fields as parameters. 在分区循环内部，每个行字典使用其内部 Schema 字段作为参数来动态格式化 `prompt_template` 字符串。
* When `answer_flatten` is true, `return_format` is `json`, and the client payload output evaluates to a valid dictionary, the schema shifts dynamically by appending all root-level keys of that JSON object directly into the row dictionary. 当 `answer_flatten` 为真、`return_format` 为 `json` 且客户端负载输出评估为有效字典时，Schema 通过将该 JSON 对象的所有根级键直接追加到行字典中来进行动态转变。
* If the flattening prerequisites are not met, a single column specified by `output_field` is appended to the current row key collection. 如果不满足展平前提条件，则将由 `output_field` 指定的单个列追加到当前行键集合中。
* The mutated RDD is converted back into a DataFrame structural layout via the `toDF()` operator, standardizing the dynamic schema state. 改变后的 RDD 通过 `toDF()` 算子重新转换为 DataFrame 结构化布局，从而标准化动态 Schema 状态。

### Final Output Schema Final State 输出 Schema 最终态
* All original columns from the input DataFrame are preserved in their initial sequence and data types. 输入 DataFrame 中的所有原始列均按其初始顺序和数据类型予以保留。
* Under standard or non-flattened execution (`answer_flatten` is False or `return_format` is `text`): A single column designated by `output_field` is appended to the final DataFrame, carrying a data type of `StringType`. 在标准或未展平的执行下（`answer_flatten` 为 False 或 `return_format` 为 `text`）：最终 DataFrame 中追加了一个由 `output_field` 指定的新列，其数据类型为 `StringType`。
* Under flattened JSON execution (`answer_flatten` is True and `return_format` is `json`): No specific `output_field` is guaranteed; instead, multiple dynamic columns are appended to the final DataFrame corresponding to the root keys of the returned JSON structure, with data types inferred as `StringType`. 在展平 JSON 的执行下（`answer_flatten` 为 True 且 `return_format` 为 `json`）：不保证生成特定的 `output_field`；相反，最终 DataFrame 中会追加多个对应于返回 JSON 结构根键的动态列，其数据类型被推断为 `StringType`。"""

    class LiteLLMMapperParams(BaseModel):
        base_url: str = Field(default=None)
        model_name: str = Field(...)
        api_key: str = Field(
            ...,
            description=(
                "API key or executor environment reference such as "
                "${LITELLM_API_KEY}."
            ),
        )
        temperature: float = Field(default=0.2)
        max_tokens: Optional[int] = Field(default=None)
        max_retries: int = Field(default=3)
        frequency_penalty: float = Field(default=None)

        prompt_template: Optional[str] = Field(..., description="Prompt template for LLM")
        return_format: Literal["json", "text"] = Field(
            default="json", description="Return format of the LLM output"
        )
        answer_flatten: bool = Field(
            default=False,
            description="Whether to flatten the answer, aka whether use a field to contain the whole answer, valid only when return_format=='json'",
        )

    config = LiteLLMMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: LiteLLMMapper.LiteLLMMapperParams = check_params(self.config, kwargs)
        self.base_url = params.base_url
        self.model_name = params.model_name
        self.api_key = params.api_key
        self.temperature = params.temperature
        self.max_tokens = params.max_tokens
        self.max_retries = params.max_retries
        self.frequency_penalty = params.frequency_penalty

        self.prompt_template = params.prompt_template
        self.return_format = params.return_format
        self.answer_flatten = params.answer_flatten

    def request_llm(self, partition):
        from data_refiner.llm.client import LiteLLMClient

        llm_client = LiteLLMClient(
            api_key=self.resolve_api_key(),
            base_url=self.base_url,
            model_name=self.model_name,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            max_retries=self.max_retries,
            frequency_penalty=self.frequency_penalty,
        )

        for row in partition:
            row_dict = row.asDict()
            prompt = self.prompt_template.format(**row_dict)
            answer = llm_client.invoke_once(prompt, return_format=self.return_format)
            if self.answer_flatten and self.return_format == "json" and isinstance(answer, dict):
                row_dict.update(answer)
            else:
                row_dict[self.output_field] = answer
            yield Row(**row_dict)

    def resolve_api_key(self) -> str:
        if self.api_key.startswith("${") and self.api_key.endswith("}"):
            env_name = self.api_key[2:-1]
            value = os.getenv(env_name)
            if not value:
                raise ValueError(f"environment variable {env_name} is not set")
            return value
        return self.api_key

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        df = recorder.load(self.input_df)
        check_column_schema(df, self.field, StringType())
        res_df = df.rdd.mapPartitions(self.request_llm).toDF()
        return res_df
