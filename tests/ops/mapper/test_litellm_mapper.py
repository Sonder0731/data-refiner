from data_refiner.core import recorder
from data_refiner.ops.mapper.litellm_mapper import LiteLLMMapper
import pytest


@pytest.fixture(scope="class")
def records(spark):
    test_data = [
        ("What are the top fuel-efficient SUVs in 2025",),
        ("How to maintain a hybrid car battery for longer lifespan",),
        ("Compare Tesla Model Y and Ford Mustang Mach-E performance",),
    ]
    df = spark.createDataFrame(test_data, ["query"])
    df.cache()
    recorder.record("test.df", df)


class TestLlmMapper:
    def test_resolves_api_key_from_executor_environment(self, monkeypatch):
        monkeypatch.setenv("LITELLM_API_KEY", "secret")
        mapper = LiteLLMMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="query",
            prompt_template="{query}",
            api_key="${LITELLM_API_KEY}",
            model_name="test-model",
            output_field="answer",
        )

        assert mapper.resolve_api_key() == "secret"

    def test_ask_llm_return_json(self, spark, records):
        llm_mapper = LiteLLMMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="query",
            prompt_template="""My question is: {query}, your answer should be shortly about 10 words.
                Your answer format:
                ```json
                {{"answer":...}}
                ```""",
            api_key="",
            # base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            model_name="dashscope/qwen3-max",
            output_field="answer",
            return_format="json",
            answer_flatten=False,
            drop=["query"],
            show=True,
        )
        df = llm_mapper.process()
        assert len([row for row in df.collect() if row["answer"]]) == 3

    def test_ask_llm_return_text_but_request_json(self, spark, records):
        llm_mapper = LiteLLMMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="query",
            prompt_template="""My question is: {query}, your answer should be shortly about 10 words.""",
            api_key="",
            # base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            model_name="dashscope/qwen3-max",
            output_field="answer",
            return_format="json",
            answer_flatten=True,
            drop=["query"],
            show=True,
        )
        df = llm_mapper.process()
        assert df.count() == 3

    def test_ask_llm_return_text(self, spark, records):
        llm_mapper = LiteLLMMapper(
            input_df="test.df",
            output_df="test.df_output",
            field="query",
            prompt_template="""My question is: {query}, your answer should be shortly about 10 words.""",
            api_key="",
            # base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            model_name="dashscope/qwen3-max",
            output_field="answer",
            return_format="text",
            answer_flatten=True,
            drop=["query"],
            show=True,
        )
        df = llm_mapper.process()
        assert len([row for row in df.collect() if row["answer"]]) == 3
