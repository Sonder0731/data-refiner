from data_refiner.runner import Runner
from data_refiner.utils.tools import get_env_var
from tests.tools import assert_same_by_dict


def test_character_normalization_pipeline(spark):
    runner = Runner.from_yaml(get_env_var("CHARACTER_NORMALIZATION_PIPELINE_PATH"))
    df = runner.run_locally(spark=spark)
    assert [x.asDict() for x in df.collect()] == [{"content": "B乙二牛二(二)IX"}]


def test_chinese_traditional2simple_pipeline(spark):
    runner = Runner.from_yaml(get_env_var("CHINESE_TRADITIONAL2SIMPLE_PIPELINE_PATH"))
    df = runner.run_locally(spark=spark)
    assert [x.asDict() for x in df.collect()] == [{"t2s_content": "在这里输入要转换的内容"}]


def test_html_similarity_detection_pipeline(spark):
    runner = Runner.from_yaml(get_env_var("HTML_STRUCTURE_SIMILARITY_PIPELINE_PATH"))
    df = runner.run_locally(spark=spark)
    rdd = [row["__id__"] for row in df.rdd.collect()]
    assert rdd == [0, 2, 3, 4, 6, 1, 5]


def test_edit_distance_pipeline(spark):
    runner = Runner.from_yaml(get_env_var("EDIT_DISTANCE_PIPELINE_PATH"))
    df = runner.run_locally(spark=spark)
    assert [x.asDict() for x in df.collect()] == [{"edit_distance": 2}]


def test_filter_by_line_regex_pipeline(spark):
    runner = Runner.from_yaml(get_env_var("FILTER_BY_LINE_REGEX_PIPELINE_PATH"))
    df = runner.run_locally(spark=spark)
    assert [x.asDict() for x in df.collect()] == [{"content": "Hello, world!\n你好，世界！"}]


def test_html_content_extraction_pipeline(spark):
    runner = Runner.from_yaml(get_env_var("HTML_CONTENT_EXTRACTION_PIPELINE_PATH"))
    runner.run_locally(spark=spark)


def test_jieba_tokenizer_pipeline(spark):
    runner = Runner.from_yaml(get_env_var("JIEBA_NORMAL_TOKENIZER_PIPELINE_PATH"))
    df = runner.run_locally(spark=spark)
    assert [x.asDict() for x in df.collect()] == [{"tokens": ["今天", "的", "天气", "真", "好"]}]


def test_language_identification_pipeline(spark):
    runner = Runner.from_yaml(get_env_var("LANGUAGE_IDENTIFICATION_PIPELINE_PATH"))
    df = runner.run_locally(spark=spark)
    assert_same_by_dict(
        df,
        [
            {"__id__": 1, "language": "ja"},
            {"__id__": 2, "language": "ja"},
            {"__id__": 3, "language": "ja"},
            {"__id__": 4, "language": "en"},
            {"__id__": 5, "language": "en"},
            {"__id__": 6, "language": "en"},
            {"__id__": 7, "language": "zh"},
            {"__id__": 8, "language": "zh"},
            {"__id__": 9, "language": "zh"},
        ],
    )


def test_mask_pii_pipeline(spark):
    runner = Runner.from_yaml(get_env_var("MASK_PII_PIPELINE_PATH"))
    df = runner.run_locally(spark=spark)
    assert [x.asDict() for x in df.collect()] == [
        {
            "mask_content": "XX的手机号码是<ZH_PHONE_NUMBER>，电话号码是：<ZH_LANDLINE_NUMBER>。我的身份证号是<ZH_ID_CARD>。订单号为123456。"
        },
        {"mask_content": "His name is Mr. Jones and his phone number is [US_PHONE_NUMBER_MASK]"},
        {"mask_content": "This operator refers to <ANONYMIZED>"},
    ]


def test_temp_view(spark):
    runner = Runner.from_yaml(get_env_var("TEST_TEMP_VIEW_PIPELINE_PATH"))
    runner.run_locally(spark=spark)
