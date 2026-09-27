import os
import shutil
from loguru import logger
from pathlib import Path
from data_refiner.resources.resource_uri import (
    LANGUAGE_IDENTIFIER_MODEL_URL,
    GARBLED_IDENTIFIER_MODEL_URL,
    EN_CORE_WEB_SM_URL,
    EN_CORE_WEB_MD_URL,
    ZH_CORE_WEB_SM_URL,
    ZH_CORE_WEB_MD_URL,
    TRASH_HOST_URL_1,
    TRASH_HOST_URL_2,
    SENSITIVELEXICON_JSON_URL,
    GRAPHFRAMES_0_8_4_SPARK_3_5_S_2_12_JAR_URL,
)
from data_refiner.utils.downloader import Resource, ResourceType, CompressedResource
from data_refiner.utils.path_set import LocalPath

# region: path variable
REPO_ROOT = LocalPath.repo_root()
PROJECT_ROOT = LocalPath.project_root()
RUNTIME_RESOURCES_ROOT = LocalPath.runtime_resources_root()
MODEL_ROOT = LocalPath.model_root()
DATA_ROOT = LocalPath.data_root()
TEMP_ROOT = LocalPath.temp_root()
TEST_ROOT = LocalPath.test_root()
TEST_PIPELINE_CONF_ROOT = LocalPath.test_pipeline_conf_root()

os.environ["REPO_ROOT"] = str(REPO_ROOT)
os.environ["PROJECT_ROOT"] = str(PROJECT_ROOT)
os.environ["RUNTIME_RESOURCES_ROOT"] = str(RUNTIME_RESOURCES_ROOT)
os.environ["MODEL_ROOT"] = str(MODEL_ROOT)
os.environ["DATA_ROOT"] = str(DATA_ROOT)
os.environ["TEMP_ROOT"] = str(TEMP_ROOT)
os.environ["TEST_ROOT"] = str(TEST_ROOT)
os.environ["TEST_PIPELINE_CONF_ROOT"] = str(TEST_PIPELINE_CONF_ROOT)


# endregion


def init_resource_env():
    # region: model paths
    Resource(
        ResourceType.MODEL,
        "GARBLED_IDENTIFIER_PATH",
        resources=[(GARBLED_IDENTIFIER_MODEL_URL, "garbled_text_identifier.bin")],
    )
    Resource(
        ResourceType.MODEL,
        "LANGUAGE_IDENTIFIER_PATH",
        resources=[
            (
                LANGUAGE_IDENTIFIER_MODEL_URL,
                "lid.176.bin",
            )
        ],
    )
    CompressedResource(
        ResourceType.MODEL,
        "EN_CORE_WEB_SM_PATH",
        resources=[
            (
                EN_CORE_WEB_SM_URL,
                "en_core_web_sm-3.8.0/en_core_web_sm/en_core_web_sm-3.8.0",
            )
        ],
    )
    CompressedResource(
        ResourceType.MODEL,
        "EN_CORE_WEB_LG_PATH",
        resources=[
            (
                EN_CORE_WEB_MD_URL,
                "en_core_web_md-3.8.0/en_core_web_md/en_core_web_md-3.8.0",
            )
        ],
    )
    CompressedResource(
        ResourceType.MODEL,
        "ZH_CORE_WEB_SM_PATH",
        resources=[
            (
                ZH_CORE_WEB_SM_URL,
                "zh_core_web_sm-3.8.0/zh_core_web_sm/zh_core_web_sm-3.8.0",
            )
        ],
    )
    CompressedResource(
        ResourceType.MODEL,
        "ZH_CORE_WEB_MD_PATH",
        resources=[
            (
                ZH_CORE_WEB_MD_URL,
                "zh_core_web_md-3.8.0/zh_core_web_md/zh_core_web_md-3.8.0",
            )
        ],
    )
    # endregion

    # region: data paths
    Resource(
        ResourceType.DATA,
        "SENSITIVE_KEYWORDS_PATH",
        resources=[(SENSITIVELEXICON_JSON_URL, "SensitiveLexicon.json")],
    )
    Resource(
        ResourceType.DATA,
        "TRASH_HOST_PATHS",
        resources=[
            (
                TRASH_HOST_URL_1,
                "KADhosts.txt",
            ),
            (
                TRASH_HOST_URL_2,
                "FadeMindhosts.txt",
            ),
        ],
    )
    # endregion

    # region: jars
    Resource(
        ResourceType.JAR,
        "GRAPHFRAMES_0_8_4_SPARK_3_5_S_2_12_JAR_PATH",
        resources=[
            (GRAPHFRAMES_0_8_4_SPARK_3_5_S_2_12_JAR_URL, "graphframes-0.8.4-spark3.5-s_2.12.jar")
        ],
    )
    # endregion


def init_cfg_env():
    from dotenv import load_dotenv
    import nltk

    load_dotenv()
    nltk_data_path = DATA_ROOT.joinpath("nltk_data")
    if not os.path.exists(nltk_data_path):
        os.makedirs(nltk_data_path, exist_ok=True)
    os.environ["SPARK_LOCAL_DIRS"] = str(REPO_ROOT.joinpath("spark_local_dirs"))
    os.environ["NLTK_DATA"] = str(nltk_data_path)
    if nltk_data_path.is_dir() and not any(nltk_data_path.iterdir()):
        nltk.download("popular", download_dir=nltk_data_path)


def zip_dependency():
    base_name = "data-refiner-runtime-resources"
    dependency_path = Path.cwd().joinpath(f"{base_name}.zip")
    dependency_path.unlink(missing_ok=True)
    logger.info("Packing dependency...")
    shutil.make_archive(
        base_name=dependency_path.with_suffix(""),
        format="zip",
        root_dir=LocalPath.runtime_resources_root(),
    )
    logger.info("Pack dependency.zip successfully.")


def init():
    init_cfg_env()
    init_resource_env()
