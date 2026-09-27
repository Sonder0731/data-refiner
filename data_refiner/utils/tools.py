import importlib.metadata
import os
import tarfile
import shutil
from functools import wraps
from pathlib import Path
from typing import Dict, Iterable, Union, Type, Callable, Any

import pyspark
from loguru import logger
from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from data_refiner.core.dependency import FilterLevel


def check_params(check_class: Type[BaseModel], params: Dict):
    keys = check_class.__annotations__.keys()
    params = {k: v for k, v in params.items() if k in keys}
    data_class = check_class(**params)
    return data_class


def check_column_schema(
    dataframe: DataFrame,
    column_name: str,
    expect_type: Union[Iterable[pyspark.sql.types.DataType], pyspark.sql.types.DataType],
):
    """
    Check the schema of a column in a dataframe as expected.
    Args:
        dataframe: df
        column_name: the column name to check
        expect_type: the expected type of the column, can be a list of types or a single type

    Returns:
        if the schema of the column matches the expected type, nothing will be returned. Otherwise, a ValueError will be raised.
    """
    try:
        column_schema = dataframe.schema[column_name]
        actual_type = column_schema.dataType
        is_type_match = (
            any(actual_type == et for et in expect_type)
            if isinstance(expect_type, list)
            else actual_type == expect_type
        )
        if is_type_match:
            logger.info(f"Check the schema of column '{column_name}' successfully")
        else:
            raise ValueError(
                f"Check the schema of column '{column_name}' failed, The column type is {actual_type}, but expect type is {expect_type}"
            )
    except KeyError:
        raise ValueError(f"Column '{column_name}' is not exist in {dataframe.printSchema()}")


def return_df_by_filter_level(df: DataFrame, tag_field: str, filter_level: str, reverse=False):
    """
    There are three filter levels: TAG, FILTER, and TAG_AND_FILTER.
    - TAG: Tag the boolean value in a new column.
    - FILTER: Filter the rows that the boolean value is True, without any boolean column created.
    - TAG_AND_FILTER: Tag the boolean value in a new column and filter the rows that the boolean value is False.
    """
    if filter_level == FilterLevel.TAG:
        pass
    if filter_level == FilterLevel.FILTER:
        if reverse:
            df = df.filter(~F.col(tag_field)).drop(tag_field)
        else:
            df = df.filter(F.col(tag_field)).drop(tag_field)
    if filter_level == FilterLevel.TAG_AND_FILTER:
        if reverse:
            df = df.filter(~F.col(tag_field))
        else:
            df = df.filter(F.col(tag_field))
    return df


def get_env_var(env_name: str) -> Union[str, Iterable[str]]:
    value = os.environ.get(env_name)
    if value is None:
        # raise ValueError(f"{env_name} is not set")
        return ""
    if value.count(";") > 0:
        return [name.strip() for name in value.split(";")]
    else:
        return value


def get_all_files_recursive(directory_path: str) -> Iterable[Path]:
    """
    Recursively find all files in a directory and its subdirectories.
    :param directory_path: dir path
    :return: list of file paths
    """
    path = Path(directory_path)
    if not path.is_dir():
        return []

    absolute_file_paths = []
    for item in path.rglob("**/*"):
        if item.is_file():
            absolute_file_paths.append(item.resolve())
    return absolute_file_paths


def tar_gz_extractor(file_path: str, target_path: str):
    with tarfile.open(file_path, "r:gz") as tar:
        tar.extractall(target_path)


def ensure_path_exists(_func: Callable[..., Any] = None, *, clean: bool = False):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            path = func(*args, **kwargs)
            if isinstance(path, str):
                path = Path(path)
            if not isinstance(path, Path):
                raise TypeError(f"Expected Path or str, got {type(path)}")

            if clean and path.exists():
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()

            os.makedirs(path, exist_ok=True)
            return path

        return wrapper

    if _func is not None:
        return decorator(_func)

    return decorator


def load_class_from_path(path: Union[str, Path], class_name: str):
    path = Path(path)
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cls = getattr(module, class_name)
    return cls
