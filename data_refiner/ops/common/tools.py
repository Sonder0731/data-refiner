from functools import wraps

from loguru import logger
from pyspark.sql import DataFrame

from data_refiner.core.dependency import PERSIST_LEVEL


def output_debug_info(cls, df: DataFrame):
    logger.debug(f"{cls.__name__} debug info:")
    df.show(10, truncate=False)


def output_count_info(cls, df: DataFrame):
    logger.info(f"The count of output of {cls.__name__}: {df.count()}")


def output_cache_info(cls, df: DataFrame, cache_level):
    logger.info(f"Caching {cls.__name__} output to {PERSIST_LEVEL.get(cache_level)}")
    df.persist(PERSIST_LEVEL.get(cache_level))
    if df.is_cached:
        logger.info(f"{cls.__name__} output is cached")


def resonance(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        df = func(self, *args, **kwargs)
        df: DataFrame = df if isinstance(df, DataFrame) else df.toDF()
        if self.select:
            df = df.select(*self.select)
        if self.renames:
            for old_name, new_name in self.renames.items():
                df = df.withColumnRenamed(old_name, new_name)
        if self.drop:
            df = df.drop(*self.drop)
        if self.limit:
            logger.info(f"Limit the output of {self.__class__.__name__} to {self.limit}")
            df = df.limit(self.limit)
        if self.cache:
            if df.is_cached:
                logger.info(f"{func.__name__} output is already cached")
            else:
                logger.info(f"Cache {self.__class__.__name__} output to {self.cache}")
                df.persist(PERSIST_LEVEL.get(self.cache))
        if self.temp_view_name:
            logger.info(
                f"Create temp view {self.temp_view_name} " f"for {self.__class__.__name__} output"
            )
            df.createOrReplaceTempView(self.temp_view_name)
        if self.count:
            logger.info(f"The data count of output of {self.__class__.__name__}: {df.count()}")
        if self.show:
            logger.debug(f"Show {self.__class__.__name__} dataframe")
            df.show(truncate=getattr(self.show, "truncate", True))
        return df

    return wrapper
