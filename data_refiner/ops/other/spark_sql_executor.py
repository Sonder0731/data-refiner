from pydantic import BaseModel, Field
from pyspark.sql import DataFrame
from loguru import logger

from data_refiner.core.meta_operator import (
    Operator,
    OperatorConstraint,
    processing_operator,
)
from data_refiner.ops.common.tools import resonance
from data_refiner.utils.tools import check_params
from data_refiner.core import recorder


@processing_operator
class SparkSqlExecutor(Operator, OperatorConstraint):
    """
    A general-purpose operator that executes any user-provided Spark SQL query on the input DataFrame. 一个通用运算符，用于对输入的 DataFrame 执行任何用户提供的 Spark SQL
    """

    CONSTRAINT = """### Input Column Constraints 输入列约束
      * `Referenced Relations 引用关系`: This operator does not receive or register an input DataFrame. Every table or view referenced by `sql_query` must already exist in
      the provided Spark session. Required columns and data types depend on the SQL statement. 该算子不接收或注册输入 DataFrame。`sql_query` 引用的表或视图必须已存在于传入的
      Spark 会话中，所需字段及其类型由 SQL 语句决定。

      ### Argument and Column Mapping 参数与列的映射
      * `sql_query` -> SQL Statement SQL 语句: The Spark SQL statement to execute. It determines the referenced relations, expressions, filters, aggregations, and output
      columns. 要执行的 Spark SQL 语句，由其决定引用的表或视图、表达式、过滤条件、聚合方式和输出字段。

      ### Schema Transformation Process Schema 转换过程
      * `Relation Resolution 关系解析`: The operator does not create temporary views. Spark resolves tables and views already registered in the supplied session. 算子不会创建
      临时视图；Spark 从传入的会话中解析已注册的表和视图。
      * `SQL Execution SQL 执行`: The operator executes `spark.sql(sql_query)`. Spark parses, analyzes, optimizes, and executes the statement. 算子通过 `spark.sql(sql_query)`
      执行语句，由 Spark 完成解析、分析、优化和执行。

      ### Output Schema Final State 输出 Schema 最终态
      * `Dynamic Output Schema 动态输出 Schema`: For result-producing queries, output columns and data types are determined by the SQL projection and aliases. For other SQL
      statements, the returned DataFrame follows Spark SQL behavior. 对于产生查询结果的语句，输出字段及其类型由 SQL 投影和别名决定；其他 SQL 语句返回的 DataFrame 遵循 Spark
      SQL 的行为。"""

    class SparkSqlExecutorParams(BaseModel):
        sql_query: str = Field(
            ...,
            description=(
                "The Spark SQL statement to execute. Any referenced tables or views must "
                "already exist in the provided Spark session. "
                "要执行的 Spark SQL 语句；引用的表或视图必须已存在于传入的 Spark 会话中。"
            ),
        )
        output_df: str = Field(..., description="The output dataframe name. 输出 DataFrame 的名称")

    config = SparkSqlExecutorParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(
            self.config,
            kwargs,
        )
        self.sql_query = params.sql_query
        self.output_df = params.output_df

    @resonance
    def process(self, *args, **kwargs) -> DataFrame:
        spark = kwargs.get("spark")
        result_df = spark.sql(self.sql_query)
        return result_df

    def run(self, *args, **kwargs):
        try:
            df: DataFrame = self.process(*args, **kwargs)
            recorder.record(self.output_df, df)
            return df
        finally:
            self.unpersist_tmps()
