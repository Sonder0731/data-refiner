from typing import Optional, List, Union, Literal, Dict, ClassVar

from loguru import logger
from pydantic import BaseModel, Field, model_validator
from pyspark import RDD
from pyspark.sql import DataFrame

from data_refiner.core import recorder
from data_refiner.core.dependency import FilterLevel, DeduplicatorMode, PERSIST_LEVEL
from data_refiner.utils.tools import check_params


def meta_operator(cls):
    cls.__operator_type__ = "META"
    return cls


def processing_operator(cls):
    cls.__operator_type__ = "PROCESSING"
    return cls


# region: operator contract class
class OperatorConstraint:
    """
    Operator metadata `notes` class for processing operator.
    """

    REQUIRED_OVERRIDES = ("CONSTRAINT",)
    __slots__ = ()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for name in cls.REQUIRED_OVERRIDES:
            if name not in cls.__dict__:
                raise TypeError(f"{cls.__name__} must override `{name}`")


class OperatorReference:
    """
    Operator metadata `notes` class for processing operator.
    """

    REQUIRED_OVERRIDES = ("REFERENCE",)
    __slots__ = ()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for name in cls.REQUIRED_OVERRIDES:
            if name not in cls.__dict__:
                raise TypeError(f"{cls.__name__} must override `{name}`")


class OperatorExample:
    """
    Operator metadata `example` class for processing operator.
    """

    REQUIRED_OVERRIDES = ("EXAMPLE",)
    __slots__ = ()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for name in cls.REQUIRED_OVERRIDES:
            if name not in cls.__dict__:
                raise TypeError(f"{cls.__name__} must override `{name}`")

    # endregion
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for name in cls.REQUIRED_OVERRIDES:
            if name not in cls.__dict__:
                raise TypeError(f"{cls.__name__} must override `{name}`")


# endregion


class ShowOptions(BaseModel):
    truncate: bool = Field(
        default=True,
        description="Whether to truncate displayed values. 是否截断展示内容",
    )


@meta_operator
class Operator:
    """
    The super meta operator for all operators. 所有算子类的父类
    """

    class OperatorParams(BaseModel):
        show: bool | ShowOptions = Field(
            default=False,
            description=(
                'Controls DataFrame display. JSON examples: {"show": false} disables display; '
                '{"show": true} or {"show": {"truncate": true}} displays truncated values; '
                '{"show": {"truncate": false}} displays full values. '
                '控制 DataFrame 展示。JSON 配置：{"show": false} 表示不展示；'
                '{"show": true} 或 {"show": {"truncate": true}} 表示展示并截断过长内容；'
                '{"show": {"truncate": false}} 表示展示完整内容。'
            ),
            json_schema_extra={
                "doc_type": 'boolean or object {"truncate": boolean}',
            },
        )
        cache: Literal["disk", "memory", "memory_disk"] = Field(
            default="disk",
            description="Cache the output dataframe after operator processing. Options are `disk`, `memory` or `memory_disk`. 算子处理完成后缓存输出 DataFrame。缓存方式可选：`disk`、`memory` 或 `memory_disk`",
            json_schema_extra={
                "options": {
                    "disk": "disk cache 仅磁盘缓存",
                    "memory": "memory cache 仅内存缓存",
                    "memory_disk": "memory and disk cache 内存和磁盘缓存",
                }
            },
        )
        partitions: Optional[int] = Field(
            default=None,
            description="Repartition the number of partitions of input dataframe. 重新分区输入 DataFrame 的分区数量",
        )
        count: bool = Field(
            default=False,
            description="Whether to count the number of rows of output dataframe. 是否统计输出 DataFrame 的行数",
            json_schema_extra={
                "options": {
                    "True": "count rows 统计行数",
                    "False": "not count rows 不统计行数",
                }
            },
        )
        limit: Optional[int] = Field(
            default=None,
            description="Limit the number of rows of output dataframe. 限制当前算子处理完后的输出 DataFrame 的行数",
        )
        drop: Optional[List[str]] = Field(
            default=None,
            description="Drop the specified columns in output dataframe. 删除输出 DataFrame 中指定的列",
        )
        select: Optional[List[str]] = Field(
            default=None,
            description="Select the specified columns in output dataframe. 选择输出 DataFrame 中指定的列",
        )
        renames: Optional[Dict[str, str]] = Field(
            default=None, description="Rename the specified columns. 重命名指定的列"
        )

        temp_view_name: Optional[str] = Field(
            default=None,
            description="Optional. Specifies the name used to register the operator's output DataFrame as a Spark SQL temporary view. When set, the operator calls createOrReplaceTempView(view_name) after execution, making the result available to downstream SQL operators. If not set, no temporary view is created. 可选。指定当前算子输出 DataFrame 注册为 Spark SQL 临时视图时使用的名称。设置后，算子执行完成会调用 createOrReplaceTempView(view_name) 创建或替换同名临时视图，供后续 SQL 算子引用；未设置时不创建临时视图。",
        )

        @model_validator(mode="after")
        def check_drop_and_select_exclusivity(self) -> 'OperatorParams':
            if self.drop is not None and self.select is not None:
                raise ValueError(
                    "Cannot specify both 'drop' and 'select'. Please use only one of them."
                )
            return self

    config = OperatorParams
    __slots__ = list(config.model_fields.keys()) + ["tmp_dfs"]

    def __init__(self, *args, **kwargs):
        params: Operator.OperatorParams = check_params(
            Operator.OperatorParams,
            kwargs,
        )
        self.partitions = params.partitions
        self.show = params.show
        self.cache = params.cache
        self.count = params.count
        self.limit = params.limit
        self.drop = params.drop
        self.select = params.select
        self.renames = params.renames
        self.temp_view_name = params.temp_view_name
        self.tmp_dfs = []

    def unpersist_tmps(self):
        """
        Clear the cache of the internal processing operator
        """
        for df in self.tmp_dfs:
            logger.info("Unpersist temporary dataframe")
            df.unpersist()

    def persist_tmps(self, element: Union[DataFrame, RDD], persist_level: str = "disk"):
        element.persist(PERSIST_LEVEL[persist_level])
        self.tmp_dfs.append(element)

    def process(self, *args, **kwargs):
        raise NotImplementedError("Process method not implemented")

    def run(self, *args, **kwargs):
        raise NotImplementedError("Run method not implemented")


@meta_operator
class InputOutputOperator(Operator):
    """
    The meta operator for operators that need an input DataFrame and produce an output DataFrame.
    """

    class InputOutputOperatorParams(BaseModel):
        input_df: str = Field(..., description="The input dataframe name. 输入 DataFrame 的名称")
        output_df: str = Field(..., description="The output dataframe name. 输出 DataFrame 的名称")

    config = InputOutputOperatorParams
    __slots__ = list(config.model_fields.keys()) + ["tmp_dfs"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: InputOutputOperator.InputOutputOperatorParams = check_params(
            InputOutputOperator.InputOutputOperatorParams, kwargs
        )
        self.input_df = params.input_df
        self.output_df = params.output_df

    def run(self, *args, **kwargs):
        try:
            df: DataFrame = self.process(*args, **kwargs)
            recorder.record(self.output_df, df)
            return df
        finally:
            self.unpersist_tmps()


# region: Reader
@meta_operator
class Reader(Operator):
    """
    The meta operator for operators that read external data into a Spark DataFrame.
    """

    class ReaderParams(BaseModel):
        output_df: str = Field(..., description="The output dataframe name. 输出 DataFrame 的名称")

    config = ReaderParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: Reader.ReaderParams = check_params(
            Reader.ReaderParams,
            kwargs,
        )
        self.output_df = params.output_df

    def run(self, *args, **kwargs):
        try:
            df: DataFrame = self.process(*args, **kwargs)
            recorder.record(self.output_df, df)
            return df
        finally:
            self.unpersist_tmps()


@meta_operator
class PathReader(Reader):
    """
    The meta operator for reader operators that load data from a filesystem path.
    """

    class PathReaderParams(BaseModel):
        input_path: str = Field(..., description="The input data location path. 输入数据路径")

    config = PathReaderParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: PathReader.PathReaderParams = check_params(
            PathReader.PathReaderParams,
            kwargs,
        )
        self.input_path = params.input_path


@meta_operator
class TableReader(Reader):
    """
    The meta operator for reader operators that load data from a table name.
    """

    class TableReaderParams(BaseModel):
        table_name: str = Field(..., description="The input table name. 输入表名")

    config = TableReaderParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: TableReader.TableReaderParams = check_params(
            TableReader.TableReaderParams,
            kwargs,
        )
        self.table_name = params.table_name


# endregion
@meta_operator
class Writer(Operator):
    """
    The meta operator for operators that write a Spark DataFrame to an external target.
    """

    class WriterParams(BaseModel):
        input_df: str = Field(..., description="The input dataframe name. 输入 DataFrame 的名称")
        output_df: Optional[str] = Field(
            default=None, description="The output dataframe name. 输出 DataFrame 的名称"
        )

    config = WriterParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: Writer.WriterParams = check_params(
            Writer.WriterParams,
            kwargs,
        )
        self.input_df = params.input_df
        self.output_df = params.output_df

    def run(self, *args, **kwargs):
        try:
            df: DataFrame = self.process(*args, **kwargs)
            if self.output_df:
                recorder.record(self.output_df, df)
            return df
        finally:
            self.unpersist_tmps()


@meta_operator
class Reducer(InputOutputOperator):
    """
    The meta operator for operators that aggregate or summarize one field into a reduced output DataFrame.
    """

    class ReducerParams(BaseModel):
        field: str = Field(
            ..., description="The field name used for reduction. 用于归并计算的字段名"
        )

    config = ReducerParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: Reducer.ReducerParams = check_params(
            Reducer.ReducerParams,
            kwargs,
        )
        self.field = params.field


# region: Mapper
@meta_operator
class SimpleMapper(InputOutputOperator):
    """
    The meta operator for simple transformation (single input and single output).
    """

    class SimpleMapperParams(BaseModel):
        field: str = Field(..., description="The field name to map on. 要进行映射的字段名")
        output_field: str = Field(
            ..., description="The field name to store the mapped value. 用于存储映射结果的字段名"
        )

    config = SimpleMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: SimpleMapper.SimpleMapperParams = check_params(
            SimpleMapper.SimpleMapperParams,
            kwargs,
        )

        self.field = params.field
        self.output_field = params.output_field


@meta_operator
class MultiInSingleOutMapper(InputOutputOperator):
    """
    The meta operator for multiple input and single output mapping.
    """

    class MultiInSingleOutMapperParams(BaseModel):
        fields: List[str] = Field(
            ..., description="The list of field names to map on. 要进行映射的字段名列表"
        )
        output_field: str = Field(
            ..., description="The output field name after transforming. 转换后的输出字段名"
        )

    config = MultiInSingleOutMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: MultiInSingleOutMapper.MultiInSingleOutMapperParams = check_params(
            MultiInSingleOutMapper.MultiInSingleOutMapperParams,
            kwargs,
        )
        self.fields = params.fields
        self.output_field = params.output_field


@meta_operator
class SingleInMultiOutMapper(InputOutputOperator):
    """
    The meta operator for single input and multiple output mapping.
    """

    class SingleInMultiOutMapperParams(BaseModel):
        field: str = Field(..., description="The field name to map on. 要进行映射的字段名")

    config = SingleInMultiOutMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: SingleInMultiOutMapper.SingleInMultiOutMapperParams = check_params(
            SingleInMultiOutMapper.SingleInMultiOutMapperParams,
            kwargs,
        )
        self.field = params.field


@meta_operator
class MultiInMultiOutMapper(InputOutputOperator):
    """
    The meta operator for multiple input and multiple output mapping.
    """

    class MultiInMultiOutMapperParams(BaseModel):
        fields: List[str] = Field(
            ..., description="The list of field names to map on. 要进行映射的字段名列表"
        )
        output_fields: List[str] = Field(
            ...,
            description="The list of output field names after transforming. 转换后的输出字段名列表",
        )

    config = MultiInMultiOutMapperParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: MultiInMultiOutMapper.MultiInMultiOutMapperParams = check_params(
            MultiInMultiOutMapper.MultiInMultiOutMapperParams,
            kwargs,
        )
        self.fields = params.fields
        self.output_fields = params.output_fields


# endregion


# region: filter
@meta_operator
class Filter(InputOutputOperator):
    """
    The meta operator for all filter processing operators.
    """

    class FilterParams(BaseModel):
        field: str = Field(
            ...,
            description="The field name (column name in dataframe) to process. This parameter may have special meaning in some operators; see the specific operator document. 要处理的字段名（DataFrame 列名）；在部分算子中该参数可能有特殊含义，请参考具体算子文档",
        )
        tag_field: str = Field(
            default="tag", description="The tag field name (column name). 标签字段名（列名）"
        )
        mode: str = Field(
            default=FilterLevel.FILTER,
            description="The filter mode. 过滤模式",
            json_schema_extra={
                "options": {
                    "tag": "tag only 仅打标",
                    "filter": "filter rows 过滤数据行",
                    "tag_and_filter": "tag and filter 打标并过滤",
                }
            },
        )

    config = FilterParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params: Filter.FilterParams = check_params(
            Filter.FilterParams,
            kwargs,
        )
        self.field: str = params.field
        self.tag_field: str = params.tag_field
        self.mode: str = params.mode


# endregion


# region: duplicator
@meta_operator
class Deduplicator(InputOutputOperator):
    """
    The meta operator for all deduplication processing operators.
    """

    class DeduplicatorParams(BaseModel):
        field: str = Field(
            ...,
            description="The field name (column name in dataframe) to deduplicate. 要去重的字段名（DataFrame 列名）",
        )
        mode: str = Field(
            default=DeduplicatorMode.DEDUP,
            description="The mode of deduplication. 去重模式",
            json_schema_extra={
                "options": {
                    "dedup": "remove duplicates 去重",
                    "dup": "keep duplicates only 仅保留重复数据",
                    "dedup_with_dup": "deduplicate and keep duplicates separately 去重并单独保留重复数据",
                }
            },
        )
        # comparison_field: Optional[str] = Field(default=None)
        comparison_function: Optional[str] = Field(
            default=None,
            description="A comparison function used to resolve duplicates by deciding which record to keep. 用于在重复数据之间决定保留哪条记录的比较函数",
        )

    config = DeduplicatorParams
    __slots__ = list(config.model_fields.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        params = check_params(Deduplicator.DeduplicatorParams, kwargs)
        self.field = params.field
        self.mode = params.mode
        self.comparison_function = self.init_comparison_function(params.comparison_function)

    def init_comparison_function(
        self, func_string: str, func_name="udf_reduce_priority_func", args="r1, r2"
    ):
        if func_string:
            code = f"def {func_name}({args}):\n"
            for line in func_string.splitlines():
                code += f"    {line}\n"
            namespace = {}
            exec(code, namespace)
            return namespace[func_name]
        else:
            return lambda r1, r2: r1


# endregion


# region: sampler
@meta_operator
class Sampler(InputOutputOperator):
    """
    The meta operator for operators that sample rows from an input dataframe.
    """
# endregion
