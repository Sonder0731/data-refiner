from data_refiner.core import recorder
from tests.tools import assert_same_by_dict

from data_refiner.ops.other.field_type_converter import FieldTypeConverter


class TestFieldTypeConverter:
    def test_field_type_conversion(self, spark):
        # Create test data with mixed types that need conversion
        test_data = [(1, "123", "45.67"), (2, "456", "78.90"), (3, None, None)]
        df = spark.createDataFrame(test_data, ["__id__", "age_str", "score_str"])
        recorder.record("test.df", df)

        # Convert string fields to integer and double
        # Note: field and output_field are required but field_type_converter uses field_type_mapping
        # So we pass dummy values for field and output_field to satisfy validation
        mapper = FieldTypeConverter(
            input_df="test.df",
            output_df="test.df_output",
            field_type_mapping={"age_str": "integer", "score_str": "double"},
            show=True,
        )
        df_processed = mapper.process(spark=spark)
        expect = [
            {"__id__": 1, "age_str": 123, "score_str": 45.67},
            {"__id__": 2, "age_str": 456, "score_str": 78.90},
            {"__id__": 3, "age_str": None, "score_str": None},
        ]
        assert_same_by_dict(df_processed, expect)
