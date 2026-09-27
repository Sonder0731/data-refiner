import pytest

from data_refiner.core import recorder
from data_refiner.ops.mapper.mask_pii_mapper import MaskPiiMapper
from tests.tools import assert_same_by_dict


class TestMaskPiiMapper:
    def test_rejects_presidio_wrapper_shape(self):
        with pytest.raises(ValueError, match="type"):
            MaskPiiMapper(
                input_df="test.df",
                output_df="test.df_output",
                field="text",
                output_field="anonymized_text",
                mask_map={
                    "PHONE_NUMBER": {
                        "operator_name": "replace",
                        "params": {"new_value": "<PHONE>"},
                    }
                },
            )

    def test_mask_pii(self, spark):
        test_data = [
            (
                1,
                "XX的手机号码是13111111111，电话号码是：0571-11111111。我的身份证号是330123188807311111。订单号为123456。",
            ),
            (2, "His name is Mr. Jones and his phone number is 212-555-5555"),
            (3, "This operator refers to https://github.com/microsoft/presidio"),
            (4, None),
        ]
        df = spark.createDataFrame(test_data, ["__id__", "text"])
        recorder.record("test.df", df)

        op = MaskPiiMapper(
            input_df="test.df",
            output_df="test.df_output",
            mask_map={
                "PHONE_NUMBER": {
                    "type": "replace",
                    "new_value": "[US_PHONE_NUMBER_MASK]",
                },
                "ZH_ID_CARD": None,
                "ZH_PHONE_NUMBER": None,
                "ZH_LANDLINE_NUMBER": None,
                "URL": None,
            },
            field="text",
            output_field="anonymized_text",
            show=True,
            drop=["text"],
        )
        df = op.process()

        assert_same_by_dict(
            df,
            [
                {
                    "__id__": 1,
                    "anonymized_text": "XX的手机号码是<ZH_PHONE_NUMBER>，电话号码是：<ZH_LANDLINE_NUMBER>。我的身份证号是<ZH_ID_CARD>。订单号为123456。",
                },
                {
                    "__id__": 2,
                    "anonymized_text": "His name is Mr. Jones and his phone number is [US_PHONE_NUMBER_MASK]",
                },
                {"__id__": 3, "anonymized_text": "This operator refers to <ANONYMIZED>"},
                {"__id__": 4, "anonymized_text": None},
            ],
        )
