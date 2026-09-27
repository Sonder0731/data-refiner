"""
Reference:
-https://github.com/dongrixinyu/JioNLP
"""

from presidio_analyzer import PatternRecognizer, Pattern

# region: regex patterns
ZH_CELL_PHONE_PATTERN = (
    r"(?<=[^\d])(((\+86)?([- ])?)?((1[3-9][0-9]))([- ])?\d{4}([- ])?\d{4})(?=[^\d])"
)
ZH_LANDLINE_PHONE_PATTERN = (
    r"(?<=[^\d])(([\(（])?0\d{2,3}[\)） —-]{1,2}\d{7,8}|\d{3,4}[ -]\d{3,4}[ -]\d{4})(?=[^\d])"
)
ZH_ID_CARD_PATTERN = (
    r"(?<=[^0-9a-zA-Z])"
    r"((1[1-5]|2[1-3]|3[1-7]|4[1-6]|5[0-4]|6[1-5]|71|81|82|91)"
    r"(0[0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-3]|5[1-3]|90)"
    r"(0[0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-3]|5[1-7]|6[1-4]|7[1-4]|8[1-7])"
    r"(18|19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])"
    r"\d{3}[0-9xX])"
    r"(?=[^0-9a-zA-Z])"
)

CHINA_PROVINCE_ALIAS = "港澳台京津沪渝黑吉辽新藏青蒙晋冀豫甘陕川贵云宁苏浙皖鲁赣鄂湘粤闽桂琼"

MOTOR_VEHICLE_LICENCE_PLATE_PATTERN = "".join(
    [
        r"([",
        CHINA_PROVINCE_ALIAS[3:],
        r"]",
        r"[A-HJ-NP-Za-hj-np-z]",
        r"[·. 　]?",
        r"[A-HJ-NP-Za-hj-np-z0-9]{5,6})",
        r"(?![\da-zA-Z])",
    ]
)
# endregion

# region: pattern init
ZH_ID_CARD_PATTERN = Pattern(name="ZH_ID_CARD", regex=ZH_ID_CARD_PATTERN, score=1.0)
ZH_CELL_PHONE_PATTERN = Pattern(name="ZH_CELL_PHONE", regex=ZH_CELL_PHONE_PATTERN, score=1.0)
ZH_LANDLINE_PHONE_PATTERN = Pattern(
    name="ZH_LANDLINE_PHONE", regex=ZH_LANDLINE_PHONE_PATTERN, score=1.0
)
# endregion

# region: recognizer init
zh_phone_number_recognizer = PatternRecognizer(
    patterns=[ZH_CELL_PHONE_PATTERN],
    supported_entity="ZH_PHONE_NUMBER",
    name="ChinesePhoneNumberRecognizer",
)

zh_landline_recognizer = PatternRecognizer(
    patterns=[ZH_LANDLINE_PHONE_PATTERN],
    supported_entity="ZH_LANDLINE_NUMBER",
    name="ChineseLandlineNumberRecognizer",
)

zh_id_card_recognizer = PatternRecognizer(
    patterns=[ZH_ID_CARD_PATTERN],
    supported_entity="ZH_ID_CARD",
    name="ChineseIdCardRecognizer",
)
# endregion
