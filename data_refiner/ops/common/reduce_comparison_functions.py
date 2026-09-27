from pyspark import Row


def get_longer_text(row_1: Row, row_2: Row) -> str:
    return row_1["text"] if len(row_1["text"]) >= len(row_2["text"]) else row_2["text"]


def get_higher_grade(row_1: Row, row_2: Row):
    return row_1["grade"] if len(row_1["grade"]) >= len(row_2["grade"]) else row_2["grade"]


def get_higher_grade_then_longer_text(row_1: Row, row_2: Row):
    if row_1["grade"] > row_2["grade"]:
        return row_1
    elif row_1["grade"] < row_2["grade"]:
        return row_2
    else:
        if len(row_1["text"]) >= len(row_2["text"]):
            return row_1
        else:
            return row_2
