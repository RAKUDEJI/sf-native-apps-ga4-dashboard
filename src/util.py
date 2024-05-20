import os


def is_prod() -> bool:

    return os.environ["USER"] == "udf"


def calculate_delta(current_value, previous_value):
    if previous_value is None:
        return "NA"

    delta = ((current_value - previous_value) / previous_value) * 100
    return str(round(delta) / 10) + "%"
