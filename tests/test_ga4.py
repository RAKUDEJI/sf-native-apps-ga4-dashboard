import pandas as pd
import pytest
from snowflake.snowpark import Column, DataFrame, Session
from snowflake.snowpark.dataframe import col
from snowflake.snowpark.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ga4 import get_current_user_events, get_ga4, get_new_user_events

schema = StructType([StructField("user_pseudo_id", StringType()), StructField("event_date", DateType())])


@pytest.fixture(scope="session", autouse=True)
def snowflake_session():
    session = Session.builder.configs(connection_parameters).getOrCreate()
    yield session
    session.close()


@pytest.fixture(scope="session")
def ga4_df(snowflake_session: Session):
    #https://calcdate.onl.jp/ で計算
    #3ヶ月=12週間=84日とする
    test_data = [
        ["user1", "2023-01-01"],
        ["user1", "2023-01-08"],
        ["user2", "2023-01-02"],
        ["user3", "2023-01-08"],
        ["user4", "2022-12-25"],# 1/9より15日前
        ["user5", "2023-01-08"],
        ["user6", "2023-01-09"],
        ["user7", "2022-11-10"],# 1/9より60日前 < 3ヶ月
        ["user8", "2022-10-13"],# 1/9より88日前 > 3ヶ月
        ["user9", "2022-09-15"],# 1/9より116日前 > 3ヶ月
    ]
    return snowflake_session.create_dataframe(test_data, schema=schema)


# 1/9の集計結果には1/2は含まず、1/3から1/9までのデータが含まれる。
@pytest.mark.parametrize(
    "weeks_back, expected",
    [
        (1, [["user3", "2023-01-08"], ["user5", "2023-01-08"], ["user6", "2023-01-09"]]),
        (4, [["user1", "2023-01-01"], ["user1", "2023-01-08"], ["user2", "2023-01-02"], ["user3", "2023-01-08"], ["user4", "2022-12-25"], ["user5", "2023-01-08"], ["user6", "2023-01-09"],]),
        (12, [["user1", "2023-01-01"], ["user1", "2023-01-08"], ["user2", "2023-01-02"], ["user3", "2023-01-08"], ["user4", "2022-12-25"], ["user5", "2023-01-08"], ["user6", "2023-01-09"], ["user7", "2022-11-10"]]),
    ],
)
def test_new_users_identity(ga4_df: DataFrame, weeks_back: int, expected: list,snowflake_session: Session):
    actual =get_new_user_events(ga4_df, weeks_back)
    expected=snowflake_session.create_dataframe(expected, schema=schema)
  
    assert actual.collect() == expected.collect()



@pytest.mark.parametrize(
    "weeks_back, expected",
    [
        (1, [["user1", "2023-01-08"], ["user3", "2023-01-08"], ["user5", "2023-01-08"], ["user6", "2023-01-09"]]), 
        (4, [["user1", "2023-01-01"], ["user1", "2023-01-08"], ["user2", "2023-01-02"], ["user3", "2023-01-08"], ["user4", "2022-12-25"], ["user5", "2023-01-08"], ["user6", "2023-01-09"],]),  # 4週間前の現週ユーザーID
        (12, [["user1", "2023-01-01"], ["user1", "2023-01-08"], ["user2", "2023-01-02"], ["user3", "2023-01-08"], ["user4", "2022-12-25"], ["user5", "2023-01-08"], ["user6", "2023-01-09"], ["user7", "2022-11-10"]]),  # 12週間前の現週ユーザーID
    ],
)
def test_current_users_identity(ga4_df: DataFrame, weeks_back: int, expected: list, snowflake_session: Session):
    current_users = get_current_user_events(ga4_df, weeks_back)
    expected=snowflake_session.create_dataframe(expected, schema=schema)
    assert current_users.collect() == expected.collect()
    
    
