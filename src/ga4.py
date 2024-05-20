from datetime import datetime

import snowflake.snowpark.functions as f
from snowflake.snowpark import Session
from snowflake.snowpark.dataframe import DataFrame, col
from snowflake.snowpark.functions import count, lit
from snowflake.snowpark.types import StringType

import util
from ga4model import Ga4Prop


def get_ga4(session: Session, reference_table: str = None):
    """
    GA4データのビューを返す
    """
    if util.is_prod():
        return session.sql(f"select * from reference('{reference_table}')")
    else:
        return session.table("ANALYTICS_272014362__VIEW")


def get_current_dataframe(df: DataFrame, week: int = 1):
    """
    GA4データのビューから特定期間のdataframeを取得
    """

    max_event_date: datetime = df.select(f.max(col("event_date"))).collect()[0][0]

    r = df.filter(
        col(Ga4Prop.EVENT_DATE.name)
        > f.dateadd("day", lit(-week * 7), lit(max_event_date))
    )

    return r


def get_new_user_events(df: DataFrame, week: int = 1):
    """
    GA4データのビューから特定期間の新規ユーザーに絞り込む
    """

    max_event_date: datetime = df.select(f.max(col("event_date"))).collect()[0][0]

    recent_period_user_ids = (
        df.where(
            col("event_date") > f.dateadd("day", lit(-week * 7), lit(max_event_date))
        )
        .select(col("user_pseudo_id"))
        .distinct()
    )
    # 指定した週よりも前の期間のユーザー
    before_recent_period_user_ids = (
        df.where(
            col("event_date") <= f.dateadd("day", lit(-week * 7), lit(max_event_date))
        )
        .select(col("user_pseudo_id"))
        .distinct()
    )
    r = df.where(
        (
            col("user_pseudo_id").isin(
                recent_period_user_ids.minus(before_recent_period_user_ids)
            )
        )
        & (col("event_date") > f.dateadd("day", lit(-week * 7), lit(max_event_date)))
    ).order_by(col("user_pseudo_id"))
    return r


def get_current_user_events(df: DataFrame, week: int = 1):
    """
    GA4データのビューから現在のユーザーに絞り込む
    """
    max_event_date: datetime = df.select(f.max(col(Ga4Prop.EVENT_DATE.name))).collect()[
        0
    ][0]

    r = df.filter(
        col(Ga4Prop.EVENT_DATE.name)
        > f.dateadd("day", lit(-week * 7), lit(max_event_date))
    ).order_by(col(Ga4Prop.USER_PSEUDO_ID.name))

    return r


def get_previous_period_user_events(df: DataFrame, week: int = 1):
    """
    GA4データのビューから指定した期間より前の期間のユーザーに絞り込む
    """
    max_event_date: datetime = df.select(f.max(col(Ga4Prop.EVENT_DATE.name))).collect()[
        0
    ][0]

    # 現在～指定期間までのユーザー
    current_period_user_ids = (
        df.where(
            col("event_date") > f.dateadd("day", lit(-week * 7), lit(max_event_date))
        ).select(col("user_pseudo_id"))
        # .distinct()
    )
    # 現在～前の期間までのユーザー
    current_to_previous_period_user_ids = (
        df.where(
            col("event_date") >= f.dateadd("day", lit(-week * 14), lit(max_event_date))
        ).select(col("user_pseudo_id"))
        # .distinct()
    )

    r = df.where(
        (
            col("user_pseudo_id").isin(
                current_to_previous_period_user_ids.minus(current_period_user_ids)
            )
        )
        & (col("event_date") > f.dateadd("day", lit(-week * 14), lit(max_event_date)))
    ).order_by(col("user_pseudo_id"))

    return r


def get_previous_period_new_user_events(df: DataFrame, week: int = 1):
    """
    GA4データのビューから指定した期間より前の期間のユーザーに絞り込む
    """
    max_event_date: datetime = df.select(f.max(col(Ga4Prop.EVENT_DATE.name))).collect()[
        0
    ][0]

    # 現在～指定期間までのユーザー
    current_period_user_ids = (
        df.where(
            col("event_date") > f.dateadd("day", lit(-week * 7), lit(max_event_date))
        )
        .select(col("user_pseudo_id"))
        .distinct()
    )
    # 現在～前の期間までのユーザー
    current_to_previous_period_user_ids = (
        df.where(
            col("event_date") >= f.dateadd("day", lit(-week * 14), lit(max_event_date))
        )
        .select(col("user_pseudo_id"))
        .distinct()
    )

    # 前の期間よりも前までのユーザー
    previous_period_user_ids = (
        df.where(
            col("event_date") < f.dateadd("day", lit(-week * 14), lit(max_event_date))
        )
        .select(col("user_pseudo_id"))
        .distinct()
    )

    r = df.where(
        (
            col("user_pseudo_id").isin(
                current_to_previous_period_user_ids.minus(
                    current_period_user_ids
                ).minus(previous_period_user_ids)
            )
        )
        & (col("event_date") > f.dateadd("day", lit(-week * 14), lit(max_event_date)))
    ).order_by(col("user_pseudo_id"))

    return r


def load_pagetitle_data(df: Session):

    return (
        df.flatten(col("event_params"))
        .where((col("event_name") == lit("page_view")))
        .select(
            col(Ga4Prop.EVENT_DATE.name),
            col("value").getItem("key").cast(StringType()).alias("param_key"),
            col("value")["value"]["string_value"]
            .cast(StringType())
            .alias("page_title"),
        )
        .where(col("param_key") == lit("page_title"))
        .group_by(col("page_title"))
        .agg(col("page_title"), count("*").alias("count"))
        .order_by(col("count"), ascending=False)
    )
