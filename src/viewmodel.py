import snowflake.snowpark.functions as f
from snowflake.snowpark.dataframe import col
from snowflake.snowpark.functions import count
import snowflake.cortex as cortex

import ga4

from connection import get_connection
from ga4model import Ga4Prop


class ViewModel:
    def __init__(self):
        self.session = get_connection()

    # def _generate_folium_map(self, df):
    #     m = folium.Map(
    #         location=(36.56583, 139.88361),
    #         tiles="cartodbdark_matter",
    #         attr="都道府県庁所在地、人口、面積(2016年)",
    #         zoom_start=4,
    #     )
    #     folium.Choropleth(
    #         geo_data=get_geo_json(util.is_prod()),
    #         data=df.to_pandas(),
    #         columns=["PREFECTURE_ID", "COUNT"],
    #         key_on="feature.properties.id",
    #         fill_color="Blues",
    #         nan_fill_color="darkgray",
    #         fill_opacity=0.8,
    #         nan_fill_opacity=0.8,
    #         line_opacity=0.2,
    #     ).add_to(m)
    #     return m

    def _get_ga4_data(self, reference_table: str, days=7):
        source = ga4.get_ga4(self.session, reference_table)
        week = days // 7
        return ga4.get_current_user_events(source, week)

    def _get_new_users_data(self, reference_table: str, days=7):
        source = ga4.get_ga4(self.session, reference_table)
        week = days // 7
        return ga4.get_new_user_events(source, week)

    def _get_previous_users_data(self, reference_table: str, days=7):
        source = ga4.get_ga4(self.session, reference_table)
        week = days // 7
        return ga4.get_previous_period_user_events(source, week)

    def _get_previous_new_users_data(self, reference_table: str, days=7):
        source = ga4.get_ga4(self.session, reference_table)
        week = days // 7
        return ga4.get_previous_period_new_user_events(source, week)

    def _aggregate_by_column(self, df, column):
        return (
            df.group_by(col(column))
            .agg(f.count(col(column)).alias("count"))
            .order_by(col("count"), ascending=False)
            .filter(col(column).is_not_null())
        )

    def load_data(self, reference_table: str, days=7):

        data = self._get_ga4_data(reference_table, days)

        user_aggregated_df = data.group_by(col(Ga4Prop.EVENT_DATE.name)).agg(
            f.count_distinct(col("user_pseudo_id")).alias("count")
        )

        event_name_aggregated_df = self._aggregate_by_column(data, Ga4Prop.EVENT_NAME.name)
        user_event_aggregated_df = (
            data.group_by(col(Ga4Prop.EVENT_DATE.name))
            .agg(col(Ga4Prop.EVENT_DATE.name), count("*").as_("count"))
            .order_by(col("count"), ascending=False)
        )
        users_trafic_aggregated_df = self._aggregate_by_column(data, Ga4Prop.TRAFFIC_SOURCE_NAME.name)
        user_country_aggregated_df = self._aggregate_by_column(data, Ga4Prop.GEO_COUNTRY.name)
        user_device_model_aggregated_df = self._aggregate_by_column(data, Ga4Prop.DEVICE_MOBILE_MODEL_NAME.name).limit(
            10
        )
        user_device_os_aggregated_df = self._aggregate_by_column(
            data, Ga4Prop.DEVICE_OPERATING_SYSTEM_VERSION.name
        ).limit(10)

        # udfはここでインポートしないといけない。
        from udf import get_prefecture_id

        user_region_aggregated_df = (
            data.group_by(col(Ga4Prop.GEO_REGION.name))
            .agg(
                get_prefecture_id(col(Ga4Prop.GEO_REGION.name)).alias("prefecture_id"),
                f.count(col(Ga4Prop.GEO_REGION.name)).alias("count"),
            )
            .order_by(col("count"), ascending=False)
            .filter(col(Ga4Prop.GEO_REGION.name).is_not_null())
        )

        # folium_map = self._generate_folium_map(user_region_aggregated_df)

        # 以下、集計結果を辞書形式で返却
        # 実際の集計結果の合計値を動的に計算することを推奨
        return {
            "event_name_aggregated_df": event_name_aggregated_df,
            "user_aggregated_df": user_aggregated_df,
            "user_event_aggregated_df": user_event_aggregated_df,
            "user_country_aggregated_df": user_country_aggregated_df,
            "user_region_aggregated_df": user_region_aggregated_df,
            "users_trafic_aggregated_df": users_trafic_aggregated_df,
            "user_device_model_aggregated_df": user_device_model_aggregated_df,
            "user_device_os_aggregated_df": user_device_os_aggregated_df,
            "user_event_aggregated_df_sum": user_event_aggregated_df.agg(f.sum("count")).collect()[0][0],
            "user_aggregated_df_sum": user_aggregated_df.agg(f.sum("count")).collect()[0][0],
            "pagetitle_data": ga4.load_pagetitle_data(data),
        }

    def new_user_data(self, reference_table: str, days=7):
        new_users_data = self._get_new_users_data(reference_table, days)

        new_users_ids_aggregated_df = new_users_data.group_by(col(Ga4Prop.EVENT_DATE.name)).agg(
            f.count_distinct(col("user_pseudo_id")).alias("count")
        )
        new_users_pagetitle_data = ga4.load_pagetitle_data(new_users_data)
        new_users_event_name_aggregated_df = self._aggregate_by_column(new_users_data, Ga4Prop.EVENT_NAME.name)
        new_users_trafic_aggregated_df = self._aggregate_by_column(new_users_data, Ga4Prop.TRAFFIC_SOURCE_NAME.name)
        new_users_device_model_aggregated_df = self._aggregate_by_column(
            new_users_data, Ga4Prop.DEVICE_MOBILE_MODEL_NAME.name
        ).limit(10)
        new_users_device_os_aggregated_df = self._aggregate_by_column(
            new_users_data, Ga4Prop.DEVICE_OPERATING_SYSTEM_VERSION.name
        ).limit(10)

        # udfはここでインポートしないといけない。
        from udf import get_prefecture_id

        new_users_region_aggregated_df = (
            new_users_data.group_by(col(Ga4Prop.GEO_REGION.name))
            .agg(
                get_prefecture_id(col(Ga4Prop.GEO_REGION.name)).alias("prefecture_id"),
                f.count(col(Ga4Prop.GEO_REGION.name)).alias("count"),
            )
            .order_by(col("count"), ascending=False)
            .filter(col(Ga4Prop.GEO_REGION.name).is_not_null())
        )
        new_users_country_aggregated_df = self._aggregate_by_column(new_users_data, Ga4Prop.GEO_COUNTRY.name)

        # new_users_folium_map = self._generate_folium_map(new_users_region_aggregated_df)

        return {
            "new_users_ids_aggregated_df": new_users_ids_aggregated_df,
            "new_users_pagetitle_data": new_users_pagetitle_data,
            "new_users_event_name_aggregated_df": new_users_event_name_aggregated_df,
            "new_users_trafic_aggregated_df": new_users_trafic_aggregated_df,
            "new_users_device_model_aggregated_df": new_users_device_model_aggregated_df,
            "new_users_device_os_aggregated_df": new_users_device_os_aggregated_df,
            "new_users_region_aggregated_df": new_users_region_aggregated_df,
            "new_users_country_aggregated_df": new_users_country_aggregated_df,
            "new_users_aggregated_sum": new_users_ids_aggregated_df.agg(f.sum("count")).collect()[0][0],
        }

    def previous_user_data(self, reference_table: str, days=7):

        previous_users_data = self._get_previous_users_data(reference_table, days)
        previous_new_users_data = self._get_previous_new_users_data(reference_table, days)

        previous_new_users_ids_aggregated_df = previous_new_users_data.group_by(col(Ga4Prop.EVENT_DATE.name)).agg(
            f.count_distinct(col("user_pseudo_id")).alias("count")
        )

        previous_user_event_aggregated_df = (
            previous_users_data.group_by(col(Ga4Prop.EVENT_DATE.name))
            .agg(col(Ga4Prop.EVENT_DATE.name), count("*").as_("count"))
            .order_by(col("count"), ascending=False)
        )

        previous_users_ids_aggregated_df = previous_users_data.group_by(col(Ga4Prop.EVENT_DATE.name)).agg(
            f.count_distinct(col("user_pseudo_id")).alias("count")
        )

        previous_users_event_name_aggregated_df = self._aggregate_by_column(
            previous_users_data, Ga4Prop.EVENT_NAME.name
        )

        return {
            "previous_users_ids_aggregated_df": previous_users_ids_aggregated_df,
            "previous_users_event_name_aggregated_df": previous_users_event_name_aggregated_df,
            "previous_new_users_ids_aggregated_df": previous_new_users_ids_aggregated_df,
            "previous_user_event_aggregated_sum": previous_user_event_aggregated_df.agg(f.sum("count")).collect()[0][0],
            "previous_users_aggregated_sum": previous_users_ids_aggregated_df.agg(f.sum("count")).collect()[0][0],
            "previous_new_users_aggregated_sum": previous_new_users_ids_aggregated_df.agg(f.sum("count")).collect()[0][
                0
            ],
        }

    def get_complete(self, prompt):
        response = cortex.Complete("snowflake-arctic", prompt, self.session)
        print(response)
        return response
