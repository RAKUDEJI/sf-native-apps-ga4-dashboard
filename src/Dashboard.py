import os


import altair as alt
import streamlit as st
from connection import get_connection


from translate import get_label
from util import calculate_delta, is_prod
from viewmodel import ViewModel
import json
import pandas as pd
import re


if is_prod():
    from permission import object_reference

st.set_page_config(layout="wide")


def main():
    lang_option = display_language_selector()
    st.title(get_label("key_title", lang_option))
    period_option = display_period_selector(lang_option)

    try:

        with st.expander(get_label("key_expander_label", lang_option)):
            chat_ui(lang_option)
        with st.spinner(get_label("key_loading_label", lang_option)):

            prepared_data, new_user_data, previous_user_data = load_data(period_option)
            st.session_state["prepared_data"] = prepared_data
            st.session_state["new_user_data"] = new_user_data
            st.session_state["previous_user_data"] = previous_user_data
            display_metrics(
                st.session_state["prepared_data"],
                st.session_state["new_user_data"],
                st.session_state["previous_user_data"],
                lang_option,
            )
            display_line_charts(
                st.session_state["prepared_data"],
                st.session_state["new_user_data"],
                lang_option,
            )

            filter_option = display_filter_selector(lang_option)
            display_charts(
                st.session_state["prepared_data"],
                st.session_state["new_user_data"],
                filter_option,
                lang_option,
            )

    except Exception as e:
        # st.write(e)
        st.warning(get_label("key_load_error", lang_option), icon="⚠️")


def display_language_selector():
    return st.selectbox("Language", ("English", "日本語"))


def display_period_selector(lang_option):
    return st.selectbox(
        get_label("key_select_period", lang_option),
        ("7", "14", "28", "84"),
        format_func=lambda x: f"{x + get_label('key_select_oprion_suffix', lang_option)}",
    )


def load_data(period_option):
    vm = ViewModel()
    prepared_data = vm.load_data("ga4_view", int(period_option))
    new_user_data = vm.new_user_data("ga4_view", int(period_option))
    previous_user_data = vm.previous_user_data("ga4_view", int(period_option))
    return prepared_data, new_user_data, previous_user_data


def display_metrics(prepared_data, new_user_data, previous_user_data, lang_option):
    user_aggregated_df_sum = prepared_data["user_aggregated_df_sum"]
    aggregated_df_sum = prepared_data["user_event_aggregated_df_sum"]
    new_users_aggregated_sum = new_user_data["new_users_aggregated_sum"]
    previous_users_ids_aggregated_sum = previous_user_data["previous_users_aggregated_sum"]
    previous_user_event_aggregated_sum = previous_user_data["previous_user_event_aggregated_sum"]
    previous_new_users_aggregated_sum = previous_user_data["previous_new_users_aggregated_sum"]

    col1, col2, col3, _ = st.columns(4)

    with col1:
        display_metric(
            get_label("key_user_count", lang_option),
            user_aggregated_df_sum,
            calculate_delta(user_aggregated_df_sum, previous_users_ids_aggregated_sum),
        )
    with col2:
        display_metric(
            get_label("key_event_count", lang_option),
            aggregated_df_sum,
            calculate_delta(aggregated_df_sum, previous_user_event_aggregated_sum),
        )
    with col3:
        display_metric(
            get_label("key_new_user_count", lang_option),
            new_users_aggregated_sum,
            calculate_delta(new_users_aggregated_sum, previous_new_users_aggregated_sum),
        )


def display_metric(label, value, delta):
    delta_color = "off" if delta == "NA" else "normal"
    st.metric(label, value, delta=f"{delta}", delta_color=delta_color)


def display_line_charts(prepared_data, new_user_data, lang_option):

    session = get_connection()

    try:

        forecast_event = session.sql(f"select * from reference('forecast_event_view')").to_pandas()

        forecast_user = session.sql(f"select * from reference('forecast_user_view')").to_pandas()

        forecast_new_user = session.sql(f"select * from reference('forecast_new_user_view')").to_pandas()

        user_data = prepared_data["user_aggregated_df"]
        event_data = prepared_data["user_event_aggregated_df"]
        new_user_data = new_user_data["new_users_ids_aggregated_df"]

        # データの準備
        actual_data_dict = {
            "user_data": user_data,
            "event_data": event_data,
            "new_user_data": new_user_data,
        }

        forecast_data_dict = {
            "user_data": forecast_user,
            "event_data": forecast_event,
            "new_user_data": forecast_new_user,
        }

        max_scaled_values = display_and_get_max_values(actual_data_dict, forecast_data_dict)

        max_scale_user_data = max_scaled_values.get("user_data")
        max_scale_event_data = max_scaled_values.get("event_data")
        max_scale_new_user_data = max_scaled_values.get("new_user_data")

        tab1, tab2, tab3 = st.tabs(
            [
                get_label("key_user_count", lang_option),
                get_label("key_event_count", lang_option),
                get_label("key_new_user_count", lang_option),
            ]
        )

        with tab1:
            col1, col2 = st.columns(2)

            scale = alt.Scale(domain=[0, max_scale_user_data])

            with col1:
                st.markdown(get_label("key_graph_title_actual", lang_option))
                display_chart(user_data, lang_option, scale)
            with col2:
                st.markdown(get_label("key_graph_title_forecast", lang_option))

                user_forecast_data = pd.DataFrame(forecast_user)

                display_forecast_chart(user_forecast_data, lang_option, scale)

        with tab2:
            col1, col2 = st.columns(2)

            scale = alt.Scale(domain=[0, max_scale_event_data])
            with col1:
                st.markdown(get_label("key_graph_title_actual", lang_option))
                display_chart(event_data, lang_option, scale)
            with col2:
                st.markdown(get_label("key_graph_title_forecast", lang_option))
                event_forecast_data = pd.DataFrame(forecast_event)
                display_forecast_chart(event_forecast_data, lang_option, scale)

        with tab3:
            col1, col2 = st.columns(2)
            scale = alt.Scale(domain=[0, max_scale_new_user_data])

            with col1:
                st.markdown(get_label("key_graph_title_actual", lang_option))
                display_chart(new_user_data, lang_option, scale)
            with col2:
                st.markdown(get_label("key_graph_title_forecast", lang_option))
                new_user_forecast_data = pd.DataFrame(forecast_new_user)
                display_forecast_chart(pd.DataFrame(new_user_forecast_data), lang_option, scale)
    except Exception as e:
        # st.write(e)
        st.warning(get_label("key_forecast_data_load_error", lang_option), icon="⚠️")


def display_and_get_max_values(actual_data_dict, forecast_data_dict, scale_ratio=1.2):
    max_values = {}
    for key, data in actual_data_dict.items():
        max_actual_df = data.to_pandas()["COUNT"].max()
        max_values[key] = max_actual_df

    for key, forecast_data in forecast_data_dict.items():
        forecast_df = pd.DataFrame(forecast_data)
        max_forecast_df = forecast_df["UPPER_BOUND"].max()
        max_values[key] = max(max_values[key], max_forecast_df)

    max_scaled_values = {key: value * scale_ratio for key, value in max_values.items()}

    return max_scaled_values


def display_chart(df, lang_option, scale):
    chart = (
        alt.Chart(df.to_pandas())
        .mark_line(point=True, color="#195AFF")
        .encode(
            x=alt.X(
                "EVENT_DATE:T",
                axis=alt.Axis(
                    title="",
                    format=get_label("key_graph_label_format_month-day", lang_option),
                ),
            ),
            y=alt.Y("COUNT:Q", axis=alt.Axis(title=""), scale=scale),
        )
        .properties(title="")
    )

    st.altair_chart(chart, use_container_width=True)


def display_forecast_chart(df, lang_option, scale):
    base = alt.Chart(df).encode(
        x=alt.X(
            "TS:T",
            axis=alt.Axis(
                title="",
                format=get_label("key_graph_label_format_month-day", lang_option),
            ),
        )
    )

    forecast_line = base.mark_line(point=True, color="#195AFF").encode(
        y=alt.Y("FORECAST:Q", axis=alt.Axis(title=""), scale=scale)
    )

    bound_area = base.mark_area(opacity=0.2, color="#195AFF").encode(
        y=alt.Y("LOWER_BOUND:Q", scale=scale), y2="UPPER_BOUND:Q"
    )

    chart = alt.layer(bound_area, forecast_line).properties(title="")
    st.altair_chart(chart, use_container_width=True)


def display_filter_selector(lang_option):
    return st.selectbox(
        f"{get_label('key_filter_option_title', lang_option)}",
        (
            f"{get_label('key_filter_option_all_users', lang_option)}",
            f"{get_label('key_filter_option_new_users', lang_option)}",
        ),
    )


def display_charts(prepared_data, new_user_data, filter_option, lang_option):
    col1, col2 = st.columns(2)

    with col1:
        df = (
            prepared_data["user_device_model_aggregated_df"]
            if filter_option == "全ユーザー"
            else new_user_data["new_users_device_model_aggregated_df"]
        )
        display_bar_chart(
            df,
            "COUNT:Q",
            "DEVICE_MOBILE_MODEL_NAME:N",
            get_label("key_graph_device_model_count", lang_option),
        )

    with col2:
        df = (
            prepared_data["event_name_aggregated_df"]
            if filter_option == "全ユーザー"
            else new_user_data["new_users_event_name_aggregated_df"]
        )
        display_bar_chart(
            df,
            "COUNT:Q",
            "EVENT_NAME:N",
            get_label("key_graph_title_user_event_count", lang_option),
        )

    col3, col4 = st.columns(2)
    with col3:
        df = (
            prepared_data["user_device_os_aggregated_df"]
            if filter_option == "全ユーザー"
            else new_user_data["new_users_device_os_aggregated_df"]
        )
        display_bar_chart(
            df,
            "COUNT:Q",
            "DEVICE_OPERATING_SYSTEM_VERSION:N",
            get_label("key_graph_title_device_os_ver_count", lang_option),
        )

    with col4:
        with st.container():
            df = (
                prepared_data["user_region_aggregated_df"]
                if filter_option == "全ユーザー"
                else new_user_data["new_users_region_aggregated_df"]
            )
            display_region_chart(df, lang_option)


def display_bar_chart(df, x_column, y_column, title):
    chart = (
        alt.Chart(df.to_pandas())
        .mark_bar(color="#195AFF")
        .encode(
            x=alt.X(x_column, axis=alt.Axis(title="")),
            y=alt.Y(y_column, axis=alt.Axis(title=""), sort="-x"),
        )
        .properties(title=title)
    )
    st.altair_chart(chart, use_container_width=True)


def display_region_chart(df, lang_option):
    chart = (
        alt.Chart(df.to_pandas().head(10))
        .mark_bar(color="#195AFF")
        .encode(
            x=alt.X("COUNT:Q", axis=alt.Axis(title="")),
            y=alt.Y("GEO_REGION:N", axis=alt.Axis(title=""), sort="-x"),
        )
        .properties(title=get_label("key_graph_title_user_region_count", lang_option))
    )

    if lang_option == "日本語":
        st.altair_chart(chart, use_container_width=True)
        # col1, col2 = st.columns(2)
        # display_map = (
        #     prepared_data["folium_map"]
        #     if filter_option == "全ユーザー"
        #     else new_user_data["new_users_folium_map"]
        # )
        # with col1:
        #     st_folium(display_map, height=512, returned_objects=[])
        # with col2:
        #     st.altair_chart(chart, use_container_width=True)
    else:
        st.altair_chart(chart, use_container_width=True)


def chat_ui(lang_option):
    if "messages" not in st.session_state:
        st.session_state.messages = []

    if question := st.text_input(get_label("key_text_input_label", lang_option)):
        # prompt = generate_prompt(question, lang_option)
        with st.spinner(get_label("key_loading_label", lang_option)):
            st.session_state.messages = []
            response = get_complete(question, lang_option)
            # st.write(response)
            st.session_state.messages.append(response)
            content = st.session_state.messages[0]
            # content = res
            display_response(content)

            if st.session_state.messages:
                # col1, col2 = st.columns(2)
                # with col2:
                # if st.button(
                #     get_label("key_button_regeneration_label", lang_option)
                # ):
                #     st.session_state.messages = []
                #     st.session_state.messages.append(
                #         get_complete(prompt, lang_option)
                #     )
                # with col1:
                df = None
                if st.button(
                    get_label("key_button_display_df_label", lang_option),
                    key="rerun",
                ):
                    code_block = get_code_block(response)
                    if code_block:
                        try:
                            df = query_dataframe(code_block, lang_option)
                            if df:
                                st.dataframe(df, hide_index=True)
                                # CSVダウンロード
                                # st.button(
                                #     get_label(
                                #         "key_button_cdv_dl_label", lang_option
                                #     )
                                # )
                            else:
                                st.warning(
                                    get_label("key_SQL_error", lang_option),
                                    icon="⚠️",
                                )
                        except Exception as e:
                            # st.write(e)
                            st.warning(get_label("key_SQL_error", lang_option), icon="⚠️")


def get_complete(prompt, lang_option):
    session = get_connection()

    system_prompt = get_system_prompt(lang_option)
    try:
        response = session.sql(
            f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic',
                ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT('role','system','content','{system_prompt}'),
                    OBJECT_CONSTRUCT('role', 'user', 'content', '{prompt}')
                ),
                OBJECT_CONSTRUCT('temperature', 0)
            )
        """
        ).collect()[0][0]

        data = json.loads(response)
        messages_value = data["choices"][0]["messages"]

        return messages_value
    except Exception as e:
        # st.write(e)
        st.warning(get_label("key_llm_error", lang_option), icon="⚠️")


def get_system_prompt(lang_option):
    if lang_option == "日本語":
        return """
            SQLの説明テキストで質問に答える
            ユーザはWeb GA4のデータを探索しています。
            # 制約事項
            ・1つのSQLを提示すること。
            ・以下の#Tableセクションのテーブルが出力SQLで使用されます。
            ・マークダウン形式で出力すること。
            ・出力SQLのカラムには、以下の#column infoを使用すること。
            ・説明文は日本語とする。

            # Table
            reference(\\'ga4\\_view\\')

            #column info
            COLUMN_NAME,DATA_TYPE
            DEVICE_MOBILE_MARKETING_NAME,TEXT
            USER_ID,TEXT
            APP_INFO_INSTALL_SOURCE,TEXT
            DEVICE_TIME_ZONE_OFFSET_SECONDS,NUMBER
            DEVICE_WEB_INFO_HOSTNAME,TEXT
            PRIVACY_INFO_ANALYTICS_STORAGE,TEXT
            EVENT_DIMENSIONS_HOSTNAME,TEXT
            ECOMMERCE_UNIQUE_ITEMS,NUMBER
            EVENT_NAME,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_CONTENT,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_SOURCE,TEXT
            TRAFFIC_SOURCE_MEDIUM,TEXT
            GEO_SUB_CONTINENT,TEXT
            COLLECTED_TRAFIC_SOURCE_DCLID,TEXT
            ECOMMERCE_TAX_VALUE,FLOAT
            DEVICE_MOBILE_OS_HARDWARE_MODEL,TEXT
            DEVICE_BROWSER,TEXT
            USER_FIRST_TOUCH_TIMESTAMP,TIMESTAMP_NTZ
            APP_INFO_INSTALL_STORE,TEXT
            DEVICE_ADVERTISING_ID,TEXT
            PRIVACY_INFO_ADS_STORAGE,TEXT
            EVENT_PARAMS,VARIANT
            DEVICE_CATEGORY,TEXT
            STREAM_ID,TEXT
            DEVICE_MOBILE_BRAND_NAME,TEXT
            ECOMMERCE_PURCHASE_REVENUE,FLOAT
            ECOMMERCE_SHIPPING_VALUE_IN_USD,FLOAT
            DEVICE_OPERATING_SYSTEM_VERSION,TEXT
            EVENT_BUNDLE_SEQUENCE_ID,NUMBER
            DEVICE_VENDOR_ID,TEXT
            ECOMMERCE_TRANSACTION_ID,TEXT
            USER_PSEUDO_ID,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_CAMPAIGN_NAME,TEXT
            APP_INFO_ID,TEXT
            EVENT_VALUE_IN_USD,FLOAT
            PRIVACY_INFO_USES_TRANSIENT_TOKEN,TEXT
            GEO_METRO,TEXT
            DEVICE_MOBILE_MODEL_NAME,TEXT
            TRAFFIC_SOURCE_SOURCE,TEXT
            GEO_COUNTRY,TEXT
            ITEMS,VARIANT
            COLLECTED_TRAFIC_SOURCE_SRSLTID,TEXT
            EVENT_DATE,DATE
            DEVICE_BROWSER_VERSION,TEXT
            EVENT_SERVER_TIMESTAMP_OFFSET,NUMBER
            ECOMMERCE_REFUND_VALUE_IN_USD,FLOAT
            TRAFFIC_SOURCE_NAME,TEXT
            ECOMMERCE_SHIPPING_VALUE,FLOAT
            ECOMMERCE_TAX_VALUE_IN_USD,FLOAT
            GEO_CITY,TEXT
            USER_PROPERTIES,VARIANT
            DEVICE_OPERATING_SYSTEM,TEXT
            EVENT_PREVIOUS_TIMESTAMP,TIMESTAMP_NTZ
            ECOMMERCE_PURCHASE_REVENUE_IN_USD,FLOAT
            USER_LTV_CURRENCY,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_CAMPAIGN_ID,TEXT
            DEVICE_WEB_INFO_BROWSER,TEXT
            GEO_REGION,TEXT
            DEVICE_WEB_INFO_BROWSER_VERSION,TEXT
            DEVICE_LANGUAGE,TEXT
            PLATFORM,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_MEDIUM,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_TERM,TEXT
            ECOMMERCE_REFUND_VALUE,FLOAT
            EVENT_TIMESTAMP,TIMESTAMP_NTZ
            APP_INFO_VERSION,TEXT
            ECOMMERCE_TOTAL_ITEM_QUANTITY,NUMBER
            GEO_CONTINENT,TEXT
            USER_LTV_REVENUE,FLOAT
            APP_INFO_FIREBASE_APP_ID,TEXT
            DEVICE_IS_LIMITED_AD_TRACKING,BOOLEAN
            COLLECTED_TRAFIC_SOURCE_GCLID,TEXT

            # example Input Format
            先週、最も使用された機器名のトップ5を教えてください。
            ## example Output format
            前週に最も使用されたデバイス名のトップ5を検索する。
            ```sql
            SELECT DEVICE_MOBILE_BRAND_NAME, COUNT(*) AS usage_count
            FROM reference(\\'ga4\\_view\\')
            WHERE EVENT_DATE BETWEEN DATEADD(WEEK, -1, CURRENT_DATE()) AND CURRENT_DATE()
            GROUP BY DEVICE_MOBILE_BRAND_NAME
            ORDER BY usage_count DESC
            LIMIT 5;
            ```
            解説:
            上記のクエリは、前週のデバイス名の使用状況を集計し、使用頻度の高い上位5つのデバイス名とその使用回数を取得します。
            以下は、クエリの内容を自然な日本語で説明したものです。
            ga4_viewテーブルを参照します。WHEREクローズでは、EVENT_DATEが前週の日付範囲内にあるレコードをフィルタリングします。
            DATEADD(WEEK, -1, CURRENT_DATE())は、現在の日付から1週間前の日付を計算します。CURRENT_DATE()は現在の日付を取得します。
            GROUP BY DEVICE_MOBILE_BRAND_NAMEは、デバイス名ごとにレコードをグループ化します。COUNT(*)は、各デバイス名の出現回数をカウントします。
            ORDER BY usage_count DESCは、使用回数の降順で結果をソートします。LIMIT 5は、使用回数が最も多い上位5つのデバイス名を取得します。
            このクエリは、前週に最も頻繁に使用されたデバイス名の上位5つと、それぞれの使用回数を返します。デバイス名はDEVICE_MOBILE_BRAND_NAME列に格納されています。

        """
    else:
        return """
            Answer questions with an explanatory text for the SQL
            User is exploring web GA4 data.
            #Constraints
            ・One SQL must be presented.
            ・The tables in the #Table section below are used in the output SQL.
            ・Output in markdown format.
            ・The following #column examples must be used for columns in the SQL output.
            ・Explanatory text should use English


            # Table
            reference(\\'ga4\\_view\\')


            #column info
            COLUMN_NAME,DATA_TYPE
            DEVICE_MOBILE_MARKETING_NAME,TEXT
            USER_ID,TEXT
            APP_INFO_INSTALL_SOURCE,TEXT
            DEVICE_TIME_ZONE_OFFSET_SECONDS,NUMBER
            DEVICE_WEB_INFO_HOSTNAME,TEXT
            PRIVACY_INFO_ANALYTICS_STORAGE,TEXT
            EVENT_DIMENSIONS_HOSTNAME,TEXT
            ECOMMERCE_UNIQUE_ITEMS,NUMBER
            EVENT_NAME,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_CONTENT,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_SOURCE,TEXT
            TRAFFIC_SOURCE_MEDIUM,TEXT
            GEO_SUB_CONTINENT,TEXT
            COLLECTED_TRAFIC_SOURCE_DCLID,TEXT
            ECOMMERCE_TAX_VALUE,FLOAT
            DEVICE_MOBILE_OS_HARDWARE_MODEL,TEXT
            DEVICE_BROWSER,TEXT
            USER_FIRST_TOUCH_TIMESTAMP,TIMESTAMP_NTZ
            APP_INFO_INSTALL_STORE,TEXT
            DEVICE_ADVERTISING_ID,TEXT
            PRIVACY_INFO_ADS_STORAGE,TEXT
            EVENT_PARAMS,VARIANT
            DEVICE_CATEGORY,TEXT
            STREAM_ID,TEXT
            DEVICE_MOBILE_BRAND_NAME,TEXT
            ECOMMERCE_PURCHASE_REVENUE,FLOAT
            ECOMMERCE_SHIPPING_VALUE_IN_USD,FLOAT
            DEVICE_OPERATING_SYSTEM_VERSION,TEXT
            EVENT_BUNDLE_SEQUENCE_ID,NUMBER
            DEVICE_VENDOR_ID,TEXT
            ECOMMERCE_TRANSACTION_ID,TEXT
            USER_PSEUDO_ID,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_CAMPAIGN_NAME,TEXT
            APP_INFO_ID,TEXT
            EVENT_VALUE_IN_USD,FLOAT
            PRIVACY_INFO_USES_TRANSIENT_TOKEN,TEXT
            GEO_METRO,TEXT
            DEVICE_MOBILE_MODEL_NAME,TEXT
            TRAFFIC_SOURCE_SOURCE,TEXT
            GEO_COUNTRY,TEXT
            ITEMS,VARIANT
            COLLECTED_TRAFIC_SOURCE_SRSLTID,TEXT
            EVENT_DATE,DATE
            DEVICE_BROWSER_VERSION,TEXT
            EVENT_SERVER_TIMESTAMP_OFFSET,NUMBER
            ECOMMERCE_REFUND_VALUE_IN_USD,FLOAT
            TRAFFIC_SOURCE_NAME,TEXT
            ECOMMERCE_SHIPPING_VALUE,FLOAT
            ECOMMERCE_TAX_VALUE_IN_USD,FLOAT
            GEO_CITY,TEXT
            USER_PROPERTIES,VARIANT
            DEVICE_OPERATING_SYSTEM,TEXT
            EVENT_PREVIOUS_TIMESTAMP,TIMESTAMP_NTZ
            ECOMMERCE_PURCHASE_REVENUE_IN_USD,FLOAT
            USER_LTV_CURRENCY,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_CAMPAIGN_ID,TEXT
            DEVICE_WEB_INFO_BROWSER,TEXT
            GEO_REGION,TEXT
            DEVICE_WEB_INFO_BROWSER_VERSION,TEXT
            DEVICE_LANGUAGE,TEXT
            PLATFORM,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_MEDIUM,TEXT
            COLLECTED_TRAFIC_SOURCE_MANUAL_TERM,TEXT
            ECOMMERCE_REFUND_VALUE,FLOAT
            EVENT_TIMESTAMP,TIMESTAMP_NTZ
            APP_INFO_VERSION,TEXT
            ECOMMERCE_TOTAL_ITEM_QUANTITY,NUMBER
            GEO_CONTINENT,TEXT
            USER_LTV_REVENUE,FLOAT
            APP_INFO_FIREBASE_APP_ID,TEXT
            DEVICE_IS_LIMITED_AD_TRACKING,BOOLEAN
            COLLECTED_TRAFIC_SOURCE_GCLID,TEXT

            # example Input Format
            Please tell me the top 5 most used device names from last week.
            ## example Output format
            To retrieve the top 5 most used device names from the previous week.
            ```sql
            SELECT DEVICE_MOBILE_BRAND_NAME, COUNT(*) AS usage_count
            FROM reference(\\'ga4\\_view\\')
            WHERE EVENT_DATE BETWEEN DATEADD(WEEK, -1, CURRENT_DATE()) AND CURRENT_DATE()
            GROUP BY DEVICE_MOBILE_BRAND_NAME
            ORDER BY usage_count DESC
            LIMIT 5;
            ```
            Explanation:
            reference(\\'ga4\\_view\\') refers to the ga4_view table.
            The WHERE clause filters the records where EVENT_DATE falls within the date range of the previous week.
            DATEADD(WEEK, -1, CURRENT_DATE()) calculates the date one week ago from the current date.
            CURRENT_DATE() retrieves the current date.
            GROUP BY DEVICE_MOBILE_BRAND_NAME groups the records by device name.
            COUNT(*) counts the number of occurrences for each device name.
            ORDER BY usage_count DESC sorts the results in descending order based on the usage count.
            LIMIT 5 retrieves the top 5 device names with the highest usage count.
            This query will return the top 5 most frequently used device names and their respective usage counts from the previous week. The device names are stored in the DEVICE_MOBILE_BRAND_NAME column.
        """


def display_response(response):
    with st.chat_message("assistant"):
        st.markdown(response)


def get_code_block(message):

    code_blocks = re.findall(r"```sql(.*?)```", message, re.DOTALL)
    if code_blocks:
        for code_block in code_blocks:
            return code_block.strip().rstrip(";")
    else:
        # print(message)
        sql = message.strip().rstrip(";")
        return sql


def query_dataframe(query, lang_option):
    session = get_connection()
    try:
        dataframe = session.sql(query)
        return dataframe
    except Exception as e:
        # st.write(e)
        st.warning(get_label("key_SQL_error", lang_option), icon="⚠️")


if __name__ == "__main__":
    if os.environ["USER"] == "udf":
        with object_reference("ga4_view"):
            main()
    else:
        main()
