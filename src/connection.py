from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import json, util


def get_connection():
    # 本番環境
    if util.is_prod():
        try:
            return get_active_session()
        except:
            return None
    # テスト環境
    else:
        connection_parameters = json.loads(open(".connection_param.json").read())
        return Session.builder.configs(connection_parameters).getOrCreate()
