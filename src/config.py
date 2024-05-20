from typing import Optional
from connection import get_connection
import pandas as pd
from typing import Dict, Any
from typing import Dict, Iterable, Optional
import json
from snowflake.snowpark import Session, Table, Column
from snowflake.snowpark.functions import col, when_matched, when_not_matched

Variant = Dict


class KeyValueTable:
    def __init__(self, name: str, append_only: bool = False):
        self._session = get_connection()
        self._name = name
        self._append_only = append_only

    @property
    def _table(self) -> Table:
        return self.session.table(self._name)

    @property
    def session(self):
        if not self._session:
            raise ValueError("No current session")
        return self._session

    def _key_value_df(self, key: str, value: Variant):
        if not isinstance(value, dict):
            raise ValueError(f"Provided value is of {type(value)} type, expected dict")
        return self.session.create_dataframe([[key, value]], schema=["key", "value"])

    # 設定値の取得
    def get_value(self, key: str) -> Optional[Variant]:
        if self._append_only:
            rows = (
                self._table.select("value").filter(col("key") == key).sort(col("timestamp").desc()).limit(1).collect()
            )
        else:
            rows = self._table.select("value").filter(col("key") == key).collect()
        if not rows:
            return None
        return json.loads(rows[0][0])

    def get_all_where(self, where: Column) -> Iterable[Variant]:
        rows = self._table.select("value").filter(where).sort(col("key").asc()).collect()
        return [json.loads(r[0]) for r in rows]

    # 設定値の保存
    def merge(self, key: str, value: Variant):
        target = self._table
        source = self._key_value_df(key, value)

        if self._append_only:
            return self._key_value_df(key, value).write.mode("append").save_as_table(self._name, column_order="name")

        return target.merge(
            source,
            target.col("key") == source.col("key"),
            [
                when_matched().update({"value": source.col("value")}),
                when_not_matched().insert({"key": source.col("key"), "value": source.col("value")}),
            ],
        )


AppConfigTable = KeyValueTable(name=f"CONFIG")
