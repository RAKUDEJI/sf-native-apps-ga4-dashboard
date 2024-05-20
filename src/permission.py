from contextlib import contextmanager
import snowflake.permissions as permissions


@contextmanager
def object_reference(table_name):
    try:
        associations = permissions.get_reference_associations(table_name)
        if len(associations) == 0:
            permissions.request_reference(table_name)
        yield
    finally:
        # コンテキストマネージャの終了時に必要なクリーンアップ処理をここに配置
        # この例では特にクリーンアップ処理は必要ないため、passを使用
        pass
