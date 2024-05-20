import geopandas as gpd
from connection import get_connection


def get_geo_json(is_prod: bool):
    if is_prod:
        # get_connection().file.get("@app_src.stage/seed/japan.json", "/tmp/japan.json")

        return gpd.read_file("japan.json")
    else:
        return gpd.read_file("seed/japan.json")
