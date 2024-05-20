#!/bin/bash
snow sql --query "CREATE APPLICATION PACKAGE IF NOT EXISTS ga4_dash_pkg DISTRIBUTION = EXTERNAL"
snow sql --query "CREATE STAGE IF NOT EXISTS  ga4_dash_pkg.PUBLIC.src"
snow object stage copy app --parallel 12 --overwrite  @ga4_dash_pkg.PUBLIC.src
snow object stage copy src --parallel 12 --overwrite  @ga4_dash_pkg.PUBLIC.src/streamlit
#snow sql --query "ALTER APPLICATION PACKAGE ga4_dash_pkg ADD VERSION v1 USING '@ga4_dash_pkg.PUBLIC.src'"
snow sql --query "ALTER APPLICATION PACKAGE ga4_dash_pkg ADD PATCH FOR VERSION v1 USING '@ga4_dash_pkg.PUBLIC.src'"