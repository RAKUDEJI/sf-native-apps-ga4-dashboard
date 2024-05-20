#!/bin/bash
snow sql --query "CREATE APPLICATION PACKAGE IF NOT EXISTS ga4_dash_pkg_dev DISTRIBUTION = INTERNAL"
snow sql --query "CREATE STAGE IF NOT EXISTS  ga4_dash_pkg_dev.PUBLIC.src"
snow object stage copy app --overwrite  @ga4_dash_pkg_dev.PUBLIC.src
snow object stage copy src --overwrite  @ga4_dash_pkg_dev.PUBLIC.src/streamlit
snow sql --query "ALTER APPLICATION PACKAGE ga4_dash_pkg_dev ADD VERSION v1 USING '@ga4_dash_pkg_dev.PUBLIC.src'"