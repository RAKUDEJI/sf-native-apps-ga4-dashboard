from enum import Enum, auto


class Ga4Prop(Enum):
    EVENT_DATE = auto()
    EVENT_TIMESTAMP = auto()
    EVENT_NAME = auto()
    EVENT_PARAMS = auto()
    EVENT_PREVIOUS_TIMESTAMP = auto()
    EVENT_VALUE_IN_USD = auto()
    EVENT_BUNDLE_SEQUENCE_ID = auto()
    EVENT_SERVER_TIMESTAMP_OFFSET = auto()
    USER_ID = auto()
    USER_PSEUDO_ID = auto()
    PRIVACY_INFO_ANALYTICS_STORAGE = auto()
    PRIVACY_INFO_ADS_STORAGE = auto()
    PRIVACY_INFO_USES_TRANSIENT_TOKEN = auto()
    USER_PROPERTIES = auto()
    USER_FIRST_TOUCH_TIMESTAMP = auto()
    USER_LTV_REVENUE = auto()
    USER_LTV_CURRENCY = auto()
    DEVICE_CATEGORY = auto()
    DEVICE_MOBILE_BRAND_NAME = auto()
    DEVICE_MOBILE_MODEL_NAME = auto()
    DEVICE_MOBILE_MARKETING_NAME = auto()
    DEVICE_MOBILE_OS_HARDWARE_MODEL = auto()
    DEVICE_OPERATING_SYSTEM = auto()
    DEVICE_OPERATING_SYSTEM_VERSION = auto()
    DEVICE_VENDOR_ID = auto()
    DEVICE_ADVERTISING_ID = auto()
    DEVICE_LANGUAGE = auto()
    DEVICE_IS_LIMITED_AD_TRACKING = auto()
    DEVICE_TIME_ZONE_OFFSET_SECONDS = auto()
    DEVICE_BROWSER = auto()
    DEVICE_BROWSER_VERSION = auto()
    DEVICE_WEB_INFO_BROWSER = auto()
    DEVICE_WEB_INFO_BROWSER_VERSION = auto()
    DEVICE_WEB_INFO_HOSTNAME = auto()
    GEO_CONTINENT = auto()
    GEO_COUNTRY = auto()
    GEO_REGION = auto()
    GEO_CITY = auto()
    GEO_SUB_CONTINENT = auto()
    GEO_METRO = auto()
    APP_INFO_ID = auto()
    APP_INFO_VERSION = auto()
    APP_INFO_INSTALL_STORE = auto()
    APP_INFO_FIREBASE_APP_ID = auto()
    APP_INFO_INSTALL_SOURCE = auto()
    TRAFFIC_SOURCE_NAME = auto()
    TRAFFIC_SOURCE_MEDIUM = auto()
    TRAFFIC_SOURCE_SOURCE = auto()
    STREAM_ID = auto()
    PLATFORM = auto()
    EVENT_DIMENSIONS_HOSTNAME = auto()
    ECOMMERCE_TOTAL_ITEM_QUANTITY = auto()
    ECOMMERCE_PURCHASE_REVENUE_IN_USD = auto()
    ECOMMERCE_PURCHASE_REVENUE = auto()
    ECOMMERCE_REFUND_VALUE_IN_USD = auto()
    ECOMMERCE_REFUND_VALUE = auto()
    ECOMMERCE_SHIPPING_VALUE_IN_USD = auto()
    ECOMMERCE_SHIPPING_VALUE = auto()
    ECOMMERCE_TAX_VALUE_IN_USD = auto()
    ECOMMERCE_TAX_VALUE = auto()
    ECOMMERCE_UNIQUE_ITEMS = auto()
    ECOMMERCE_TRANSACTION_ID = auto()
    ITEMS = auto()
    COLLECTED_TRAFIC_SOURCE_MANUAL_CAMPAIGN_ID = auto()
    COLLECTED_TRAFIC_SOURCE_MANUAL_CAMPAIGN_NAME = auto()
    COLLECTED_TRAFIC_SOURCE_MANUAL_SOURCE = auto()
    COLLECTED_TRAFIC_SOURCE_MANUAL_MEDIUM = auto()
    COLLECTED_TRAFIC_SOURCE_MANUAL_TERM = auto()
    COLLECTED_TRAFIC_SOURCE_MANUAL_CONTENT = auto()
    COLLECTED_TRAFIC_SOURCE_GCLID = auto()
    COLLECTED_TRAFIC_SOURCE_DCLID = auto()
    COLLECTED_TRAFIC_SOURCE_SRSLTID = auto()


def get_english_label_for_ga4prop(prop: Ga4Prop) -> str:
    english_labels = {
        Ga4Prop.EVENT_DATE: "Event Date",
        Ga4Prop.EVENT_TIMESTAMP: "Event Timestamp",
        Ga4Prop.EVENT_NAME: "Event Name",
        Ga4Prop.EVENT_PARAMS: "Event Parameters",
        Ga4Prop.EVENT_PREVIOUS_TIMESTAMP: "Event Previous Timestamp",
        Ga4Prop.EVENT_VALUE_IN_USD: "Event Value in USD",
        Ga4Prop.EVENT_BUNDLE_SEQUENCE_ID: "Event Bundle Sequence ID",
        Ga4Prop.EVENT_SERVER_TIMESTAMP_OFFSET: "Event Server Timestamp Offset",
        Ga4Prop.USER_ID: "User ID",
        Ga4Prop.USER_PSEUDO_ID: "User Pseudo ID",
        Ga4Prop.PRIVACY_INFO_ANALYTICS_STORAGE: "Privacy Info Analytics Storage",
        Ga4Prop.PRIVACY_INFO_ADS_STORAGE: "Privacy Info Ads Storage",
        Ga4Prop.PRIVACY_INFO_USES_TRANSIENT_TOKEN: "Privacy Info Uses Transient Token",
        Ga4Prop.USER_PROPERTIES: "User Properties",
        Ga4Prop.USER_FIRST_TOUCH_TIMESTAMP: "User First Touch Timestamp",
        Ga4Prop.USER_LTV_REVENUE: "User LTV Revenue",
        Ga4Prop.USER_LTV_CURRENCY: "User LTV Currency",
        Ga4Prop.DEVICE_CATEGORY: "Device Category",
        Ga4Prop.DEVICE_MOBILE_BRAND_NAME: "Device Mobile Brand Name",
        Ga4Prop.DEVICE_MOBILE_MODEL_NAME: "Device Mobile Model Name",
        Ga4Prop.DEVICE_MOBILE_MARKETING_NAME: "Device Mobile Marketing Name",
        Ga4Prop.DEVICE_MOBILE_OS_HARDWARE_MODEL: "Device Mobile OS Hardware Model",
        Ga4Prop.DEVICE_OPERATING_SYSTEM: "Device Operating System",
        Ga4Prop.DEVICE_OPERATING_SYSTEM_VERSION: "Device Operating System Version",
        Ga4Prop.DEVICE_VENDOR_ID: "Device Vendor ID",
        Ga4Prop.DEVICE_ADVERTISING_ID: "Device Advertising ID",
        Ga4Prop.DEVICE_LANGUAGE: "Device Language",
        Ga4Prop.DEVICE_IS_LIMITED_AD_TRACKING: "Device Is Limited Ad Tracking",
        Ga4Prop.DEVICE_TIME_ZONE_OFFSET_SECONDS: "Device Time Zone Offset Seconds",
        Ga4Prop.DEVICE_BROWSER: "Device Browser",
        Ga4Prop.DEVICE_BROWSER_VERSION: "Device Browser Version",
        Ga4Prop.DEVICE_WEB_INFO_BROWSER: "Device Web Info Browser",
        Ga4Prop.DEVICE_WEB_INFO_BROWSER_VERSION: "Device Web Info Browser Version",
        Ga4Prop.DEVICE_WEB_INFO_HOSTNAME: "Device Web Info Hostname",
        Ga4Prop.GEO_CONTINENT: "Geo Continent",
        Ga4Prop.GEO_COUNTRY: "Geo Country",
        Ga4Prop.GEO_REGION: "Geo Region",
        Ga4Prop.GEO_CITY: "Geo City",
        Ga4Prop.GEO_SUB_CONTINENT: "Geo Sub Continent",
        Ga4Prop.GEO_METRO: "Geo Metro",
        Ga4Prop.APP_INFO_ID: "App Info ID",
        Ga4Prop.APP_INFO_VERSION: "App Info Version",
        Ga4Prop.APP_INFO_INSTALL_STORE: "App Info Install Store",
        Ga4Prop.APP_INFO_FIREBASE_APP_ID: "App Info Firebase App ID",
        Ga4Prop.APP_INFO_INSTALL_SOURCE: "App Info Install Source",
        Ga4Prop.TRAFFIC_SOURCE_NAME: "Traffic Source Name",
        Ga4Prop.TRAFFIC_SOURCE_MEDIUM: "Traffic Source Medium",
        Ga4Prop.TRAFFIC_SOURCE_SOURCE: "Traffic Source Source",
        Ga4Prop.STREAM_ID: "Stream ID",
        Ga4Prop.PLATFORM: "Platform",
        Ga4Prop.EVENT_DIMENSIONS_HOSTNAME: "Event Dimensions Hostname",
        Ga4Prop.ECOMMERCE_TOTAL_ITEM_QUANTITY: "Ecommerce Total Item Quantity",
        Ga4Prop.ECOMMERCE_PURCHASE_REVENUE_IN_USD: "Ecommerce Purchase Revenue in USD",
        Ga4Prop.ECOMMERCE_PURCHASE_REVENUE: "Ecommerce Purchase Revenue",
        Ga4Prop.ECOMMERCE_REFUND_VALUE_IN_USD: "Ecommerce Refund Value in USD",
        Ga4Prop.ECOMMERCE_REFUND_VALUE: "Ecommerce Refund Value",
        Ga4Prop.ECOMMERCE_SHIPPING_VALUE_IN_USD: "Ecommerce Shipping Value in USD",
        Ga4Prop.ECOMMERCE_SHIPPING_VALUE: "Ecommerce Shipping Value",
        Ga4Prop.ECOMMERCE_TAX_VALUE_IN_USD: "Ecommerce Tax Value in USD",
        Ga4Prop.ECOMMERCE_TAX_VALUE: "Ecommerce Tax Value",
        Ga4Prop.ECOMMERCE_UNIQUE_ITEMS: "Ecommerce Unique Items",
        Ga4Prop.ECOMMERCE_TRANSACTION_ID: "Ecommerce Transaction ID",
        Ga4Prop.ITEMS: "Items",
        Ga4Prop.COLLECTED_TRAFIC_SOURCE_MANUAL_CAMPAIGN_ID: "Collected Traffic Source Manual Campaign ID",
        Ga4Prop.COLLECTED_TRAFIC_SOURCE_MANUAL_CAMPAIGN_NAME: "Collected Traffic Source Manual Campaign Name",
        Ga4Prop.COLLECTED_TRAFIC_SOURCE_MANUAL_SOURCE: "Collected Traffic Source Manual Source",
        Ga4Prop.COLLECTED_TRAFIC_SOURCE_MANUAL_MEDIUM: "Collected Traffic Source Manual Medium",
        Ga4Prop.COLLECTED_TRAFIC_SOURCE_MANUAL_TERM: "Collected Traffic Source Manual Term",
        Ga4Prop.COLLECTED_TRAFIC_SOURCE_MANUAL_CONTENT: "Collected Traffic Source Manual Content",
        Ga4Prop.COLLECTED_TRAFIC_SOURCE_GCLID: "Collected Traffic Source GCLID",
        Ga4Prop.COLLECTED_TRAFIC_SOURCE_DCLID: "Collected Traffic Source DCLID",
        Ga4Prop.COLLECTED_TRAFIC_SOURCE_SRSLTID: "Collected Traffic Source SRSLTID",
    }
    return english_labels[prop]
