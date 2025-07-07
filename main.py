import os
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta, timezone
from clickhouse_driver import Client

# --- Config ---
import os

APPSFLYER_TOKEN = os.environ.get('APPSFLYER_TOKEN')
APP_ID = os.environ.get('APP_ID')

CH_HOST = os.environ.get('CH_HOST')
CH_PORT = int(os.environ.get('CH_PORT', 9000))
CH_USER = os.environ.get('CH_USER')
CH_PASSWORD = os.environ.get('CH_PASSWORD')
CH_DATABASE = os.environ.get('CH_DATABASE')
CH_TABLE = os.environ.get('CH_TABLE')

APPSFLYER_TO_CH = {
    "Attributed Touch Type": "attributed_touch_type",
    "Attributed Touch Time": "attributed_touch_time",
    "Install Time": "install_time",
    "Event Time": "event_time",
    "Event Name": "event_name",
    "Partner": "partner",
    "Media Source": "media_source",
    "Campaign": "campaign",
    "Adset": "adset",
    "Ad": "ad",
    "Ad Type": "ad_type",
    "Contributor 1 Touch Type": "contributor_1_touch_type",
    "Contributor 1 Touch Time": "contributor_1_touch_time",
    "Contributor 1 Partner": "contributor_1_partner",
    "Contributor 1 Match Type": "contributor_1_match_type",
    "Contributor 1 Media Source": "contributor_1_media_source",
    "Contributor 1 Campaign": "contributor_1_campaign",
    "Contributor 1 Engagement Type": "contributor_1_engagement_type",
    "Contributor 2 Touch Type": "contributor_2_touch_type",
    "Contributor 2 Touch Time": "contributor_2_touch_time",
    "Contributor 2 Partner": "contributor_2_partner",
    "Contributor 2 Media Source": "contributor_2_media_source",
    "Contributor 2 Campaign": "contributor_2_campaign",
    "Contributor 2 Match Type": "contributor_2_match_type",
    "Contributor 2 Engagement Type": "contributor_2_engagement_type",
    "Contributor 3 Touch Type": "contributor_3_touch_type",
    "Contributor 3 Touch Time": "contributor_3_touch_time",
    "Contributor 3 Partner": "contributor_3_partner",
    "Contributor 3 Media Source": "contributor_3_media_source",
    "Contributor 3 Campaign": "contributor_3_campaign",
    "Contributor 3 Match Type": "contributor_3_match_type",
    "Contributor 3 Engagement Type": "contributor_3_engagement_type",
    "City": "city",
    "IP": "ip",
    "AppsFlyer ID": "appsflyer_id",
    "Customer User ID": "customer_user_id",
    "IDFA": "idfa",
    "IDFV": "idfv",
    "Device Category": "device_category",
    "Platform": "platform",
    "OS Version": "os_version",
    "Bundle ID": "bundle_id",
    "Is Retargeting": "is_retargeting",
    "Attribution Lookback": "attribution_lookback",
    "Match Type": "match_type",
    "Device Download Time": "device_download_time",
    "Device Model": "device_model",
    "Engagement Type": "engagement_type"
}
ADDITIONAL_FIELDS = (
    'blocked_reason_rule,store_reinstall,impressions,contributor3_match_type,custom_dimension,conversion_type,'
    'gp_click_time,match_type,mediation_network,oaid,deeplink_url,blocked_reason,blocked_sub_reason,'
    'gp_broadcast_referrer,gp_install_begin,campaign_type,custom_data,rejected_reason,device_download_time,'
    'keyword_match_type,contributor1_match_type,contributor2_match_type,device_model,monetization_network,'
    'segment,is_lat,gp_referrer,blocked_reason_value,store_product_page,device_category,app_type,'
    'rejected_reason_value,ad_unit,keyword_id,placement,network_account_id,install_app_store,amazon_aid,att,'
    'engagement_type,gdpr_applies,ad_user_data_enabled,ad_personalization_enabled'
)

def get_vn_time_range(hours=2):
    now_utc = datetime.now(timezone.utc)
    now_vn = now_utc + timedelta(hours=7)
    to_time = now_vn
    from_time = to_time - timedelta(hours=hours)
    return from_time.strftime('%Y-%m-%d %H:%M:%S'), to_time.strftime('%Y-%m-%d %H:%M:%S')

def download_appsflyer_installs(from_time, to_time):
    # AppsFlyer API chỉ nhận yyyy-mm-dd HH:MM:SS, nhưng khuyên dùng ISO8601
    # Để chắc chắn nên lấy data từ start to end (timezone Asia/Ho_Chi_Minh)
    url = (
        f"https://hq1.appsflyer.com/api/raw-data/export/app/{APP_ID}/installs_report/v5"
        f"?from={from_time}&to={to_time}&timezone=Asia%2FHo_Chi_Minh"
        f"&additional_fields={ADDITIONAL_FIELDS}"
    )
    headers = {"Authorization": APPSFLYER_TOKEN, "accept": "text/csv"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print("❌ Error:", resp.text)
        return None
    csvfile = StringIO(resp.text)
    df = pd.read_csv(csvfile)
    # Chuẩn hóa header BOM
    df.rename(columns=lambda x: x.strip('\ufeff'), inplace=True)
    return df

def main():
    from_time, to_time = get_vn_time_range(2)
    print(f"🕒 Đang lấy data AppsFlyer từ {from_time} đến {to_time} (Asia/Ho_Chi_Minh)")
    df = download_appsflyer_installs(from_time, to_time)
    if df is None or df.empty:
        print("⚠️ Không có data AppsFlyer trong khoảng này.")
        return

    # Chỉ lấy đúng các cột map cho ClickHouse
    col_map = {h: APPSFLYER_TO_CH[h] for h in df.columns if h in APPSFLYER_TO_CH}
    df = df[list(col_map.keys())]
    df.rename(columns=col_map, inplace=True)
    ch_columns = list(df.columns)

    # Query ClickHouse để lấy các appsflyer_id đã có trong khoảng from_time → to_time
    client = Client(
        host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASSWORD, database=CH_DATABASE
    )
    result = client.execute(
        f"SELECT appsflyer_id FROM {CH_TABLE} WHERE install_time >= '{from_time}' AND install_time <= '{to_time}'"
    )
    existed = set(str(r[0]) for r in result if r[0])
    print(f"🔎 Có {len(existed)} ID đã tồn tại trong ClickHouse.")

    df_new = df[~df['appsflyer_id'].astype(str).isin(existed)]
    print(f"➕ Số dòng mới sẽ insert: {len(df_new)}")

    if not df_new.empty:
        rows_to_insert = df_new.where(pd.notnull(df_new), None).values.tolist()
        client.execute(
            f"INSERT INTO {CH_TABLE} ({', '.join(ch_columns)}) VALUES",
            rows_to_insert
        )
        print("✅ Đã insert lên ClickHouse xong!")
    else:
        print("Không có dòng mới để insert.")

if __name__ == "__main__":
    main()
