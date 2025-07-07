import os
import requests
import csv
from io import StringIO
from datetime import datetime, timedelta, timezone
from clickhouse_driver import Client
import re

# -- Config thông tin dùng chung --
APPSFLYER_TOKEN = os.environ.get('APPSFLYER_TOKEN')
CH_HOST = os.environ.get('CH_HOST')
CH_PORT = int(os.environ.get('CH_PORT', 9000))
CH_USER = os.environ.get('CH_USER')
CH_PASSWORD = os.environ.get('CH_PASSWORD')
CH_DATABASE = os.environ.get('CH_DATABASE')
CH_TABLE = os.environ.get('CH_TABLE', 'install')

# -- List App IDs (thêm app tại đây) --
APP_IDS = [
    "vn.ghn.app.giaohangnhanh",
    "id1203171490"
]

# -- Map AppsFlyer header sang ClickHouse --
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

DATETIME_COLUMNS = {
    'attributed_touch_time','install_time','event_time',
    'contributor_1_touch_time','contributor_2_touch_time','contributor_3_touch_time','device_download_time'
}

def get_vn_time_range(hours=2):
    now_utc = datetime.now(timezone.utc)
    now_vn = now_utc + timedelta(hours=7)
    to_time = now_vn
    from_time = to_time - timedelta(hours=hours)
    return from_time.strftime('%Y-%m-%d %H:%M:%S'), to_time.strftime('%Y-%m-%d %H:%M:%S')

def parse_datetime(val):
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ('', 'null', 'none', 'n/a'):
        return None
    # Xử lý về đúng định dạng ClickHouse (không có .000)
    m = re.match(r"^(\d{4}-\d{2}-\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.\d+)?$", s)
    if m:
        date_part, hour, minute, second = m.groups()
        return f"{date_part} {hour}:{minute}:{second}"
    print(f"⚠️ DateTime sai định dạng: '{val}' -> set None")
    return None

def process_app(app_id):
    from_time, to_time = get_vn_time_range(2)
    print(f"\n---- Đang xử lý app_id: {app_id} ----")
    url = (
        f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/installs_report/v5"
        f"?from={from_time}&to={to_time}&timezone=Asia%2FHo_Chi_Minh"
        f"&additional_fields={ADDITIONAL_FIELDS}"
    )
    headers = {"Authorization": APPSFLYER_TOKEN, "accept": "text/csv"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print("❌ Error:", resp.text)
        return

    csvfile = StringIO(resp.text)
    reader = csv.DictReader(csvfile)
    fieldnames = reader.fieldnames
    if fieldnames[0].startswith('\ufeff'):
        fieldnames[0] = fieldnames[0].replace('\ufeff', '')

    ch_columns = [APPSFLYER_TO_CH[h] for h in fieldnames if h in APPSFLYER_TO_CH]

    # Query ClickHouse lấy các appsflyer_id đã có
    client = Client(
        host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASSWORD, database=CH_DATABASE
    )
    result = client.execute(
        f"SELECT appsflyer_id FROM {CH_TABLE} WHERE install_time >= '{from_time}' AND install_time <= '{to_time}'"
    )
    existed = set(str(r[0]) for r in result if r[0])

    # Chuẩn bị data để insert (chỉ lấy các row mới)
    rows_to_insert = []
    for row in reader:
        if not row.get("AppsFlyer ID"):
            continue
        if row["AppsFlyer ID"] in existed:
            continue
        record = []
        for col in fieldnames:
            col_ch = APPSFLYER_TO_CH.get(col)
            value = row[col]
            if col_ch in DATETIME_COLUMNS:
                record.append(parse_datetime(value))
            else:
                record.append(value if value not in ('', 'null', None) else None)
        rows_to_insert.append(record)

    print(f"🆕 {len(rows_to_insert)} dòng mới sẽ insert vào {CH_TABLE}")
    if rows_to_insert:
        client.execute(
            f"INSERT INTO {CH_TABLE} ({', '.join(ch_columns)}) VALUES",
            rows_to_insert
        )
        print(f"✅ Đã insert {len(rows_to_insert)} dòng vào ClickHouse cho app_id: {app_id}")
    else:
        print("Không có dòng mới để insert cho app_id:", app_id)

def main():
    for app_id in APP_IDS:
        process_app(app_id)

if __name__ == "__main__":
    main()
