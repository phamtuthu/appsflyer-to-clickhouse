import requests
import csv
from io import StringIO
from clickhouse_driver import Client
import re
from datetime import datetime

# --- Config ---
APPSFLYER_TOKEN = 'Bearer YOUR_API_TOKEN'
APP_ID = 'vn.ghn.app.shiip'

CH_HOST = '103.232.122.212'
CH_PORT = 9000
CH_USER = 'default'
CH_PASSWORD = 'thinv'
CH_DATABASE = 'ghn_c2'
CH_TABLE = 'install'

# Header mapping: Appsflyer => ClickHouse
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

CH_COLUMNS = list(APPSFLYER_TO_CH.values())
DATETIME_COLUMNS = {
    'attributed_touch_time','install_time','event_time',
    'contributor_1_touch_time','contributor_2_touch_time','contributor_3_touch_time','device_download_time'
}

def parse_datetime(val):
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ('', 'null', 'none', 'n/a'):
        return None
    match = re.match(r"^(\d{4}-\d{2}-\d{2}) (\d{1,2}):(\d{2}):(\d{2})$", s)
    if match:
        date_part, hour, minute, second = match.groups()
        hour = hour.zfill(2)
        return f"{date_part} {hour}:{minute}:{second}"
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", s):
        return s
    print(f"⚠️ DateTime sai định dạng: '{val}' -> set NULL")
    return None

def get_today_str():
    # Lấy ngày hiện tại dạng yyyy-mm-dd theo giờ VN (UTC+7)
    now = datetime.utcnow()
    now_vn = now.timestamp() + 7*3600
    return datetime.fromtimestamp(now_vn).strftime('%Y-%m-%d')

def main():
    today = get_today_str()  # Lấy data ngày hiện tại
    url = f"https://hq1.appsflyer.com/api/raw-data/export/app/{APP_ID}/installs_report/v5?from={today}&to={today}&timezone=Asia%2FHo_Chi_Minh"
    headers = {"Authorization": APPSFLYER_TOKEN, "accept": "text/csv"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print("❌ Error download Appsflyer CSV:", resp.text)
        return

    csvfile = StringIO(resp.text)
    reader = csv.DictReader(csvfile)
    raw_headers = reader.fieldnames
    # Map cột Appsflyer sang ClickHouse
    header_map = [APPSFLYER_TO_CH.get(h, None) for h in raw_headers]
    col_idx_map = {ch_col: i for i, ch_col in enumerate(header_map) if ch_col}

    rows_to_insert = []
    for row in reader:
        record = []
        for col in CH_COLUMNS:
            idx = col_idx_map.get(col, None)
            val = row[raw_headers[idx]] if idx is not None else None
            if col in DATETIME_COLUMNS:
                record.append(parse_datetime(val))
            else:
                record.append(val if val not in ('', 'null', None) else None)
        rows_to_insert.append(record)

    print(f"Inserting {len(rows_to_insert)} rows to ClickHouse...")
    if not rows_to_insert:
        print("No data to insert.")
        return

    client = Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE
    )
    client.execute(
        f"INSERT INTO {CH_TABLE} ({', '.join(CH_COLUMNS)}) VALUES",
        rows_to_insert
    )
    print("✅ Done!")

if __name__ == "__main__":
    main()
