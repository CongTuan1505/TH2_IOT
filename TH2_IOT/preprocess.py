import numpy as np
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.preprocessing import MinMaxScaler

# THÔNG TIN CẤU HÌNH INFLUXDB CLOUD CỦA BẠN
INFLUX_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = "_9cD5cUvca7FSkIlPHyY5pOcuFhEcz2ggh2VXmapgPvkCDLHcflgEbCf5dhkJ30D4X6wBCTDDVULlbKtWaaMnw=="
INFLUX_ORG = "ptit_iot"
BUCKET_RAW = "iot_raw"
BUCKET_PROCESSED = "iot_processed"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()
write_api = client.write_api(write_options=SYNCHRONOUS)

def fetch_raw_data(time_range="-15m"):
    """Truy vấn dữ liệu thô trong 15 phút gần nhất"""
    query = f'''
    from(bucket: "{BUCKET_RAW}")
      |> range(start: {time_range})
      |> filter(fn: (r) => r["_measurement"] == "sensor_telemetry")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    df = query_api.query_data_frame(org=INFLUX_ORG, query=query)
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True) if len(df) > 0 else pd.DataFrame()
    return df

def clean_and_process(df):
    if df.empty or len(df) < 5:
        print("[THÔNG BÁO] Chưa đủ dữ liệu để tiền xử lý (cần tối thiểu 5 bản ghi). Hãy chờ collector thu thập thêm.")
        return None

    # Đưa thời gian về dạng DatetimeIndex chuẩn
    df['_time'] = pd.to_datetime(df['_time'])
    df = df.set_index('_time').sort_index()
    work_df = df[['temperature', 'humidity']].astype(float).copy()

    # 1. Xử lý Missing values bằng nội suy thời gian
    work_df = work_df.interpolate(method='time').ffill().bfill()

    # 2. Xử lý Outlier bằng IQR (Interquartile Range)
    for col in ['temperature', 'humidity']:
        q1 = work_df[col].quantile(0.25)
        q3 = work_df[col].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        work_df[col] = np.clip(work_df[col], lower_bound, upper_bound)

    # 3. Resampling: Gom mẫu theo chu kỳ 5 giây, tính giá trị trung bình
    resampled = work_df.resample('5s').mean().interpolate(method='time')

    # 4. Feature Engineering: Rolling mean và Độ chênh lệch delta
    resampled['rolling_mean_temp'] = resampled['temperature'].rolling(window=3, min_periods=1).mean()
    resampled['temp_delta'] = resampled['temperature'].diff().fillna(0)

    # 5. Chuẩn hóa dữ liệu Min-Max scaling về đoạn [0, 1]
    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(resampled[['temperature', 'humidity']])
    resampled['temp_norm'] = scaled[:, 0]
    resampled['hum_norm'] = scaled[:, 1]

    return resampled

def save_processed_data(processed_df):
    if processed_df is None or processed_df.empty:
        return

    points = []
    for timestamp, row in processed_df.iterrows():
        p = (
            Point("cleaned_sensor_telemetry")
            .tag("pipeline_status", "cleaned")
            .field("temperature", float(row['temperature']))
            .field("humidity", float(row['humidity']))
            .field("rolling_mean_temp", float(row['rolling_mean_temp']))
            .field("temp_delta", float(row['temp_delta']))
            .field("temp_norm", float(row['temp_norm']))
            .field("hum_norm", float(row['hum_norm']))
            .time(timestamp, WritePrecision.NS)
        )
        points.append(p)

    write_api.write(bucket=BUCKET_PROCESSED, org=INFLUX_ORG, record=points)
    print(f"[THÀNH CÔNG] Đã tiền xử lý và lưu {len(points)} bản ghi vào bucket '{BUCKET_PROCESSED}'!")

if __name__ == "__main__":
    print("Đang đọc dữ liệu thô từ InfluxDB...")
    raw = fetch_raw_data()
    processed = clean_and_process(raw)
    save_processed_data(processed)