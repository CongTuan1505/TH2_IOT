import json
import pandas as pd
from flask import Flask, render_template_string
from influxdb_client import InfluxDBClient

app = Flask(__name__)

INFLUX_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = "joegPaiqDAMKcJYHRvRGxSm7DMKy7oeracVT0fNG9KEi-_ddmW-9Nf0WsZ1CK8nb_n-lQ9J7KXCgbT83VDD7gw=="
INFLUX_ORG = "ptit_iot"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

def query_bucket():
    # Câu truy vấn Flux chuẩn: lấy 20 bản ghi gần nhất của nhiệt độ và độ ẩm
    query = '''
    from(bucket: "iot_raw")
      |> range(start: -1d)
      |> filter(fn: (r) => r["_measurement"] == "sensor_telemetry")
      |> filter(fn: (r) => r["_field"] == "temperature" or r["_field"] == "humidity" or r["_field"] == "seq")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"], desc: false)
      |> limit(n: 50)
    '''
    try:
        df = query_api.query_data_frame(org=INFLUX_ORG, query=query)
        if isinstance(df, list):
            df = pd.concat(df, ignore_index=True) if len(df) > 0 else pd.DataFrame()
        
        print(f"[DEBUG] Số dòng truy vấn được: {len(df)}")
        if not df.empty:
            print("[DEBUG] Các cột có trong DataFrame:", df.columns.tolist())
            df['_time'] = pd.to_datetime(df['_time']).dt.strftime('%H:%M:%S')
        return df
    except Exception as e:
        print(f"[LỖI QUERY INFLUXDB]: {e}")
        return pd.DataFrame()

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <title>IoT Dashboard - INT14149</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 20px 40px; background: #f8fafc; color: #1e293b; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #e2e8f0; padding-bottom: 12px; }
        .kpi-container { display: flex; gap: 20px; margin: 20px 0; }
        .kpi-card { flex: 1; background: #fff; padding: 18px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); border-left: 4px solid #3b82f6; }
        .kpi-card h3 { margin: 0 0 8px 0; font-size: 14px; color: #64748b; }
        .kpi-card .val { font-size: 28px; font-weight: bold; color: #0f172a; }
        .chart-box { background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 24px; }
        table { width: 100%; border-collapse: collapse; margin-top: 15px; background: #fff; border-radius: 8px; overflow: hidden; }
        th, td { padding: 10px 14px; text-align: left; border-bottom: 1px solid #e2e8f0; font-size: 14px; }
        th { background: #f1f5f9; color: #475569; }
        .btn-refresh { background: #2563eb; color: #fff; border: none; padding: 10px 18px; border-radius: 6px; cursor: pointer; font-weight: 500; }
        .btn-refresh:hover { background: #1d4ed8; }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <h2>BÀI THỰC HÀNH SỐ 2: HỆ THỐNG GIÁM SÁT & TIỀN XỬ LÝ DỮ LIỆU IoT</h2>
            <p style="margin: 4px 0; color: #64748b;">Môn: IoT và Ứng dụng (INT14149)</p>
        </div>
        <button class="btn-refresh" onclick="location.reload()">Làm mới dữ liệu (Refresh)</button>
    </div>

    <div class="kpi-container">
        <div class="kpi-card">
            <h3>Nhiệt độ hiện tại</h3>
            <div class="val">{{ latest_temp }} °C</div>
        </div>
        <div class="kpi-card" style="border-left-color: #06b6d4;">
            <h3>Độ ẩm hiện tại</h3>
            <div class="val">{{ latest_hum }} %</div>
        </div>
        <div class="kpi-card" style="border-left-color: #10b981;">
            <h3>Số bản ghi thu thập (Raw)</h3>
            <div class="val">{{ raw_count }}</div>
        </div>
    </div>

    <div class="chart-box">
        <h3>1. Đồ thị chuỗi thời gian Sensor DHT22 (Raw Telemetry)</h3>
        <canvas id="rawChart" height="90"></canvas>
    </div>

    <div class="chart-box">
        <h3>2. Bảng các bản ghi gần nhất thu thập từ MQTT</h3>
        <table>
            <thead>
                <tr>
                    <th>Thời gian</th>
                    <th>Device ID</th>
                    <th>Sequence</th>
                    <th>Nhiệt độ (°C)</th>
                    <th>Độ ẩm (%)</th>
                </tr>
            </thead>
            <tbody>
                {% for row in table_data %}
                <tr>
                    <td>{{ row.get('_time', '--') }}</td>
                    <td>{{ row.get('device_id', 'esp32_lab2') }}</td>
                    <td>{{ row.get('seq', '--') }}</td>
                    <td>{{ row.get('temperature', '--') }}</td>
                    <td>{{ row.get('humidity', '--') }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>

    <script>
        const labels = {{ chart_labels | tojson }};
        const tempData = {{ chart_temp | tojson }};
        const humData = {{ chart_hum | tojson }};

        new Chart(document.getElementById('rawChart'), {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Nhiệt độ (°C)',
                        data: tempData,
                        borderColor: '#ef4444',
                        backgroundColor: 'rgba(239, 68, 68, 0.1)',
                        tension: 0.3
                    },
                    {
                        label: 'Độ ẩm (%)',
                        data: humData,
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        tension: 0.3
                    }
                ]
            },
            options: { 
                responsive: true,
                scales: {
                    y: { beginAtZero: false }
                }
            }
        });
    </script>
</body>
</html>
"""

@app.route("/")
def index():
    raw_df = query_bucket()
    latest_temp = "--"
    latest_hum = "--"
    raw_count = len(raw_df)
    labels, temp_list, hum_list = [], [], []
    table_data = []

    if not raw_df.empty and 'temperature' in raw_df.columns and 'humidity' in raw_df.columns:
        latest = raw_df.iloc[-1]
        latest_temp = f"{float(latest.get('temperature', 0)):.1f}"
        latest_hum = f"{float(latest.get('humidity', 0)):.1f}"
        
        subset = raw_df.tail(25)
        labels = subset['_time'].tolist() if '_time' in subset.columns else list(range(len(subset)))
        temp_list = [float(x) for x in subset['temperature']]
        hum_list = [float(x) for x in subset['humidity']]
        table_data = subset.tail(10).to_dict(orient="records")

    return render_template_string(
        HTML_TEMPLATE,
        latest_temp=latest_temp,
        latest_hum=latest_hum,
        raw_count=raw_count,
        chart_labels=labels,
        chart_temp=temp_list,
        chart_hum=hum_list,
        table_data=table_data
    )

if __name__ == "__main__":
    print("[FLASK] Khởi động Dashboard tại: http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)