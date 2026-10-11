import json
import time
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# CẤU HÌNH INFLUXDB CLOUD
INFLUX_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"  # Thay bằng URL Cloud của bạn
INFLUX_TOKEN = "_9cD5cUvca7FSkIlPHyY5pOcuFhEcz2ggh2VXmapgPvkCDLHcflgEbCf5dhkJ30D4X6wBCTDDVULlbKtWaaMnw=="
INFLUX_ORG = "ptit_iot"
INFLUX_BUCKET_RAW = "iot_raw"

# CẤU HÌNH MQTT BROKER
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "ptit/iot/sensor_dht22/data"

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

def validate_payload(data):
    required_keys = ["device_id", "seq", "temperature", "humidity"]
    if not all(k in data for k in required_keys):
        return False
    if not (-40.0 <= data["temperature"] <= 80.0):
        return False
    if not (0.0 <= data["humidity"] <= 100.0):
        return False
    return True

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Kết nối Broker thành công! Đang lắng nghe topic...")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"[MQTT] Lỗi kết nối Broker, mã lỗi: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        if not validate_payload(payload):
            print(f"[REJECTED] Gói tin không hợp lệ: {payload}")
            return

        point = (
            Point("sensor_telemetry")
            .tag("device_id", str(payload["device_id"]))
            .field("temperature", float(payload["temperature"]))
            .field("humidity", float(payload["humidity"]))
            .field("seq", int(payload["seq"]))
            .time(time.time_ns(), WritePrecision.NS)
        )
        
        write_api.write(bucket=INFLUX_BUCKET_RAW, org=INFLUX_ORG, record=point)
        print(f"[STORED] Seq: {payload['seq']} | Nhiệt độ: {payload['temperature']}°C | Độ ẩm: {payload['humidity']}%")

    except Exception as e:
        print(f"[ERROR] Lỗi xử lý: {e}")

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_forever()