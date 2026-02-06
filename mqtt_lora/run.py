import json
import os
import paho.mqtt.client as mqtt
from threading import Timer

# Load options provided by Home Assistant Supervisor
OPTIONS_PATH = "/data/options.json"
with open(OPTIONS_PATH, "r") as f:
    options = json.load(f)

MQTT_HOST = options["mqtt_host"]
MQTT_PORT = int(options["mqtt_port"])
MQTT_USER = options["mqtt_user"]
MQTT_PASSWORD = options["mqtt_password"]
TOPIC_PREFIX = options["topic_prefix"]
DEBOUNCE_MS = int(options["debounce_ms"])
EXCLUDE_KEYS = set(options.get("exclude_keys", []))

CACHE_FILE = "/data/cache.json"
cache = {}
if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE, "r") as f:
            cache = json.load(f)
    except Exception:
        cache = {}

debounce_timers = {}

def debounce_publish(key, func):
    """Debounce state publishes per entity key."""
    t = debounce_timers.get(key)
    if t:
        t.cancel()
    timer = Timer(DEBOUNCE_MS / 1000.0, func)
    debounce_timers[key] = timer
    timer.start()

def snake(s: str) -> str:
    out = []
    for c in s:
        if c.isupper():
            out.append('_')
            out.append(c.lower())
        else:
            out.append(c)
    return ''.join(out).lstrip('_')

def save_cache():
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"Failed to save cache: {e}")

def parse_value(value_obj):
    """Return (domain, state_value, unit) from a value object."""
    unit = value_obj.get("unit") or None
    if "valueBoolean" in value_obj:
        return ("binary_sensor", bool(value_obj["valueBoolean"]), unit)
    if "valueNumber" in value_obj:
        return ("sensor", value_obj["valueNumber"], unit)
    if "valueString" in value_obj:
        return ("sensor", value_obj["valueString"], unit)
    # Unknown type
    return (None, None, None)

def publish_entity_config(client, device_id, key, value_obj, device_info):
    entity_key = snake(key)
    unique = f"{device_id}_{entity_key}"

    domain, state_value, unit = parse_value(value_obj)
    if domain is None:
        return None, None

    # Force domain to binary_sensor for error info flags
    info_type = value_obj.get("informationType", "")
    if info_type == "ERROR_INFORMATION":
        domain = "binary_sensor"

    config_topic = f"homeassistant/{domain}/{device_id}/{entity_key}/config"

    if cache.get(config_topic):
        return domain, state_value

    config_payload = {
        "name": entity_key.replace('_', ' ').title(),
        "state_topic": f"homeassistant/{domain}/{device_id}/{entity_key}/state",
        "unique_id": unique,
        "device": device_info,
        "force_update": True
    }
    if unit:
        config_payload["unit_of_measurement"] = unit

    client.publish(config_topic, json.dumps(config_payload), retain=True)
    cache[config_topic] = True
    save_cache()
    return domain, state_value

def publish_state(client, domain, device_id, entity_key, state_value):
    state_topic = f"homeassistant/{domain}/{device_id}/{entity_key}/state"
    client.publish(state_topic, str(state_value))

# MQTT callbacks

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with rc={rc}")
    client.subscribe(f"{TOPIC_PREFIX}#")
    print(f"Subscribed to {TOPIC_PREFIX}#")


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload)
    except Exception:
        return

    device = data.get("sensor", {})
    payload = data.get("message", {})
    device_id = device.get("deviceId")
    if not device_id:
        return

    device_info = {
        "identifiers": [device_id],
        "name": device.get("alias", device_id),
        "manufacturer": "MQTT/LoRa",
        "model": device.get("type"),
    }

    for key, value_obj in payload.items():
        if key in EXCLUDE_KEYS:
            continue
        # Create/ensure config
        domain, state_value = publish_entity_config(client, device_id, key, value_obj, device_info)
        if state_value is None or domain is None:
            continue
        entity_key = snake(key)

        def do_pub():
            publish_state(client, domain, device_id, entity_key, state_value)

        debounce_publish(f"{device_id}_{entity_key}", do_pub)

# Start MQTT client
client = mqtt.Client()
if MQTT_USER:
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_HOST, MQTT_PORT)
client.loop_forever()
