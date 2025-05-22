from kafka import KafkaConsumer
import json, time, logging, subprocess, os

# Setup logging
log_file = os.path.join(os.path.expanduser("~"), "Music", "BIG DATA", "ransomware_detection.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')

consumer = KafkaConsumer('file-events', bootstrap_servers='localhost:9092')
events = []
THRESHOLD = 10  # number of suspicious events
WINDOW = 5      # seconds

print("[*] Consumer listening...")

for msg in consumer:
    try:
        e = json.loads(msg.value.decode())
        print(f"[-] {e['event']} on {e['file']}")
        logging.info(f"{e['event']} on {e['file']}")
        events.append(e)

        now = time.time()
        events = [x for x in events if now - x['timestamp'] <= WINDOW]

        if len(events) > THRESHOLD:
            alert_msg = "[!!!] ALERT: Ransomware behavior detected!"
            print(alert_msg)
            logging.warning(alert_msg)

            # Show desktop notification
            subprocess.Popen(['notify-send', 'Ransomware Alert', 'Suspicious file activity detected!'])

            # Play alert sound
            subprocess.Popen(['play', '-nq', '-t', 'alsa', 'synth', '0.5', 'sine', '880'])

            events.clear()
    except Exception as err:
        print(f"[!] Error: {err}")
        logging.error(f"Error processing event: {err}")
