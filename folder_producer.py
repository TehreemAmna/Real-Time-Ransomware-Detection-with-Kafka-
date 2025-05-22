from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka import KafkaProducer
import time, json, os

producer = KafkaProducer(bootstrap_servers='localhost:9092')

class Handler(FileSystemEventHandler):
    def on_any_event(self, event):
        if event.is_directory: return
        data = {
            'event': event.event_type,
            'file': event.src_path,
            'timestamp': time.time()
        }
        producer.send('file-events', json.dumps(data).encode())
        print("[+] Event:", data)

path = os.path.join(os.path.expanduser('~'), 'Music', 'BIG DATA', 'watched_folder')
observer = Observer()
observer.schedule(Handler(), path, recursive=True)
observer.start()
print("[*] Monitoring...")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
