

import json
import time

from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# 1. Kafka topic
TOPIC = "campus-events"

# 2. Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all"
)

# 3. Generate base time in UTC
base = datetime.now(timezone.utc).replace(microsecond=0)

# 4. Define synthetic events
#events = [
    #("S101", "check-in", "Computer Science", 2),
    #("S204", "check-in", "Mathematics", 8),
    #("S101", "open-course", "Computer Science", 15),
    #("S305", "check-in", "Physics", 22),
   # ("S411", "check-in", "Computer Science", 10),
    #("S411", "check-in", "Computer Science", 35),
  #  ("S204", "check-in", "Mathematics", 48),
 #   ("S512", "check-in", "Physics", 12)
#]
events = [
    ("S101", "check-in", "Computer Science"),
    ("S204", "check-in", "Mathematics"),
    ("S101", "open-course", "Computer Science"),
    ("S305", "check-in", "Physics"),
    ("S411", "check-in", "Computer Science"),
    ("S204", "check-in", "Mathematics"),
    ("S512", "check-in", "Physics"),
    ("S601", "library-entry", "Computer Science")
]
total_events = 100
start_time = time.perf_counter()
# 5. Send events to Kafka
try:

    #for event_id, (student, event_type, dept, seconds) in enumerate(events, 1):

     #   event_time = base + timedelta(seconds=seconds)
    for event_id in range(1, total_events + 1):

        student, event_type, dept = events[(event_id - 1) % len(events)]

        event_time = base + timedelta(milliseconds=event_id * 100)
        event = {
            "eventId": event_id,
            "studentId": student,
            "eventType": event_type,
            "department": dept,
            "eventTime": event_time.isoformat()
        }

        producer.send(TOPIC, value=event)

        print("Sent:", event)

        time.sleep(1)

    # Wait for all messages to be sent
    producer.flush()
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    ingestion_rate = total_events / elapsed_time
finally:

    producer.close()

print("All events sent successfully!")
print("Total events:", total_events)
print("Sending duration:", round(elapsed_time, 3), "seconds")
print("Ingestion rate:", round(ingestion_rate, 2), "events/second")
