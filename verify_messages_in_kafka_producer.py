from kafka import KafkaConsumer, KafkaAdminClient
import json
from datetime import datetime

# Configuration
BOOTSTRAP_SERVERS = 'kafka:9092'  # Adjust if needed
OUTPUT_FILE = 'kafka_topic_report.txt'

def list_topics():
    """List all non-system Kafka topics."""
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topics = [topic for topic in admin_client.list_topics() if not topic.startswith('__')]
    admin_client.close()
    return topics

def count_messages_in_topic(topic):
    """Count messages in a topic efficiently."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=2000
    )
    count = sum(1 for _ in consumer)
    consumer.close()
    return count

def get_first_messages(topic, num_messages=2):
    """Fetch first N messages from a topic."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=2000
    )
    messages = []
    for i, msg in enumerate(consumer, 1):
        messages.append(msg.value)
        if i >= num_messages:
            break
    consumer.close()
    return messages

def generate_report():
    """Generate and save a report of topics, counts, and sample messages."""
    topics = list_topics()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with open(OUTPUT_FILE, 'a') as f:
        f.write(f"Kafka Topic Report | Generated at: {timestamp}\n")
        print(f"Kafka Topic Report | Generated at: {timestamp}\n")
        f.write(f"Bootstrap Server: {BOOTSTRAP_SERVERS}\n")
        f.write("=" * 50 + "\n\n")
        
        for topic in topics:
            count = count_messages_in_topic(topic)
            f.write(f"Topic: {topic}\n")
            print(f"Topic: {topic}")
            f.write(f"Message Count: {count}\n")
            print(f"Message Count: {count}")
            
            # if count > 0:
            #     messages = get_first_messages(topic)
            #     f.write("First 2 Messages:\n")
            #     for i, msg in enumerate(messages, 1):
            #         f.write(f"{i}. {json.dumps(msg, indent=2)}\n")
            # else:
            #     f.write("No messages found.\n")
            # f.write("-" * 50 + "\n")
            
            # Also print to console (optional)
            print(f"Processed topic: {topic}\n")
    
    print(f"\nReport saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_report()
