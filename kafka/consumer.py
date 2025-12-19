# Write your kafka consumer debugger code here 
from kafka import KafkaConsumer
import json
from datetime import datetime


class TransactionConsumer:
    """
    Simple Kafka consumer for debugging and verification.
    """

    def __init__(self, bootstrap_servers='localhost:9092', topic='transactions', group_id='debug-consumer'):
        """
        Initialize Kafka consumer.

        Args:
            bootstrap_servers: Kafka broker address
            topic: Kafka topic name
            group_id: Consumer group ID
        """
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',  # Start from beginning
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None
        )
        print(f"✓ Kafka Consumer initialized for topic: {topic}")
        print(f"  Group ID: {group_id}")
        print(f"  Starting from: earliest messages\n")

    def consume_messages(self, max_messages=None, timeout_ms=5000):
        """
        Consume and display messages from Kafka.

        Args:
            max_messages: Maximum number of messages to consume (None = infinite)
            timeout_ms: Timeout for polling
        """
        message_count = 0

        try:
            print("📥 Consuming messages (Press Ctrl+C to stop)...\n")
            print("-" * 80)

            for message in self.consumer:
                message_count += 1

                # Extract message details
                key = message.key
                value = message.value
                partition = message.partition
                offset = message.offset
                timestamp = datetime.fromtimestamp(message.timestamp / 1000)

                # Display message
                print(f"Message #{message_count}")
                print(f"  Key: {key}")
                print(f"  Value: {json.dumps(value, indent=4)}")
                print(f"  Partition: {partition} | Offset: {offset}")
                print(f"  Timestamp: {timestamp}")
                print("-" * 80)

                # Stop if max messages reached
                if max_messages and message_count >= max_messages:
                    print(f"\n✓ Reached maximum of {max_messages} messages")
                    break

        except KeyboardInterrupt:
            print(f"\n\n✓ Stopped by user. Consumed {message_count} messages.")
        except Exception as e:
            print(f"\n❌ Error consuming messages: {e}")
            raise
        finally:
            self.consumer.close()
            print("✓ Consumer closed")

    def get_topic_info(self):
        """
        Display information about the topic.
        """
        partitions = self.consumer.partitions_for_topic(self.topic)
        print(f"\n📊 Topic Information:")
        print(f"  Topic: {self.topic}")
        print(f"  Partitions: {partitions}")


def main():
    """
    Main function to run the consumer.
    """
    # Configuration
    # Use 'localhost:9092' when running from host machine
    # Use 'kafka:29092' when running inside Docker container
    import os
    KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
    TOPIC_NAME = 'transactions'

    # Create consumer
    consumer = TransactionConsumer(
        bootstrap_servers=KAFKA_BROKER,
        topic=TOPIC_NAME,
        group_id='debug-consumer-group'
    )

    # Display topic info
    consumer.get_topic_info()

    # Consume messages
    # Options:
    # 1. Consume first 10 messages:
    # consumer.consume_messages(max_messages=10)

    # 2. Consume all messages (infinite):
    consumer.consume_messages()


if __name__ == "__main__":
    main()