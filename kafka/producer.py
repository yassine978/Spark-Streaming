# Write your kafka producer code here
from kafka import KafkaProducer
import json
import csv
import time
from datetime import datetime


class TransactionProducer:
    """
    Producer that sends transaction data to Kafka.
    """

    def __init__(self, bootstrap_servers='localhost:9092', topic='transactions'):
        """
        Initialize Kafka producer.

        Args:
            bootstrap_servers: Kafka broker address
            topic: Kafka topic name
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3
        )
        print(f"✓ Kafka Producer initialized for topic: {topic}")

    def send_from_csv(self, csv_file_path, delay=0.5):
        """
        Read CSV file and send each row to Kafka.

        Args:
            csv_file_path: Path to CSV file
            delay: Delay between messages (seconds) to simulate streaming
        """
        sent_count = 0

        try:
            with open(csv_file_path, 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)

                for row in csv_reader:
                    # Create transaction record
                    transaction = {
                        "transaction_id": int(row["transaction_id"]),
                        "user_id": int(row["user_id"]),
                        "amount": float(row["amount"]),
                        "timestamp": row["timestamp"]
                    }

                    # Send to Kafka
                    future = self.producer.send(
                        self.topic,
                        key=row["transaction_id"],
                        value=transaction
                    )

                    # Wait for confirmation
                    record_metadata = future.get(timeout=10)

                    sent_count += 1
                    if sent_count % 10 == 0:
                        print(f"Sent {sent_count} transactions... "
                              f"(partition: {record_metadata.partition}, offset: {record_metadata.offset})")

                    # Simulate streaming with delay
                    time.sleep(delay)

            print(f"\n✓ Successfully sent {sent_count} transactions to Kafka")

        except Exception as e:
            print(f"❌ Error sending data: {e}")
            raise
        finally:
            self.producer.flush()
            self.producer.close()

    def send_single_transaction(self, transaction):
        """
        Send a single transaction to Kafka.

        Args:
            transaction: Dictionary with transaction data
        """
        try:
            future = self.producer.send(
                self.topic,
                key=transaction.get("transaction_id"),
                value=transaction
            )
            record_metadata = future.get(timeout=10)
            print(f"✓ Sent transaction {transaction['transaction_id']} "
                  f"to partition {record_metadata.partition}")
        except Exception as e:
            print(f"❌ Error sending transaction: {e}")
            raise


def main():
    """
    Main function to run the producer.
    """
    # Configuration
    # Use 'localhost:9092' when running from host machine
    # Use 'kafka:29092' when running inside Docker container
    import os
    KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
    TOPIC_NAME = 'transactions'
    CSV_FILE = 'data/transactions.csv'

    # Create producer and send data
    producer = TransactionProducer(
        bootstrap_servers=KAFKA_BROKER,
        topic=TOPIC_NAME
    )

    print(f"\n🚀 Starting to send transactions from {CSV_FILE}")
    print("This will simulate real-time streaming with delays between messages\n")

    producer.send_from_csv(CSV_FILE, delay=0.1)  # 100ms delay between messages


if __name__ == "__main__":
    main()
