# Write your csv to json transformation code here 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import json
import csv


def get_transaction_schema():
    """
    Returns the schema for transaction data.

    Returns:
        StructType: Schema definition for transactions
    """
    return StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])


def csv_to_json_transformer(csv_file_path, output_json_path=None):
    """
    Transforms CSV data to JSON format.
    Useful for preparing data before sending to Kafka.

    Args:
        csv_file_path: Path to CSV file
        output_json_path: Optional path to save JSON output

    Returns:
        list: List of JSON objects
    """
    json_data = []

    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            # Convert data types
            json_row = {
                "transaction_id": int(row["transaction_id"]),
                "user_id": int(row["user_id"]),
                "amount": float(row["amount"]),
                "timestamp": row["timestamp"]
            }
            json_data.append(json_row)

    # Optionally save to file
    if output_json_path:
        with open(output_json_path, 'w') as json_file:
            for record in json_data:
                json_file.write(json.dumps(record) + '\n')

    print(f"✓ Converted {len(json_data)} records from CSV to JSON")
    return json_data


if __name__ == "__main__":
    # Test the transformation
    csv_to_json_transformer(
        "../data/transactions.csv",
        "../data/transactions.json"
    )