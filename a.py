import boto3
import json
import csv
from decimal import Decimal

# Set up the DynamoDB client
dynamodb = boto3.resource('dynamodb')

def float_to_decimal(obj):
    """Convert float types to Decimal for compatibility with DynamoDB."""
    if isinstance(obj, list):
        return [float_to_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: float_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))  # Convert float to string before Decimal
    else:
        return obj

def decimal_to_float(obj):
    """Convert Decimal types to float for JSON serialization."""
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

def json_to_csv(json_file, csv_file):
    """Convert a JSON file to a CSV file."""
    with open(json_file, 'r') as jf:
        data = json.load(jf)

    # Get the headers from the first item
    headers = data[0].keys()

    with open(csv_file, 'w', newline='') as cf:
        writer = csv.DictWriter(cf, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

    print(f"Data converted from '{json_file}' to '{csv_file}'.")

def import_csv_to_dynamodb(table_name, csv_file):
    """Import data from a CSV file to a specified DynamoDB table."""
    table = dynamodb.Table(table_name)

    with open(csv_file, 'r') as cf:
        reader = csv.DictReader(cf)
        for row in reader:
            # Convert string to Decimal for numeric fields
            if 'year' in row:
                row['year'] = Decimal(row['year'])  # Ensure year is a Decimal
            if 'rating' in row:
                row['rating'] = Decimal(row['rating'])  # Ensure rating is a Decimal

            # Convert other float fields to Decimal if necessary
            row = float_to_decimal(row)
            table.put_item(Item=row)

    print(f"Data imported from '{csv_file}' to DynamoDB table '{table_name}'.")

def export_dynamodb_to_csv(table_name, output_file):
    """Export data from a DynamoDB table to a local CSV file."""
    table = dynamodb.Table(table_name)

    # Scan the table and retrieve all items
    response = table.scan()
    items = response['Items']

    # Convert all Decimals to floats for CSV serialization
    items = decimal_to_float(items)

    if items:
        # Get headers from the first item
        headers = items[0].keys()
        
        with open(output_file, 'w', newline='') as cf:
            writer = csv.DictWriter(cf, fieldnames=headers)
            writer.writeheader()
            writer.writerows(items)

        print(f"Data exported from DynamoDB table '{table_name}' to '{output_file}'.")
    else:
        print(f"No data found in the table '{table_name}'.")

def main():
    # Define the DynamoDB table name and file paths
    table_name = "Movies"  # Change this to your DynamoDB table name
    json_file = "C:\\Users\\Admin\\Desktop\\aws\\dynamodb\\local_imported_data.json"  # Local file path for import
    csv_file = "dynamodb_data_import.csv"     # Local file path for CSV
    export_csv_file = "dynamodb_data_export.csv"  # Local file path for export

    # Step 1: Convert JSON to CSV
    json_to_csv(json_file, csv_file)

    # Step 2: Import data from the CSV file to DynamoDB
    import_csv_to_dynamodb(table_name, csv_file)

    # Step 3: Export data from DynamoDB to a CSV file
    export_dynamodb_to_csv(table_name, export_csv_file)

if __name__ == "__main__":
    main()
