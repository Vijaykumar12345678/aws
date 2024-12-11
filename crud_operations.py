"""
@Author: Vijay Kumar M N
@Date: 2024-10-19
@Last Modified by: Vijay Kumar M N
@Last Modified: 2024-10-19
@Title : python program to connect for s3 bucket using boto3
"""


import boto3
import pandas as pd
import os
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, ClientError

# Load environment variables from .env file
load_dotenv()

# Initialize the S3 client with credentials from environment variables
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

# Replace this with your actual bucket name
bucket_name = input("Enter the bucket name: ")  # Bucket name should be globally unique
csv_file_name = 'movie.mkv'
s3_key = 'folder-in-s3/' + csv_file_name

def create_dataframe_and_upload():
    """Create a DataFrame of employees and upload it to S3"""
    data = {
        'EmployeeID': [1, 2, 3, 4, 5],
        'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'Department': ['HR', 'Finance', 'Engineering', 'Marketing', 'Sales'],
        'Salary': [50000, 60000, 70000, 80000, 90000],
        'JoiningDate': ['2021-01-01', '2020-05-15', '2019-03-23', '2018-07-30', '2022-09-10']
    }
    df = pd.DataFrame(data)
    # Save DataFrame to CSV
    df.to_csv(csv_file_name, index=False)
    print(f"DataFrame created and saved as {csv_file_name}")
    path="D:/file.mkv"
    # Upload the CSV file to S3
    try:
        print("File uploading in progress")
        s3.upload_file(path, bucket_name, s3_key)
        print("File uploaded to S3 successfully")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")
    except ClientError as e:
        print(f"Error uploading file to S3: {e}")

def create_bucket():
    # Attempt to create a bucket
    try:
        response = s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
        'LocationConstraint': 'ap-south-1'})  # Change region as needed
        print("Bucket created successfully:", response)
    except ClientError as e:
        print("Error creating bucket:", e)

def delete_bucket():
    """Delete an S3 bucket"""
    try:
        s3.delete_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' deleted successfully")
    except ClientError as e:
        print(f"Error deleting bucket: {e}")

def download_file():
    """Download a file from S3 (Read)"""
    try:
        download_path = 'downloaded-file.mkv'
        s3.download_file(bucket_name, s3_key, download_path)
        print(f"File downloaded successfully to {download_path}")
    except NoCredentialsError:
        print("Credentials not available")
    except ClientError as e:
        print(f"Error downloading file: {e}")

def read_file_content():
    """Read the content of a file from S3 without downloading"""
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        print("File content:\n", content)
    except NoCredentialsError:
        print("Credentials not available")
    except ClientError as e:
        print(f"Error reading file content: {e}")


def delete_file():
    """Delete a file from S3 (Delete)"""
    try:
        s3.delete_object(Bucket=bucket_name, Key=s3_key)
        print("File deleted successfully")
    except NoCredentialsError:
        print("Credentials not available")
    except ClientError as e:
        print(f"Error deleting file: {e}")

def list_files():
    """List all files in the S3 bucket"""
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(obj['Key'])
        else:
            print("Bucket is empty or you don't have permissions.")
    except NoCredentialsError:
        print("Credentials not available")
    except ClientError as e:
        print(f"Error listing files: {e}")

# Run CRUD operations in a loop
if __name__ == '__main__':
    while True:
        print("\nOptions:")
        print("1. Create Bucket")
        print("2. Delete Bucket")
        print("3. Create DataFrame and Upload to S3")
        print("4. Download File")
        print("5. Read File Content")
        
        print("6. Delete File")
        print("7. List Files in Bucket")
        print("8. Exit")

        choice = input("Choose an operation (1-8): ")

        if choice == '1':
            create_bucket()
        elif choice == '2':
            delete_bucket()
        elif choice == '3':
            create_dataframe_and_upload()
        elif choice == '4':
            download_file()
        elif choice == '5':
            read_file_content()
        elif choice == '6':
            delete_file()
        elif choice == '7':
            list_files()
        elif choice == '8':
            print("Exiting the program.")
            break
        else:
            print("Invalid choice. Please choose a valid operation.")
