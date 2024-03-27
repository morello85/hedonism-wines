import os
import boto3

def upload_files_to_s3(local_folder, bucket_name):
    s3 = boto3.client('s3')

    # List all files in the local folder
    files = os.listdir(local_folder)

    for file_name in files:
        local_file_path = os.path.join(local_folder, file_name)
        s3_object_key = file_name  # Assuming object key is same as file name

        # Check if the file already exists in the bucket
        try:
            s3.head_object(Bucket=bucket_name, Key=s3_object_key)
            print(f"File '{file_name}' already exists in the bucket. Skipping upload.")
        except:
            # If the file doesn't exist, upload it to S3
            print(f"Uploading '{file_name}' to S3 bucket '{bucket_name}'...")
            s3.upload_file(local_file_path, bucket_name, s3_object_key)
            print(f"File '{file_name}' uploaded successfully.")

if __name__ == "__main__":
    # Specify the local folder containing the files
    local_folder = "/Users/MacUser/hedonism-wines_app/data"

    # Specify the name of the S3 bucket
    bucket_name = "hedonism-wines-api-files"

    # Call the function to upload files to S3
    upload_files_to_s3(local_folder, bucket_name)
