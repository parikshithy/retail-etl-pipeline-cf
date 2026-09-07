import boto3
import os
import urllib.parse

from botocore.exceptions import ClientError


# Create the Glue client once outside the Lambda handler
glue = boto3.client("glue")


# Glue workflow name is provided through the Lambda environment variable
GLUE_WORKFLOW_NAME = os.environ["GLUE_WORKFLOW_NAME"]


def lambda_handler(event, context):
    # Log the complete incoming S3 event for debugging
    print(f"Received event: {event}")

    # Read the first S3 event record
    record = event["Records"][0]

    # Get the S3 bucket name
    bucket_name = record["s3"]["bucket"]["name"]

    # Get the uploaded object's size
    object_size = record["s3"]["object"].get("size", 0)

    # S3 object keys can be URL encoded, so decode the key
    object_key = urllib.parse.unquote_plus(
        record["s3"]["object"]["key"]
    )

    print(f"Bucket: {bucket_name}")
    print(f"Object key: {object_key}")
    print(f"Object size: {object_size} bytes")

    # Ignore anything uploaded outside raw/
    if not object_key.startswith("raw/"):
        print("Ignored: object is outside raw/")
        return {
            "statusCode": 200,
            "message": "Ignored object outside raw/"
        }

    # Ignore anything that is not a CSV file
    if not object_key.lower().endswith(".csv"):
        print("Ignored: object is not a CSV file")
        return {
            "statusCode": 200,
            "message": "Ignored non-CSV file"
        }

    # Ignore empty files
    if object_size == 0:
        print("Ignored: CSV file is empty")
        return {
            "statusCode": 200,
            "message": "Ignored empty CSV file"
        }

    # Build the exact input S3 path
    input_path = f"s3://{bucket_name}/{object_key}"

    # Get only the filename, without the .csv extension
    file_name = object_key.split("/")[-1]
    base_name = file_name.rsplit(".", 1)[0]

    # Output always goes to the dedicated output bucket, under processed/
    output_path = f"s3://retail-etl-output-s3-cf/processed/{base_name}/"

    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    print(f"Starting Glue workflow: {GLUE_WORKFLOW_NAME}")

    try:
        # Start the Glue workflow, passing paths via run properties since
        # start_workflow_run does not accept job-style --arguments.
        response = glue.start_workflow_run(
            Name=GLUE_WORKFLOW_NAME,
            RunProperties={
                "INPUT_PATH": input_path,
                "OUTPUT_PATH": output_path
            }
        )

        print("Glue workflow started successfully")
        print(f"Workflow RunId: {response['RunId']}")

        return {
            "statusCode": 200,
            "message": "Glue workflow started successfully",
            "runId": response["RunId"],
            "inputPath": input_path,
            "outputPath": output_path
        }

    except ClientError as error:
        print(f"Unexpected AWS error: {error}")
        raise