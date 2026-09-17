import boto3
import os
import urllib.parse

from botocore.exceptions import ClientError


# Create the Glue client once outside the Lambda handler
glue = boto3.client("glue")


# Glue workflow name and output bucket are provided through Lambda
# environment variables, set by the CloudFormation Lambda stack from
# the S3 stack's exported bucket name - never hardcoded here.
GLUE_WORKFLOW_NAME = os.environ["GLUE_WORKFLOW_NAME"]
OUTPUT_BUCKET_NAME = os.environ["OUTPUT_BUCKET_NAME"]


def lambda_handler(event, context):
    # Log the complete incoming event for debugging
    print(f"Received event: {event}")

    # This Lambda is invoked via an EventBridge rule (not a direct S3
    # bucket notification), so the event shape is EventBridge's own
    # "Object Created" schema, not the old S3 Records[] format:
    #   event["detail"]["bucket"]["name"]
    #   event["detail"]["object"]["key"]
    #   event["detail"]["object"]["size"]
    if event.get("detail-type") != "Object Created" or event.get("source") != "aws.s3":
        print(f"Ignored: unexpected event source/detail-type: {event.get('source')} / {event.get('detail-type')}")
        return {
            "statusCode": 200,
            "message": "Ignored non-S3-object-created event"
        }

    detail = event["detail"]

    # Get the S3 bucket name
    bucket_name = detail["bucket"]["name"]

    # Get the uploaded object's size
    object_size = detail["object"].get("size", 0)

    # EventBridge already delivers the key decoded, but unquote_plus is
    # a harmless no-op on an already-decoded string, so keep it as a
    # safety net in case that ever changes.
    object_key = urllib.parse.unquote_plus(
        detail["object"]["key"]
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
    output_path = f"s3://{OUTPUT_BUCKET_NAME}/processed/{base_name}/"

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