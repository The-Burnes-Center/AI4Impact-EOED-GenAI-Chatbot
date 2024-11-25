import json
import boto3
import pandas as pd
import numpy as np
from io import BytesIO
import logging
import os
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    logger.info("Lambda function has been invoked.")
    bedrock = boto3.client('bedrock-agent-runtime', region_name='us-east-1')
    s3 = boto3.client('s3')
    kb_id = os.getenv('KB_ID', None)

    if not kb_id:
        raise EnvironmentError("KB_ID environment variable not set")

    try:
        # Retrieve the Excel file
        local_path = retrieve_kb_docs("EOED-Master_1.xlsx", kb_id, bedrock, s3)
        print(f"File retrieved and saved to {local_path}")

        # Read the Excel file
        df_master = pd.read_excel(local_path, header=0)  # Adjust header rows if necessary
        print(df_master.head())  # For debugging

        headings = {
            "Category": df_master.columns[4:12],  # Columns E-L
            "Life Cycle": df_master.columns[12:16],  # Columns M-P
            "Size": df_master.columns[16:23],  # Columns Q-W
            "Grow Operations": df_master.columns[25:32],  # Columns Y-AF
            "Construct-New (Land)": df_master.columns[34:37],  # Columns AH-AK
            "Construct-Existing (Land)": df_master.columns[38:41]  # Columns AL-AO
        }

        # Process the DataFrame into the desired format
        data = process_excel_data(df_master, headings)

        # Return the data as a JSON response
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',  
                "Access-Control-Allow-Methods": "GET",
            },
            'body': json.dumps(data)
        }

    except Exception as e:
        print(f"Failed to retrieve or process file: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',  # Adjust as necessary
                'Content-Type': 'application/json'
            },
            'body': json.dumps(f"Failed to retrieve or process file: {str(e)}")
        }


def retrieve_kb_docs(file_name, knowledge_base_id, bedrock_client, s3_client):
    try:
        key, _ = os.path.splitext(file_name)
        print(f"Search query KB: {key}")
        response = bedrock_client.retrieve(
            knowledgeBaseId=knowledge_base_id,
            retrievalQuery={'text': key},
            retrievalConfiguration={
                'vectorSearchConfiguration': {
                    'numberOfResults': 5  # We only want the most relevant document
                }
            }
        )
        
        print(f"Retrieve API Response: {response}")

        file_uri = None
        if response.get('retrievalResults'):
            for result in response['retrievalResults']:
                uri = result['location']['s3Location']['uri']
                if file_name in uri:
                    file_uri = uri
                    break

        if not file_uri:
            raise FileNotFoundError(f"File {file_name} not found in knowledge base")

        # Parse the S3 URI
        s3_parts = file_uri.replace("s3://", "").split("/", 1)
        bucket_name = s3_parts[0]
        object_key = s3_parts[1]

        # Download the file from S3 to Lambda's /tmp directory
        local_file_path = f"/tmp/{file_name}"
        s3_client.download_file(bucket_name, object_key, local_file_path)
        print(f"File downloaded to {local_file_path}")

        return local_file_path

    except Exception as e:
        print(f"Error retrieving document: {e}")
        raise

def process_excel_data(df, headings):
    """
    Process the Excel data to extract dropdown and checkbox options dynamically.

    Args:
        df (DataFrame): The loaded Excel data.
        headings (dict): A dictionary mapping main categories to their column ranges.

    Returns:
        str: JSON string containing dropdowns, checkboxes, and records.
    """
    # Replace NaN values with None
    df = df.replace({np.nan: None})
    
    # Initialize dictionaries for dropdowns and checkboxes
    dropdowns = {}
    checkboxes = {}

    # Extract options for dropdowns and checkboxes dynamically
    for main_heading, columns in headings.items():
        if main_heading in ["Category", "Grow Operations", "Construct-New (Land)", "Construct-Existing (Land)"]:
            # Handle multi-column checkbox options
            subheadings = df[columns].iloc[1].dropna().to_dict()
            checkboxes[main_heading] = list(subheadings.keys())
        else:
            # Handle single-column dropdown options
            dropdown_values = df[columns].iloc[1].dropna().tolist()
            dropdowns[main_heading] = dropdown_values

    # Convert the DataFrame to a list of dictionaries for records
    records = df.to_dict(orient='records')

    # Prepare the data dictionary to return
    data = {
        'dropdowns': dropdowns,
        'checkboxes': checkboxes,
        'records': records
    }

    return json.dumps(data)