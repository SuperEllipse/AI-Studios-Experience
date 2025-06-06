# Author: Vish Rajagopalan
# Date: 28.April.2025

# Description:
# This script imports Air Quality data from the OpenAQ platform and saves it to a Cloudera Data Lake.
# It includes functionalities for fetching metadata, downloading air quality data, and saving the processed
# results to an S3 bucket. The script supports spatial filtering using city-specific bounding box coordinates.
# 
# Before running this script, ensure you have:
# 1. An OpenAQ API key, obtainable by signing up on the OpenAQ website: [https://openaq.org/](https://openaq.org/).
# 2. Properly configured environment variables for cities, S3 buckets, and API keys.
#
# Key Features:
# - Supports location metadata enrichment.
# - Downloads and processes data for specific cities using bounding box coordinates.
# - Saves the final transformed data to an S3 bucket.
# 
# Usage Notes:
# - Update the city configurations in `cities_config.json` to customize the locations for data ingestion.
# - Adjust date ranges using the `NUMBER_OF_DAYS` and `END_DATE` environment variables.
# 
# For further information, refer to the function-level docstrings and comments.

import os
os.chdir("/home/cdsw")
from utils.access_keys import ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, REGION_NAME
import boto3
import gzip
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime, timedelta
from utils.helper import load_city_config

CITIES_CONFIG_FILE =  os.environ["CITIES_CONFIG_FILE"]
SOURCE_BUCKET_NAME = os.environ["OPENAQ_DATA_SOURCE"] #  "openaq-data-archive"
TARGET_BUCKET_NAME = os.environ["TARGET_BUCKET_NAME"] # Replace with your destination bucket
OUTPUT_FILE_KEY_PREFIX =  os.environ["OUTPUT_FILE_KEY_PREFIX"]  # "/user/vishr/data/airquality"
API_KEY = os.environ["API_KEY"]  #= # Replace with your actual OpenAQ API key
# Initialize sessions
anonymous_session = boto3.Session()  # For public bucket
authenticated_session = boto3.Session()  # For private bucket

# Clients for respective sessions
openaq_client = anonymous_session.client('s3', region_name=REGION_NAME, config=Config(signature_version=UNSIGNED))
target_s3_client = authenticated_session.client('s3', region_name=REGION_NAME)

headers = {"X-API-Key": API_KEY}


# def load_city_config(config_file):
#     """Loads city configuration from a JSON file."""
#     with open(config_file, "r") as f:
#         return json.load(f)

def extract_metadata_fields(metadata):
    """
    Extracts metadata fields required for enrichment, including country name, owner name, 
    provider name, instruments, isMobile, and isMonitor.
    """
    return {
        "location_id": metadata.get("id",""),
        "location_name": metadata.get("name", ""),
        "country": metadata.get("country", {}).get("name", ""),
        # "owner": metadata.get("owner", {}).get("name", ""),
        "provider": metadata.get("provider", {}).get("name", ""),
        # "instruments": ", ".join(
        #     [instrument.get("name", "") for instrument in metadata.get("instruments", [])]
        # ),
        # "isMobile": metadata.get("isMobile", False),
        # "isMonitor": metadata.get("isMonitor", False),
    }


def enrich_data_with_metadata(data_df, metadata):
    """Enriches the data DataFrame with additional metadata fields."""
    # Map metadata to DataFrame using the `location` column directly
    data_df["location_name"] = data_df["location_id"].map(
        lambda location_id: metadata.get(location_id, {}).get("location_name", "")
    )
    data_df["provider"] = data_df["location_id"].map(
        lambda location_id: metadata.get(location_id, {}).get("provider", "")
    )
    return data_df


def get_location_metadata(city, bbox, api_key):
    """
    Fetch metadata for all locations within a city's bounding box.
    """
    url = "https://api.openaq.org/v3/locations"
    headers = {"X-API-Key": api_key}
    params = {"bbox": bbox, "limit": 1000}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    location_data = response.json().get("results", [])
    
    metadata = {}
    for location in location_data:
        location_id = location["id"]  # Ensure location ID is a string
        metadata[location_id] = extract_metadata_fields(location)

    print(f"Fetched metadata for {len(metadata)} locations in {city}.")  # Debugging
    return metadata



def get_location_ids(city, bbox):
    """Fetches location IDs for a city using the OpenAQ API."""
    locations_url = "https://api.openaq.org/v3/locations"
    locations_params = {"bbox": bbox, "limit": 1000}
    response = requests.get(locations_url, params=locations_params, headers=headers)
    response.raise_for_status()
    results = response.json().get("results", [])
    return [location["id"] for location in results]

def generate_date_range(start_date, end_date):
    """Generates a list of dates between start_date and end_date."""
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)

def download_data_to_dataframe(location_ids, start_date, end_date):
    """
    Downloads data for the given location IDs and consolidates it into a single DataFrame.
    
    Args:
        location_ids (list): List of location IDs to fetch data for.
        start_date (datetime): Start date for fetching data.
        end_date (datetime): End date for fetching data.
    
    Returns:
        pd.DataFrame: Consolidated DataFrame with data for all locations.
    """
    consolidated_df = pd.DataFrame()
    failed_locations = []  # To track locations that fail to return data
    
    for location_id in location_ids:
        for date in generate_date_range(start_date, end_date):
            year, month, day = date.strftime("%Y"), date.strftime("%m"), date.strftime("%d")
            prefix = f"records/csv.gz/locationid={location_id}/year={year}/month={month}/"
            response = openaq_client.list_objects_v2(Bucket=SOURCE_BUCKET_NAME, Prefix=prefix)

            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.endswith(f"{year}{month}{day}.csv.gz"):
#                        print(f"Processing file: {key}")
                        # Download and process the file
                        obj = openaq_client.get_object(Bucket=SOURCE_BUCKET_NAME, Key=key)
                        with gzip.GzipFile(fileobj=obj['Body']) as gz_file:
                            daily_df = pd.read_csv(gz_file)
                            consolidated_df = pd.concat([consolidated_df, daily_df], ignore_index=True)
            else:
                # print(f"No data returned for location ID: {location_id}")
                failed_locations.append(location_id)             

    
    if not consolidated_df.empty:
        print(f"Data successfully consolidated. Total records: {len(consolidated_df)}")
    else:
        print("No data was fetched for any location IDs.")
    
    if failed_locations:
        print(f"Locations with no data or errors: {failed_locations}")
    
    return consolidated_df


    return consolidated_df

def transform_data_for_rag(df):
    """Prepares data for RAG application."""
    # Normalize column names
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    # Add metadata (e.g., location, timestamp)
    if 'location' not in df.columns:
        df['location'] = "Unknown"  # Placeholder if location is missing
    if 'date' in df.columns:
        df['timestamp'] = pd.to_datetime(df['date'], errors='coerce')

    # Remove duplicates and null values
    df = df.drop_duplicates().dropna()

    # # Add unique identifier for vector DB
    # df['record_id'] = range(1, len(df) + 1)

    return df

def save_to_s3(df, bucket_name, file_key):
    """Saves a DataFrame to an S3 bucket as a CSV file."""
    csv_data = df.to_csv(index=False)
    target_s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_data)
    print(f"Consolidated data saved to S3 at: s3://{bucket_name}/{file_key}")

# Main execution
def main():
    END_DATE_STRING = os.getenv("END_DATE",  "31/12/2023 23:59:59 +0530")  #Enter the end date (YYYY-MM-DD):
    NUMBER_OF_DAYS= int(os.getenv("NUMBER_OF_DAYS", 10))
    # After Debug Uncomment the above and comment below     
    # END_DATE_STRING = "31/12/2023 23:59:59 +0530"  #Enter the end date (YYYY-MM-DD):
    NUMBER_OF_DAYS=  30   
    CITIES_CONFIG_FILE=os.getenv("CITIES_CONFIG_FILE", "cities_config.json")
    end_date = datetime.strptime(str(END_DATE_STRING), "%d/%m/%Y %H:%M:%S %z")
    start_date = end_date - timedelta(days=NUMBER_OF_DAYS)
    city_config = load_city_config(CITIES_CONFIG_FILE)
    for city, bbox in city_config.items():
        print(f"Fetching location metadata for city: {city}")
        location_metadata = get_location_metadata(city, bbox, API_KEY)

        print(f"Fetching location IDs for city: {city}")
        location_ids = list(location_metadata.keys())

        if location_ids:
            print(f"Found {len(location_ids)} locations for {city}. Downloading data...")
            city_data = download_data_to_dataframe(location_ids, start_date, end_date)
            
            if not city_data.empty:
                # Enrich with metadata only if city_data is not empty
                city_data = enrich_data_with_metadata(city_data, location_metadata)
                
                # Transform data for RAG
                transformed_df = transform_data_for_rag(city_data)
                
                # Define unique output file for each city
                city_output_key = f"{OUTPUT_FILE_KEY_PREFIX}/{city}_data.csv"
                
                # Save the city's transformed data to S3
                save_to_s3(transformed_df, TARGET_BUCKET_NAME, city_output_key)
                print(f"Data for city {city} saved to {city_output_key} in S3.")
            else:
                print(f"No data available for city: {city} within the specified date range.")
        else:
            print(f"No locations found for city: {city}. Skipping data download.")

    print("Processing completed for all cities.")

if __name__ == "__main__":
    main()

