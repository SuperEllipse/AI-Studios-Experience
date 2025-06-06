"""
File: air_quality_forecast.py
Author: Vish Rajagopalan
Date: 28.April.2025
Description: 
    This script processes air quality data to generate daily forecast prompts.
    It leverages S3 for data retrieval and storage, and supports zero-shot 
    and fine-tuning prompt generation for air quality prediction.

Dependencies:
    - boto3
    - pandas
    - requests
    - json
    - datetime
"""

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
from utils.helper import load_city_config
from io import StringIO  # Import StringIO for handling in-memory CSV files

CITIES_CONFIG_FILE = os.environ["CITIES_CONFIG_FILE"]
SOURCE_BUCKET_NAME = os.environ["OPENAQ_DATA_SOURCE"]  # "openaq-data-archive"
TARGET_BUCKET_NAME = os.environ["TARGET_BUCKET_NAME"]  # Replace with your destination bucket
OUTPUT_FILE_KEY_PREFIX = os.environ["OUTPUT_FILE_KEY_PREFIX"]  # "/user/vishr/data/airquality"
API_KEY = os.environ["API_KEY"]  # Replace with your actual OpenAQ API key

# Initialize sessions
anonymous_session = boto3.Session()  # For public bucket
authenticated_session = boto3.Session()  # For private bucket

# Clients for respective sessions
openaq_client = anonymous_session.client('s3', region_name=REGION_NAME, config=Config(signature_version=UNSIGNED))
target_s3_client = authenticated_session.client('s3', region_name=REGION_NAME)

headers = {"X-API-Key": API_KEY}

s3_bucket_name = TARGET_BUCKET_NAME  # Replace with your bucket name
parameter_to_use = "value" # the parameter that captures the pm25 value
history_length_days_forecast = 10  # historical Window size
forecast_window_days_forecast = 2  # Forecast window
max_prompts = 3 # max number of prompts to be generated
all_zero_shot_prompts = [] # initialize the prompts list for zero shot
all_ft_prompts = [] # initialize the prompts list for zero shot


def create_daily_forecast_prompts_v4(
    df, location_name, parameter='value', history_length_days=30, forecast_days=7, prompt_type='zero-shot', max_prompts_per_city=None
):
    """
    Generates prompts for daily air quality forecasting for multiple locations.

    Args:
        df (pd.DataFrame): Input DataFrame with 'datetime', 'location_name', and the specified parameter column.
        location_names (list): List of locations to generate prompts for.
        parameter (str): The column name containing the air quality measurement (e.g., 'value').
        history_length_days (int): The number of historical days of daily mean data to include.
        forecast_days (int): The number of future days to forecast (up to 7).
        prompt_type (str): Either 'zero-shot' or 'fine-tuning'.
        max_prompts_per_city (int, optional): Maximum number of prompts to generate per city.

    Returns:
        list: A list of prompts (for zero-shot) or a list of dictionaries (for fine-tuning).
    """
    df['datetime'] = pd.to_datetime(df['datetime'])
    prompts = []
    output_list = []
    
    df_location = df[df['location_name'] == location_name].sort_values(by='datetime').set_index('datetime')
    daily_data = df_location[[parameter]].resample('D').mean().dropna().reset_index()
    daily_data['date'] = daily_data['datetime'].dt.date

    prompt_count = 0  # Track the number of prompts generated for the city

    if prompt_type == 'zero-shot':
        end_historical_date = daily_data['date'].max()
        start_historical_date = end_historical_date - pd.Timedelta(days=history_length_days - 1)
        forecast_start_date = end_historical_date + pd.Timedelta(days=1)
        forecast_end_date = forecast_start_date + pd.Timedelta(days=forecast_days - 1)

        historical_data = daily_data[(daily_data['date'] >= start_historical_date) & (daily_data['date'] <= end_historical_date)].sort_values(by='date')
        formatted_historical_data = "| Date       | Value (µg/m³) |\n|------------|---------------|\n" + \
            "\n".join([f"| {row['date']} | {row[parameter]:.2f} |" for _, row in historical_data.iterrows()])

        instruction = (
            f"You are an advanced forecasting system tasked with predicting daily air quality for {location_name}.\n"
            f"The data includes daily mean values for '{parameter}' (in µg/m³).\n\n"
            f"### Historical Data (from {start_historical_date} to {end_historical_date}):\n"
            f"{formatted_historical_data}\n\n"
            f"### Instructions:\n"
            f"1. Forecast daily mean '{parameter}' for each day from {forecast_start_date} to {forecast_end_date}.\n"
            f"2. Provide output as a table:\n"
            f"| Date       | Predicted Value (µg/m³) |\n|------------|-------------------------|"
        )
        completion_zero_shot = f"| Date       | Predicted Value (µg/m³) |\n|------------|-------------------------|"
        prompts.append(instruction)
        output_list.append({"Prompt": instruction, "Completion": completion_zero_shot})
        prompt_count += 1

    elif prompt_type == 'fine-tuning':
        if len(daily_data) >= history_length_days + forecast_days:
            for i in range(history_length_days, len(daily_data) - forecast_days + 1):
                hist_start = i - history_length_days
                hist_end = i
                future_start = i
                future_end = i + forecast_days

                historical_context = "| Date       | Value (µg/m³) |\n|------------|---------------|\n" + \
                    "\n".join([f"| {row['date']} | {row[parameter]:.2f} |" for _, row in daily_data.iloc[hist_start:hist_end].iterrows()])
                future_target = "| Date       | Predicted Value (µg/m³) |\n|------------|-------------------------|\n" + \
                    "\n".join([f"| {row['date']} | {row[parameter]:.2f} |" for _, row in daily_data.iloc[future_start:future_end].iterrows()])

                instruction = (
                    f"You are an advanced forecasting system tasked with predicting daily air quality for {location_name}.\n"
                    f"The data includes daily mean values for '{parameter}' (in µg/m³).\n\n"
                    f"### Historical Data:\n{historical_context}\n\n"
                    f"### Instructions:\n"
                    f"1. Forecast daily mean '{parameter}' for each day.\n"
                    f"2. Provide output as a table:\n| Date       | Predicted Value (µg/m³) |\n|------------|-------------------------|"
                )
                response = future_target
                prompts.append({"instruction": instruction, "response": response})
                output_list.append({"Prompt": instruction, "Completion": response})
                prompt_count += 1

    return output_list, prompts

def load_data_from_s3(s3_bucket_name, s3_file_key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=s3_bucket_name, Key=s3_file_key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        return df
    except Exception as e:
        print(f"Error loading file from S3: {e}")
        return None

def save_to_json(data, filename):
    """Saves the list of prompts and completions to a JSON file."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Data saved to {filename}")




def main(): 
    try:
        # Attempt to load the city configuration and extract the city names
        locations_list = [city for city in load_city_config("cities_config.json").keys()]
        print("Loaded cities:", locations_list)
    except FileNotFoundError:
        # Handle the case where the file is not found
        print("Error: The configuration file 'cities_config.json' was not found.")
    except json.JSONDecodeError as e:
        # Handle the case where the JSON is not well-formed
        print(f"Error: The configuration file contains invalid JSON. Details: {e}")
    except Exception as e:
        # Handle any other unforeseen exceptions
        print(f"An unexpected error occurred: {e}")


    for location in locations_list:
        s3_file_key = f"{OUTPUT_FILE_KEY_PREFIX}/{location}_data.csv"  # Replace with the path to your hourly CSV file in S3
        print(s3_file_key)
        df_hourly = load_data_from_s3(s3_bucket_name, s3_file_key)
        
        if df_hourly is not None:
            # Zero-shot prompts
            output_list_zs, zero_shot_prompts = create_daily_forecast_prompts_v4(
                df_hourly.copy(),
                location_name=location,
                parameter=parameter_to_use,
                history_length_days=history_length_days_forecast,
                forecast_days=forecast_window_days_forecast,
                prompt_type='zero-shot',
                max_prompts_per_city=max_prompts
            )
            all_zero_shot_prompts.extend(output_list_zs)
            
            print("Zero-shot Prompt for Daily Forecast (Up to 1 Week, Revised):")
            for prompt in all_zero_shot_prompts:
                print(prompt)
                print("-" * 80)

            # Fine-tuning prompts
            output_list_ft, fine_tuning_prompts = create_daily_forecast_prompts_v4(
                df_hourly.copy(),
                location_name=location,
                parameter=parameter_to_use,
                history_length_days=history_length_days_forecast,
                forecast_days=forecast_window_days_forecast,
                prompt_type='fine-tuning',
                max_prompts_per_city=max_prompts
            )
            all_ft_prompts.extend(output_list_ft)
            print("\nFine-tuning Prompts for Daily Forecast (Up to 1 Week, Instruction-Response Pairs, Revised):")
            for example in all_ft_prompts:
                print(f"Prompt:\n{example['Prompt']}")
                print(f"\nCompletion:\n{example['Completion']}")
                print("-" * 80)        

    # Save results
    save_to_json(all_zero_shot_prompts, "data/air_quality_forecast_zeroshot_prompts.json")
    save_to_json(all_ft_prompts, "data/air_quality_forecast_ft_prompts.json")
    

if __name__ == "__main__":
    main()
