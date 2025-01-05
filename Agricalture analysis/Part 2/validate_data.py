# validate_data.py

import pytest
import pandas as pd
import numpy as np
from data_ingestion import create_db_engine, query_data, read_from_web_CSV
from weather_data_processor import WeatherDataProcessor 
from field_data_processor import FieldDataProcessor  # Assuming classes are in main.py

patterns = {
    'Rainfall': r'(\d+(\.\d+)?)\s?mm',
    'Temperature': r'(\d+(\.\d+)?)\s?C',
    'Pollution_level': r'=\s*(-?\d+(\.\d+)?)|Pollution at \s*(-?\d+(\.\d+)?)'
    }

# Mock configuration for testing
config_params = {
    # Paste in your previous dictionary data in here
    "sql_query": """
        SELECT *
            FROM geographic_features
            LEFT JOIN weather_features USING (Field_ID)
            LEFT JOIN soil_and_crop_features USING (Field_ID)
            LEFT JOIN farm_management_features USING (Field_ID)
            """, # Insert your SQL query
    "db_path": 'sqlite:///Maji_Ndogo_farm_survey_small.db', # Insert the db_path of the database
    "columns_to_rename": {'Annual_yield': 'Crop_type', 'Crop_type': 'Annual_yield'}, # Insert the disctionary of columns we want to swop the names of, 
    "values_to_rename": {'cassaval': 'cassava', 'cassava ': 'cassava', 'wheatn': 'wheat', 'wheat ': 'wheat', 'teaa': 'tea', 'tea ': 'tea'}, # Insert the croptype renaming dictionary
    "weather_csv_path": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv", # Insert the weather data CSV here
    "weather_mapping_csv": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv", # Insert the weather data mapping CSV here
    # Add two new keys
    "weather_csv_path":  "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv", # Insert the URL for the weather station data
    "regex_patterns" :  patterns, # Insert the regex pattern we used to process the messages
}

@pytest.fixture
def weather_processor():
    return WeatherDataProcessor(config_params)

@pytest.fixture
def field_processor():
    return FieldDataProcessor(config_params)

# Test if the weather DataFrame has the correct shape
def test_read_weather_DataFrame_shape(weather_processor):
    weather_processor.process()
    assert weather_processor.weather_df.shape == (1843, 4)  # Replace with expected shape
    print("Weather DataFrame shape test passed.")

# Test if the field DataFrame has the correct shape
def test_read_field_DataFrame_shape(field_processor):
    field_processor.process()
    assert field_processor.df.shape == (5654, 20)  # Replace with expected shape
    print("Field DataFrame shape test passed.")

# Test if the weather DataFrame has the expected columns
def test_weather_DataFrame_columns(weather_processor):
    weather_processor.process()
    expected_columns = ["Temperature", "Rainfall", "Pollution_level"]  # Replace with actual column names
    assert all(col in weather_processor.weather_df.Measurement.unique() for col in expected_columns)
    print("Weather DataFrame columns test passed.")

# Test if the field DataFrame has the expected columns
def test_field_DataFrame_columns(field_processor):
    field_processor.process()
    expected_columns = ["Field_ID", "Crop_type", "Elevation", "Soil_type"]  # Replace with actual column names
    assert all(col in field_processor.df.columns for col in expected_columns)
    print("Field DataFrame columns test passed.")

# Test if Elevation in field DataFrame is non-negative
def test_field_DataFrame_non_negative_elevation(field_processor):
    field_processor.process()
    assert (field_processor.df["Elevation"] >= 0).all()
    print("Field DataFrame non-negative elevation test passed.")

# Test if crop types are valid (based on a list of expected crop types)
def test_crop_types_are_valid(field_processor):
    field_processor.process()
    valid_crop_types = ['wheat', 'maize', 'rice', 'tea', 'coffee', 'cassava', 'banana', 'potato']  # Replace with valid crop types
    assert field_processor.df["Crop_type"].isin(valid_crop_types).all()
    print("Crop types validation test passed.")

# Test if rainfall values in weather DataFrame are positive
def test_positive_rainfall_values(weather_processor):
    weather_processor.process()
    assert (weather_processor.weather_df.query('Measurement == "Rainfall"')["Value"] > 0).all()
    print("Positive rainfall values test passed.")
