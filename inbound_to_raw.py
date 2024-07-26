import pandas as pd
import re
import os
from datetime import datetime

directory = r'C:\Users\Janjan Wangbu\Downloads\Assignment 2\inbound'

# Get all files in the directory
files = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
latest_file = max(files, key=os.path.getmtime)
modification_time = os.path.getmtime(latest_file)
modification_date = datetime.fromtimestamp(modification_time)


year = modification_date.strftime('%Y')
month = modification_date.strftime('%m')
date = modification_date.strftime('%d')

# Load the latest file into a pandas DataFrame
inbound = pd.read_csv(latest_file)

# Write file to raw_file
directory = f'C:/Users/Janjan Wangbu/Downloads/Assignment 2/mobile_device_raw/year={year}/month={month}/date={date}/'
os.makedirs(directory, exist_ok=True)  # This creates the directory if it doesn't exist
file_path = os.path.join(directory, f'mobile_raw_{year+month+date}.csv')

#%%%
# Save the DataFrame to CSV
inbound.to_csv(file_path, index=False)


