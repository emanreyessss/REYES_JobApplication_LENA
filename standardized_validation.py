import pandas as pd
import re
import os
from datetime import datetime


#%%% Load latest mobile_data as add_base

#Path 
base_dir = r'C:\Users\Janjan Wangbu\Downloads\Assignment 2\mobile_device_standardized'

#Max Year
years = [int(d.split('=')[1
                          ]) for d in os.listdir(base_dir) if d.startswith('year=')]
max_year = max(years)

#Max Month
year_dir = os.path.join(base_dir, f'year={max_year}')
months = [int(d.split('=')[1]) for d in os.listdir(year_dir) if d.startswith('month=')]
max_month = f'{max(months):02d}'  # Formatting month with leading zero if necessary

#Max Date
month_dir = os.path.join(year_dir, f'month={max_month}')
dates = [int(d.split('=')[1]) for d in os.listdir(month_dir) if d.startswith('date=')]
max_date = f'{max(dates):02d}'  # Formatting date with leading zero if necessary

# Step 4: Construct the directory path with the maximum year, month, and date
target_dir = os.path.join(month_dir, f'date={max_date}')

# Step 5: Load all CSV files in the target directory
csv_files = [os.path.join(target_dir, f) for f in os.listdir(target_dir) if f.endswith('.csv')]
dataframes = [pd.read_csv(file) for file in csv_files]

# If you want to concatenate all DataFrames into one
add_base = pd.concat(dataframes, ignore_index=True)

#%%% Load all the standardized database of mobile_data except the latest
# List to hold all the DataFrames
data_frames = []

for root, dirs, files in os.walk(base_dir):
    print(f"Processing directory: {root}")  # Debug statement
    for file_name in files:
        if file_name.endswith('.csv'):
            file_path = os.path.join(root, file_name)
            if file_path not in csv_files:
                # Add file_path to the set of processed files
                csv_files.append(file_path)
                print(f"Reading file: {file_path}")  # Debug statement
                try:
                    df = pd.read_csv(file_path)
                    data_frames.append(df)
                except Exception as e:
                    print(f"Failed to read {file_path}: {e}")

# Check if any DataFrames were collected and concatenate
if data_frames:
    old_base = pd.concat(data_frames, ignore_index=True)
    
#%%% Combine two
new_base = pd.concat([add_base, old_base], ignore_index=True)

#%%% Column Distribution Checking
def get_column_summary(df, column):
    if column in df.columns:
        total_rows = len(df)
        distinct_rows = len(df.drop_duplicates(subset=[column]))
        null_counts = df[column].isnull().sum()
        duplicates = total_rows - distinct_rows
        return total_rows, distinct_rows, null_counts, duplicates
    else:
        return None, None, None, None

#%%% Mobile_code checking
column = 'mobile_code'
old_base_count, old_base_distinct_count, old_base_null_count, old_base_duplicates = get_column_summary(old_base, column)
new_base_count, new_base_distinct_count, new_base_null_count, new_base_duplicates = get_column_summary(new_base, column)
add_base_count, add_base_distinct_count, add_base_null_count, add_base_duplicates = get_column_summary(add_base, column)


add_base_to_new_base = add_base_count / new_base_count if new_base_count > 0 else None
add_base_to_new_base_distinct = add_base_distinct_count / new_base_distinct_count if new_base_distinct_count > 0 else None

# Prepare results DataFrame for mobile_code
results = pd.DataFrame({
    'Description': [
        'Old Base Count',
        'New Base Count (Without Distinct)',
        'New Base Count (With Distinct)',
        'Add Base Count',
        'Add Base Count / New Base Count (Without Distinct)',
        'Add Base Count / New Base Count (With Distinct)',
        'Old Base Null Values',
        'New Base Null Values',
        'Add Base Null Values',
        'Add Base Duplicates'
    ],
    'Count': [
        old_base_count,
        new_base_count,
        new_base_distinct_count,
        add_base_count,
        add_base_to_new_base,
        add_base_to_new_base_distinct,
        old_base_null_count,
        new_base_null_count,
        add_base_null_count,
        add_base_duplicates
    ]
})

#%%% Check Column Distribution
# List of columns to compare for the subsequent sheets
columns_to_compare = [
    'mobile_brand', 'rating',
    'multi_sim', 'mobile_freq', 'mobile_voLTE', 'mobile_WIFI', 'mobile_NFC',
    'processor_core', 'processor_Hz', 'storage_ram', 'storage_hdd',
    'battery_fast_charge', 'display_Hz', 'mobile_os'
]

# Function to get distribution counts for each column
def get_column_distribution(df, column):
    if column in df.columns:
        return df.groupby(column).size().reset_index(name='Count')
    else:
        return pd.DataFrame(columns=[column, 'Count'])

# Initialize a dictionary to store results for each column
distributions = {}

# Compute distributions for each column in each DataFrame
for column in columns_to_compare:
    # Get distributions for each DataFrame
    add_base_dist = get_column_distribution(add_base, column)
    new_base_dist = get_column_distribution(new_base, column)
    old_base_dist = get_column_distribution(old_base, column)
    
    # Rename the count columns
    add_base_dist = add_base_dist.rename(columns={'Count': 'count_add'})
    new_base_dist = new_base_dist.rename(columns={'Count': 'count_new'})
    old_base_dist = old_base_dist.rename(columns={'Count': 'count_old'})
    
    # Merge the distributions on the column
    combined_dist = old_base_dist.merge(new_base_dist, on=column, how='outer')\
                                 .merge(add_base_dist, on=column, how='outer')\
                                 .fillna(0)
    
    # Calculate percentage of add_count relative to new_count
    combined_dist['percentage_add_to_new'] = (combined_dist['count_add'] / combined_dist['count_new'] * 100).fillna(0)
    
    # Store in dictionary
    distributions[column] = combined_dist

#%%%
# Save to Excel
with pd.ExcelWriter(r'C:\Users\Janjan Wangbu\Downloads\Assignment 2\mobile_device_validation\validation_standardized.xlsx') as writer:
    # Save mobile_code_summary as the first sheet
    results.to_excel(writer, sheet_name='mobile_code_comparison', index=False)
    
    # Save column distributions to subsequent sheets
    for column, df in distributions.items():
        df.to_excel(writer, sheet_name=column[:31], index=False)  # Sheet name limited to 31 characters
# %%
