import pandas as pd
import re
import os
from datetime import datetime

#%%% Run functions for profiling and column comparison


def dk_profiling(df, primary_key, suffix):
    if primary_key not in df.columns:
        raise ValueError(f"Primary key column '{primary_key}' not found in DataFrame.")
    
    total_rows = len(df)
    
    # Profile the primary key column
    row_total = total_rows 
    null_counts = df[primary_key].isnull().sum()
    null_percentage = (null_counts / total_rows * 100)
    duplicate_counts = df[primary_key].duplicated().sum()
    duplicate_percentage = (duplicate_counts / total_rows * 100)
    data_type = df[primary_key].dtype.name
    
    # Create DataFrame with profiling results for the primary key column
    profile_df = pd.DataFrame({
        f'Total Rows{suffix}': [row_total],
        f'Null Count{suffix}': [null_counts],
        f'Null Percentage{suffix}': [null_percentage],
        f'Duplicate Count{suffix}': [duplicate_counts],
        f'Duplicate Percentage{suffix}': [duplicate_percentage],
        f'Data Type{suffix}': [data_type]
    }, index=[primary_key])
    
    return profile_df

def df_profile_columns(df, primary_key, suffix):    
    if primary_key not in df.columns:
        raise ValueError(f"Primary key column '{primary_key}' not found in DataFrame.")
    
    # Exclude the primary key column
    columns_to_profile = [col for col in df.columns if col != primary_key]
    
    # Profile the columns
    data_types = df[columns_to_profile].dtypes
    unique_counts = df[columns_to_profile].nunique()
    unique_values = {col: ', '.join(map(str, df[col].dropna().unique())) for col in columns_to_profile}
    
    # Create DataFrame with profiling results for the columns
    unique_values_df = pd.DataFrame(list(unique_values.items()), columns=['Column Name', f'Unique Values{suffix}'])
    profile_df = pd.DataFrame({
        f'Data Type{suffix}': data_types,
        f'Unique Count{suffix}': unique_counts
    }).reset_index().rename(columns={'index': 'Column Name'})
    
    # Merge the unique values with the profile DataFrame
    combined_df = profile_df.merge(unique_values_df, on='Column Name', how='left')
    
    return combined_df
#%%% Load Inbound data
directory = r'C:\Users\Janjan Wangbu\Downloads\Assignment 2\inbound'

# Get all files in the directory
files = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

# Get the latest modified file
latest_file = max(files, key=os.path.getmtime)

# Get the modification time of the latest file
modification_time = os.path.getmtime(latest_file)
modification_date = datetime.fromtimestamp(modification_time)

# Extract year, month, and date
inbound_year = modification_date.strftime('%Y')
inbound_month = modification_date.strftime('%m')
inbound_date = modification_date.strftime('%d')

# Load the latest file into a pandas DataFrame
inbound = pd.read_csv(latest_file)


#%%% Load Raw Database
#Path 
base_dir = r'C:\Users\Janjan Wangbu\Downloads\Assignment 2\mobile_device_raw'

#Max Year
years = [int(d.split('=')[1]) for d in os.listdir(base_dir) if d.startswith('year=')]
max_year = max(years)

#Max Month
year_dir = os.path.join(base_dir, f'year={max_year}')
months = [int(d.split('=')[1]) for d in os.listdir(year_dir) if d.startswith('month=')]
max_month = f'{max(months):02d}'  # Formatting month with leading zero if necessary

#Max Date
month_dir = os.path.join(year_dir, f'month={max_month}')
dates = [int(d.split('=')[1]) for d in os.listdir(month_dir) if d.startswith('date=')]
max_date = f'{max(dates):02d}'  # Formatting date with leading zero if necessary


#%%% RAW DATABASE
target_dir = os.path.join(month_dir, f'date={max_date}')
csv_files = [os.path.join(target_dir, f) for f in os.listdir(target_dir) if f.endswith('.csv')]
dataframes = [pd.read_csv(file) for file in csv_files]
database = pd.concat(dataframes, ignore_index=True)

#%%% Check columns

inbound_columns = set(inbound.columns)
database_columns = set(database.columns)
columns_in_database_not_in_inbound = database_columns - inbound_columns

# Create a DataFrame for these columns
column_changes_df = pd.DataFrame({
    'Column Changes': list(columns_in_database_not_in_inbound)
})
#%%% DF for latest inbound
# Profile the new DataFrame (inbound)
pk_prof_new = dk_profiling(inbound, 'mobile_name', '_new')
col_prof_new = df_profile_columns(inbound, 'mobile_name', '_new')

# DF for load database
pk_prof_old = dk_profiling(database, 'mobile_name', '_old')
col_prof_old = df_profile_columns(database, 'mobile_name', '_old')

#%%% Processing for DF profiling and validation

# Combine Primary Key Profiling Results
combined_pk_prof = pd.DataFrame({
    'Metric': ['Total_Rows','Null Count', 'Null Percentage', 'Duplicate Count', 'Duplicate Percentage', 'Data Type'],
    'Old Value': pk_prof_old.iloc[0].values,
    'New Value': pk_prof_new.iloc[0].values
})

#%%%
# Combine Column Profiling Results
col_comparison = pd.merge(col_prof_old, col_prof_new, on='Column Name', how='outer')

# Retain 'Column Name' as the first column
old_cols = [col for col in col_comparison.columns if '_old' in col]
new_cols = [col for col in col_comparison.columns if '_new' in col]

assert len(old_cols) == len(new_cols), "Mismatch in number of old and new columns"

sorted_columns = ['Column Name']
for old_col, new_col in zip(old_cols, new_cols):
    sorted_columns.append(old_col)
    sorted_columns.append(new_col)

# Reorder the columns in the DataFrame
col_comparison = col_comparison[sorted_columns]

#%%% Validation Report Xlsx file

with pd.ExcelWriter(r'C:\Users\Janjan Wangbu\Downloads\Assignment 2\mobile_device_validation\validation_inbound.xlsx') as writer:
    combined_pk_prof.to_excel(writer, sheet_name='Profile Comparison', index=False)
    col_comparison.to_excel(writer, sheet_name='Column Comparison', index=False)
    column_changes_df.to_excel(writer, sheet_name='Changes in Columns', index=False)