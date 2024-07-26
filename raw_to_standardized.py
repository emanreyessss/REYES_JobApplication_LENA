import pandas as pd
import re
import os
from datetime import datetime


#%%% Get Max date on path

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



#%%% Inbound

target_dir = os.path.join(month_dir, f'date={max_date}')
csv_files = [os.path.join(target_dir, f) for f in os.listdir(target_dir) if f.endswith('.csv')]
dataframes = [pd.read_csv(file) for file in csv_files]
raw = pd.concat(dataframes, ignore_index=True)


#%%%Create Standard Mobile code
brand_mapping = {
    'SAMSUNG': '001', 'APPLE': '002', 'XIAOMI': '003', 'VIVO': '004', 'REALME': '005',
    'MOTOROLA': '006', 'ONEPLUS': '007', 'OPPO': '008', 'POCO': '009', 'TECNO': '010',
    'INFINIX': '011', 'IQOO': '012', 'HONOR': '013', 'NOKIA': '014', 'ITEL': '015',
    'LAVA': '016', 'NUBIA': '017', 'GOOGLE': '018', 'ASUS': '019', 'SONY': '020',
    'NOTHING': '021', 'JIO': '022', 'MEIZU': '023', 'IKALL': '024', 'HUAWEI': '025',
    'SNEXIAN': '026', 'BLACKVIEW': '027', 'DUOQIN': '028', 'REDMI': '029', 'ULEFONE': '030',
    'GIONEE': '031', 'HTC': '032', 'CELLECOR': '033', 'BLACKZONE': '034', 'DOCOSS': '035',
    'DOOGEE': '036', 'FAIRPHONE': '037'
}

#drop duplicates
df_mobile_name = raw.drop_duplicates().dropna(subset=['mobile_name'])
#Seperate Brand with Model of phone
df_mobile_name[['mobile_brand', 'mobile_model']] = df_mobile_name['mobile_name'].str.split(' ', n=1, expand=True)

#Filter mobile_model cleaning string that is in a parenthesis
df_mobile_name['mobile_model'] = df_mobile_name['mobile_model'].str.replace(r'\(.*\)', '', regex=True).str.strip()

#Create Mapping of mobile devices ; Chosen Brands ; Not All included
df_mobile_name['mobile_brand'] = df_mobile_name['mobile_brand'].str.upper()
df_mbrand_filter = df_mobile_name[df_mobile_name['mobile_brand'].isin(brand_mapping.keys())]

#Create Mobile_code based on prefix and load date of phone in the database
date_str = f'{max_year % 100:02d}'+ max_month + max_date
df_mbrand_filter['mobile_prefix'] = df_mbrand_filter['mobile_brand'].map(brand_mapping)
df_mbrand_filter['unique_id'] = df_mbrand_filter.groupby('mobile_prefix').cumcount() + 1
df_mbrand_filter['unique_id'] = df_mbrand_filter['unique_id'].apply(lambda x: f'{date_str}{x:03}')
df_mbrand_filter['mobile_code'] = df_mbrand_filter['mobile_prefix'] + df_mbrand_filter['unique_id']

#%%% Main DF
mobile_db = df_mbrand_filter.reindex(['mobile_code', 'mobile_brand', 'mobile_model','price', 'rating', 'connectivity', 'processor','storage', 'battery', 'display', 'os'], axis=1)
mobile_prim = mobile_db[['mobile_code','mobile_brand','mobile_model']]

#%%% Price column cleaning

mobile_price = mobile_db[['mobile_code','price']]
mobile_price['price_rupees'] = mobile_price['price'].replace({'₹': '', ',': ''}, regex=True).astype(int)
mobile_price.drop(columns='price', inplace=True)

#%%% Rating cleaning

mobile_rating = mobile_db[['mobile_code','rating']]
mobile_rating= mobile_rating[(mobile_db['rating'] >= 1) & (mobile_db['rating'] <= 5)]

#%%% Breakdown of connectivity column and cleaning


mobile_connec = mobile_db[['mobile_code','connectivity']]
mobile_connec['multi_sim'] = mobile_connec['connectivity'].apply(lambda x: 'Dual Sim' in x)
mobile_connec['mobile_freq'] = mobile_connec['connectivity'].apply(
                                    lambda x: '5G' if '5G' in x 
                                        else '4G' if '4G' in x 
                                        else '3G' if '3G' in x 
                                        else None)
mobile_connec['mobile_voLTE'] = mobile_connec['connectivity'].apply(lambda x: 'VoLTE' in x)
mobile_connec['mobile_WIFI'] = mobile_connec['connectivity'].apply(lambda x: 'Wi-Fi' in x)
mobile_connec['mobile_NFC'] = mobile_connec['connectivity'].apply(lambda x: 'NFC' in x)
mobile_connec.drop(columns='connectivity', inplace=True)

#%%% Breakdown of processor column and cleaning


mobile_process = mobile_db[['mobile_code', 'processor']]
mobile_process['processor'] = mobile_process['processor'].str.upper()

# Extract the string immediately before "CORE"
mobile_process['processor_core'] = mobile_process['processor'].apply(
    lambda text: re.search(r'\b(\w+)\s+CORE\b', text).group(1) if re.search(r'\b(\w+)\s+CORE\b', text) else 'Unknown')

mobile_process['processor_Hz'] = mobile_process['processor'].apply(
    lambda x: float(re.search(r'(\d+\.\d+|\d+)\s?GHz', x, re.IGNORECASE).group(1))
    if re.search(r'(\d+\.\d+|\d+)\s?GHz', x, re.IGNORECASE) else None)

mobile_process.drop(columns='processor', inplace=True)

#%%% Breakdown of storage column and cleaning


mobile_storage = mobile_db[['mobile_code','storage']]
mobile_storage['storage_ram'] = mobile_storage['storage'].apply(
    lambda x: int(re.search(r'(\d+)\s?(GB|MB)\s?RAM', x, re.IGNORECASE).group(1))
    if re.search(r'(\d+)\s?(GB|MB)\s?RAM', x, re.IGNORECASE) else None)

# Get storage hdd 
mobile_storage['storage_hdd'] = mobile_storage['storage'].apply(
    lambda x: int(re.search(r'(\d+)\s?(GB|MB)\s?inbuilt', x, re.IGNORECASE).group(1))
    if re.search(r'(\d+)\s?(GB|MB)\s?inbuilt', x, re.IGNORECASE) else None)
mobile_storage.drop(columns='storage', inplace=True)

#%%% Breakdown of battery column and cleaning


mobile_batt = mobile_db[['mobile_code','battery']]
mobile_batt['battery_capacity'] = mobile_batt['battery'].apply(
    lambda x: int(re.search(r'(\d+)\s?mAH', x, re.IGNORECASE).group(1))
    if re.search(r'(\d+)\s?mAH', x, re.IGNORECASE) else None)

mobile_batt['battery_watts'] = mobile_batt['battery'].apply(
    lambda x: int(re.search(r'(\d+)\s?W', x, re.IGNORECASE).group(1))
    if re.search(r'(\d+)\s?W', x, re.IGNORECASE) else None)

# Add battery_fast_charge column
mobile_batt['battery_fast_charge'] = mobile_batt['battery'].apply(
    lambda x: 't' if 'FAST CHARGING' in x.upper() else 'f')
mobile_batt.drop(columns='battery', inplace=True)


#%%% Breakdown of display column and cleaning

mobile_display = mobile_db[['mobile_code','display']]
mobile_display['display_inch'] = mobile_display['display'].apply(
    lambda x: int(re.search(r'(\d+)\s?inches', x, re.IGNORECASE).group(1))
    if re.search(r'(\d+)\s? inches', x, re.IGNORECASE) else None)

mobile_display['display_Hz'] = mobile_display['display'].apply(
    lambda x: int(re.search(r'(\d+)\s?Hz', x, re.IGNORECASE).group(1))
    if re.search(r'(\d+)\s?Hz', x, re.IGNORECASE) else None)

mobile_display['display_width'] = mobile_display['display'].apply(
    lambda x: int(re.search(r'(\d+)\s?x', x, re.IGNORECASE).group(1))
    if re.search(r'(\d+)\s?x', x, re.IGNORECASE) else None)

mobile_display['display_height'] = mobile_display['display'].apply(
    lambda x: int(re.search(r'(\d+)\s?px', x, re.IGNORECASE).group(1))
    if re.search(r'(\d+)\s?px', x, re.IGNORECASE) else None)
mobile_display.drop(columns='display', inplace=True)


#%%% Breakdown of OS column and cleaning

mobile_os = mobile_db[['mobile_code','os']]
os_name = ['RTOS','KAI','SYMBIAN','ANDROID','IOS','HARMONYOS']

pattern = '|'.join([re.escape(name) for name in os_name])
mobile_os = mobile_db[['mobile_code', 'os']]
mobile_os['mobile_os'] = mobile_os['os'].apply(
    lambda x: next((name for name in os_name if isinstance(x, str) and name.lower() in x.lower()), 'Other'))
mobile_os.drop(columns='os', inplace=True)

#%%% Merge DF
mobile_cleaned =  mobile_prim.merge(mobile_price, on='mobile_code', how='left')\
                             .merge(mobile_rating, on='mobile_code', how='left')\
                             .merge(mobile_connec, on='mobile_code', how='left')\
                             .merge(mobile_process, on='mobile_code', how='left')\
                             .merge(mobile_storage, on='mobile_code', how='left')\
                             .merge(mobile_batt, on='mobile_code', how='left')\
                             .merge(mobile_display, on='mobile_code', how='left')\
                             .merge(mobile_os, on='mobile_code', how='left')
                            

#%%% Export to CSV
date_load = f'{max_year}' + max_month + max_date
directory = f'C:/Users/Janjan Wangbu/Downloads/Assignment 2/mobile_device_standardized/year={max_year}/month={max_month}/date={max_date}/'
os.makedirs(directory, exist_ok=True)  
file_path = os.path.join(directory, f'mobile_database_{date_load}.csv')
#%%%
# Save the DataFrame to CSV
mobile_cleaned.to_csv(file_path, index=False)
