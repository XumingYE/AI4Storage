# import os
# import pandas as pd
# import torch
# from datetime import timedelta
# from tqdm import tqdm

# # Parameters
# smart_data_base_folder = '/mnt/raid5/sum/card/storage/StreamDFP/dataset/SMART'  # Path to the SMART data folder
# failure_file_path = '/mnt/raid5/sum/card/storage/StreamDFP/dataset/ssd_failure_label.csv'  # Path to the failure data
# output_folder = '/mnt/raid5/sum/card/storage/StreamDFP/dataset/processed_smart_data'  # Folder to save processed data
# info_window = 30  # Time window in days
# look_ahead_days = 10  # Look-ahead days
# required_columns = ['r_1', 'r_9', 'r_12', 'r_171', 'r_173', 'r_174', 'r_183', 'r_187', 'r_188', 'r_194', 'r_195', 'r_198']

# # Load the failure data
# def load_failure_data(file_path):
#     failure_data = pd.read_csv(file_path)
#     failure_data['failure_time'] = pd.to_datetime(failure_data['failure_time'])
#     return failure_data


# # Load and process SMART data from a single CSV file (daily)
# def load_smart_data_for_day(file_path):
#     disk_smart_data = pd.read_csv(file_path)
    
#     # Ensure the necessary columns are available and parse the date column
#     disk_smart_data['ds'] = pd.to_datetime(disk_smart_data['ds'], format='%Y%m%d')
#     disk_smart_data = disk_smart_data[['disk_id', 'ds', 'model'] + required_columns]
    
#     return disk_smart_data


# # Process and save data for each day
# def process_and_save_data(smart_data_base_folder, failure_data, look_ahead_days, output_folder):
#     # Get list of all files in the SMART data folder (sorted by date)
    
#     all_files = [os.path.join(smart_data_base_folder, f) for f in os.listdir(smart_data_base_folder) if f.endswith('.csv')][2:3]
#     # all_smart_files = sorted([f for f in os.listdir(smart_data_base_folder) if f.endswith('.csv')])[0]
    
#     # Process each file
#     for file_path in tqdm(all_files, desc="Processing SMART files"):
#         print(f"Processing file: {file_path}")
#         disk_smart_data = load_smart_data_for_day(file_path)
        
#         processed_data = []
        
#         # Iterate over each row in the current file
#         for _, row in disk_smart_data.iterrows():
#             disk_id = row['disk_id']
#             timestamp = row['ds']
#             model = row['model']
            
#             # Get failure data for the current disk
#             disk_failures = failure_data[failure_data['disk_id'] == disk_id]
            
#             # Check if there's a failure in the look_ahead_days window
#             label = 0
#             for _, failure in disk_failures.iterrows():
#                 failure_start = failure['failure_time'] - timedelta(days=look_ahead_days)
#                 if failure_start <= timestamp <= failure['failure_time']:
#                     label = 1
#                     break
                
#             # Collect features and label (including the necessary columns)
#             features = row[required_columns].values.flatten()
#             processed_data.append([disk_id, timestamp, model, *features, label])
        
#         # Convert the processed data to a DataFrame
#         processed_df = pd.DataFrame(processed_data, columns=['disk_id', 'ds', 'model', *required_columns, 'label'])
        
#         # Save the processed data to a new file
#         output_file_path = os.path.join(output_folder, f"{os.path.basename(file_path).split('.')[0]}_processed.csv")
#         processed_df.to_csv(output_file_path, index=False)
#         print(f"Processed data saved to {output_file_path}")


# # Main execution
# failure_data = load_failure_data(failure_file_path)
# process_and_save_data(smart_data_base_folder, failure_data, look_ahead_days, output_folder)


import os
import pandas as pd
from datetime import timedelta
from tqdm import tqdm
import multiprocessing

# Parameters
smart_data_base_folder = '/mnt/raid5/sum/card/storage/StreamDFP/dataset/SMART'  # Path to the SMART data folder
failure_file_path = '/mnt/raid5/sum/card/storage/StreamDFP/dataset/ssd_failure_label.csv'  # Path to the failure data
output_folder = '/mnt/raid5/sum/card/storage/StreamDFP/dataset/processed_smart_data'  # Folder to save processed data
info_window = 30  # Time window in days (not needed in this version)
look_ahead_days = 10  # Look-ahead days
required_columns = ['r_1', 'r_9', 'r_12', 'r_171', 'r_173', 'r_174', 'r_183', 'r_187', 'r_188', 'r_194', 'r_195', 'r_198']

# Load the failure data
def load_failure_data(file_path):
    failure_data = pd.read_csv(file_path)
    failure_data['failure_time'] = pd.to_datetime(failure_data['failure_time'])
    return failure_data

# Load and process SMART data from a single CSV file (daily)
def load_smart_data_for_day(file_path):
    disk_smart_data = pd.read_csv(file_path)
    
    # Ensure the necessary columns are available and parse the date column
    disk_smart_data['ds'] = pd.to_datetime(disk_smart_data['ds'], format='%Y%m%d')
    disk_smart_data = disk_smart_data[['disk_id', 'ds', 'model'] + required_columns]
    
    return disk_smart_data

# Process a single file and save the result
def process_file(file_path, failure_data, look_ahead_days, output_folder):
    disk_smart_data = load_smart_data_for_day(file_path)
    
    processed_data = []
    
    # Iterate over each row in the current file
    for _, row in tqdm(disk_smart_data.iterrows(), total=len(disk_smart_data), desc=f"Processing {file_path}"):
        disk_id = row['disk_id']
        timestamp = row['ds']
        model = row['model']
        
        # Get failure data for the current disk
        disk_failures = failure_data[failure_data['disk_id'] == disk_id]
        
        # Check if there's a failure in the look_ahead_days window
        label = 0
        for _, failure in disk_failures.iterrows():
            failure_start = failure['failure_time'] - timedelta(days=look_ahead_days)
            if failure_start <= timestamp <= failure['failure_time']:
                label = 1
                break
                
        # Collect features and label (including the necessary columns)
        features = row[required_columns].values.flatten()
        processed_data.append([disk_id, timestamp, model, *features, label])
    
    # Convert the processed data to a DataFrame
    processed_df = pd.DataFrame(processed_data, columns=['disk_id', 'ds', 'model', *required_columns, 'label'])
    
    # Save the processed data to a new file
    output_file_path = os.path.join(output_folder, f"{os.path.basename(file_path).split('.')[0]}_processed.csv")
    processed_df.to_csv(output_file_path, index=False)
    print(f"Processed data saved to {output_file_path}")

# Main processing function with multiprocessing
def process_and_save_data_parallel(smart_data_base_folder, failure_data, look_ahead_days, output_folder):
    # Get list of all files in the SMART data folder (sorted by date)
    all_files = [os.path.join(smart_data_base_folder, f) for f in os.listdir(smart_data_base_folder) if f.endswith('.csv')]
    
    # Use multiprocessing Pool to process files in parallel
    with multiprocessing.Pool(processes=8) as pool:
        # Process each file in parallel
        pool.starmap(process_file, [(file_path, failure_data, look_ahead_days, output_folder) for file_path in all_files])

# Main execution
if __name__ == '__main__':
    failure_data = load_failure_data(failure_file_path)
    process_and_save_data_parallel(smart_data_base_folder, failure_data, look_ahead_days, output_folder)
