import os
import pandas as pd
import numpy as np
from datetime import timedelta
from tqdm import tqdm
import pickle

# 未来优化：可以通过调整存储的csv窗口大小来加速，但是需要考虑window

# Parameters
# smart_data_base_folder = '/mnt/raid5/sum/card/storage/StreamDFP/dataset/SMART'
failure_file_path = '/mnt/raid5/sum/card/storage/StreamDFP/dataset/ssd_failure_label.csv'
smart_data_base_folder = '/mnt/raid5/sum/card/storage/StreamDFP/dataset/processed_smart_data'

lookback_days = 20  # 前20天的数据
window_size = 10  # 每个窗口大小为10天
step_size = 5  # 每次滑动5天

separator = '<SEP>'

# 分批保存文件的基础名
output_file_prefix = "processed_data_part"


failure_file_path = '/mnt/raid5/sum/card/storage/StreamDFP/dataset/ssd_failure_label.csv'  # 设置为failure数据的路径

# 加载失败数据
failure_data = pd.read_csv(failure_file_path)
failure_data['failure_time'] = pd.to_datetime(failure_data['failure_time'])

# 首先对failure_data升序排序
failure_data = failure_data.sort_values(by='failure_time', ascending=True)

# 因为如果从20180101的错误开始计算，由于没有2018年之前的数据，所以这些输入会不匹配。因此我们直接从2018年1月22日的数据开始计算。
filter_failure_data = pd.to_datetime('2018-01-22')
failure_data = failure_data[failure_data['failure_time'] >= filter_failure_data]

end_date = failure_data.iloc[0].failure_time
start_date = max(end_date - timedelta(days=lookback_days - 1), pd.to_datetime('2018-01-01'))
print(start_date, end_date)
# 获取日期范围
date_range = pd.date_range(start=start_date, end=end_date)

# 读取并合并 CSV 文件
all_data = []

for date in tqdm(date_range):
    # 格式化日期为 YYYYMMDD
    date_str = date.strftime('%Y%m%d')
    file_name = f"{date_str}_processed.csv"
    
    try:
        # 读取 CSV 文件，并附加到 all_data 列表
        df = pd.read_csv(os.path.join(smart_data_base_folder, file_name))
        df['date'] = date_str  # 添加文件日期作为一列
        all_data.append(df)
    except FileNotFoundError:
        print(f"文件 {file_name} 未找到，跳过该文件")

range_csv = pd.concat(all_data, ignore_index=True)

total_data = []
save_count = 0
output_folder = '/mnt/raid5/sum/card/storage/StreamDFP/dataset/train'
for failure in tqdm(failure_data.itertuples(), total=len(failure_data)):
    # 如果当前时间 - lookback_days > start_date, 则计算超过的时间
    if failure.failure_time > end_date + timedelta(days=1):
        days = (failure.failure_time - end_date).days
        print(f'Failure time: {failure.failure_time}, days: {days}')
        
        # 生成要删除的日期范围
        delete_dates = [(start_date + timedelta(days=i)).strftime('%Y%m%d') for i in range(days)]

        # 删除 range_csv 中指定日期的数据
        range_csv = range_csv[~range_csv['date'].isin(delete_dates)]
        
        # 加载并加入 end_date+1 和 end_date+2 的数据
        for i in range(1, days + 1):
            new_date = end_date + timedelta(days=i)
            new_date_str = new_date.strftime('%Y%m%d')
            file_name = f"{new_date_str}_processed.csv"
            
            try:
                new_data = pd.read_csv(os.path.join(smart_data_base_folder, file_name))
                new_data['date'] = new_date_str
                range_csv = pd.concat([range_csv, new_data], ignore_index=True)
            except FileNotFoundError:
                print(f"文件 {file_name} 未找到，跳过该文件")

        # 更新 start_date 和 end_date
        start_date = start_date + timedelta(days=days)
        end_date = end_date + timedelta(days=days)

    # 从 failure 中获取 disk_id 和 model
    disk_id = failure.disk_id
    model = failure.model

    # 在 range_csv 中筛选出 disk_id 和 model 与当前 failure 相同的数据
    matching_data = range_csv[(range_csv['disk_id'] == disk_id) & (range_csv['model'] == model)]
    
    for start_idx in range(0, len(matching_data) - window_size + 1, step_size):
        end_idx = start_idx + window_size
        window = matching_data.iloc[start_idx:end_idx]  # 当前窗口数据
        
        # 序列化每个窗口的数据
        serialized_data = []
        for _, row in window.iterrows():
            # 将单天数据转换为 [ds, r_1, r_9, ...] 格式
            day_data = [row['ds']] + row.filter(regex='^r_').tolist()
            serialized_data.extend(day_data)  # 将单天数据加入序列
            serialized_data.append(separator)  # 插入分隔符
        
        serialized_data = serialized_data[:-1]  # 移除最后一个多余的分隔符
        
        total_data.append({'disk_id': disk_id, 'model': model, 'data': serialized_data, 'label': window.iloc[-1].label})  # 添加失败数据
    
    if len(total_data) >= 10000:
        # 保存数据
        with open(os.path.join(output_folder, f"{output_file_prefix}_{save_count}.pkl"), 'wb') as f:
            pickle.dump(total_data, f)
        print(f"保存 {len(total_data)} 条数据到 {output_folder}/{output_file_prefix}_{save_count}.pkl")
        save_count += 1
        total_data = []

if total_data:
    # 保存数据
    with open(os.path.join(output_folder, f"{output_file_prefix}_{save_count}_{len(total_data)}.pkl"), 'wb') as f:
        pickle.dump(total_data, f)
    print(f"保存 {len(total_data)} 条数据到 {output_folder}/{output_file_prefix}_{save_count}.pkl")
    save_count += 1
    total_data = []
    