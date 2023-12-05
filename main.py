import pandas as pd
import concurrent.futures
import os
import json

def read_csv_and_append(link):
    return pd.read_csv(link, header=None, names=['pattern', 'address', 'other'], on_bad_lines='warn')

def parse_list_file(list_links, output_directory):
    with open(list_links, 'r') as file:
        list_links = file.read().splitlines()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # 使用executor.map并行处理链接
        results = list(executor.map(read_csv_and_append, list_links))
        # 拼接为一个DataFrame
        df = pd.concat(results, ignore_index=True)

    # 删除pattern中包含#号的行
    df = df[~df['pattern'].str.contains('#')].reset_index(drop=True)

    # 映射字典
    map_dict = {'DOMAIN-SUFFIX': 'domain_suffix', 'HOST-SUFFIX': 'domain_suffix', 'DOMAIN': 'domain', 'HOST': 'domain', 'DOMAIN-KEYWORD': 'domain_keyword',
                'IP-CIDR': 'ip_cidr', 'IP-CIDR6': 'ip_cidr', 'SRC-IP-CIDR': 'source_ip_cidr', 'GEOIP': 'geoip', 'DST-PORT': 'port',
                'SRC-PORT': 'source_port', "URL-REGEX": "domain_regex"}

    # 删除不在字典中的pattern
    df = df[df['pattern'].isin(map_dict.keys())].reset_index(drop=True)

    # 删除重复行
    df = df.drop_duplicates().reset_index(drop=True)
    # 替换pattern为字典中的值
    df['pattern'] = df['pattern'].replace(map_dict)

    # 使用 groupby 分组并转化为字典
    result_dict = df.groupby('pattern')['address'].apply(list).to_dict()

    # 创建自定义文件夹
    os.makedirs(output_directory, exist_ok=True)

    file_names = []  # 存储生成的文件名
    for link in list_links:
        # 使用 output_directory 拼接完整路径
        file_name = os.path.join(output_directory, os.path.basename(link).split('.')[0] + '.json')
        file_names.append(file_name)
        with open(file_name, 'w', encoding='utf-8') as output_file:
            json.dump({"version": 1, "rules": [{"pattern": key, "address": value} for key, value in result_dict.items()]}, output_file, ensure_ascii=False, indent=2)

    return result_dict, file_names

list_of_links = "../links.txt"
output_dir = "./"
result_rules, file_names = parse_list_file(list_of_links, output_directory=output_dir)
