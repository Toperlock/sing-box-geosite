import pandas as pd
import concurrent.futures
import os
import json

def read_csv_and_append(link):
    return pd.read_csv(link, header=None, names=['pattern', 'address', 'other'], on_bad_lines='warn')

def parse_list_file(link, output_directory):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # 使用executor.map并行处理链接
        results = list(executor.map(read_csv_and_append, [link]))
        # 拼接为一个DataFrame
        df = pd.concat(results, ignore_index=True)

    # 删除pattern中包含#号的行
    df = df[~df['pattern'].str.contains('#')].reset_index(drop=True)

    # 映射字典
    map_dict = {'DOMAIN-SUFFIX': 'domain_suffix', 'HOST-SUFFIX': 'domain_suffix', 'DOMAIN': 'domain', 'HOST': 'domain', 'host': 'domain',
                'DOMAIN-KEYWORD':'domain_keyword', 'HOST-KEYWORD': 'domain_keyword', 'host-keyword': 'domain_keyword', 'IP-CIDR': 'ip_cidr',
                'ip-cidr': 'ip_cidr', 'IP-CIDR6': 'ip_cidr', 
                'IP6-CIDR': 'ip_cidr','SRC-IP-CIDR': 'source_ip_cidr', 'GEOIP': 'geoip', 'DST-PORT': 'port',
                'SRC-PORT': 'source_port', "URL-REGEX": "domain_regex"}

    # 删除不在字典中的pattern
    df = df[df['pattern'].isin(map_dict.keys())].reset_index(drop=True)

    # 删除重复行
    df = df.drop_duplicates().reset_index(drop=True)
    # 替换pattern为字典中的值
    df['pattern'] = df['pattern'].replace(map_dict)

    # 创建自定义文件夹
    os.makedirs(output_directory, exist_ok=True)

    result_rules = {"version": 1, "rules": []}

    for pattern, addresses in df.groupby('pattern')['address'].apply(list).to_dict().items():
        rule_entry = {pattern: [address.strip() for address in addresses]}
        result_rules["rules"].append(rule_entry)

    # 使用 output_directory 拼接完整路径
    file_name = os.path.join(output_directory, f"{os.path.basename(link).split('.')[0]}.json")
    with open(file_name, 'w', encoding='utf-8') as output_file:
        json.dump(result_rules, output_file, ensure_ascii=False, indent=2)

    return file_name

# 读取 links.txt 中的每个链接并生成对应的 JSON 文件
with open("../links.txt", 'r') as links_file:
    links = links_file.read().splitlines()

output_dir = "./"
result_file_names = []

for link in links:
    result_file_name = parse_list_file(link, output_directory=output_dir)
    result_file_names.append(result_file_name)

# 打印生成的文件名
# for file_name in result_file_names:
    # print(file_name)
