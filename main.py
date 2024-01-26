import pandas as pd
import concurrent.futures
import os
import json
import requests
import yaml
import ipaddress

def read_yaml_from_url(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    yaml_data = yaml.safe_load(response.text)
    return yaml_data

def read_list_from_url(url):
    df = pd.read_csv(url, header=None, names=['pattern', 'address', 'other'], on_bad_lines='warn')
    return df

def is_ipv4_or_ipv6(address):
    try:
        ipaddress.IPv4Network(address)
        return 'ipv4'
    except ValueError:
        try:
            ipaddress.IPv6Network(address)
            return 'ipv6'
        except ValueError:
            return None

def parse_and_convert_to_dataframe(link):
    # 根据链接扩展名分情况处理
    if link.endswith('.yaml') or link.endswith('.txt'):
        try:
            yaml_data = read_yaml_from_url(link)
            rows = []
            if not isinstance(yaml_data, str):
                items = yaml_data.get('payload', [])
            else:
                lines = yaml_data.splitlines()
                line_content = lines[0]
                items = line_content.split()
            for item in items:
                address = item.strip("'")
                if ',' not in item:
                    if is_ipv4_or_ipv6(item):
                        pattern = 'IP-CIDR'
                    else:
                        if address.startswith('+'):
                            pattern = 'DOMAIN-SUFFIX'
                            address = address[1:]
                            if address.startswith('.'):
                                address = address[1:]
                        else:
                            pattern = 'DOMAIN'
                else:
                    pattern, address = item.split(',', 1)  
                rows.append({'pattern': pattern.strip(), 'address': address.strip(), 'other': None})
            df = pd.DataFrame(rows, columns=['pattern', 'address', 'other'])
        except:
            df = read_list_from_url(link)
    else:
        df = read_list_from_url(link)
    return df

# 对字典进行排序，含list of dict
def sort_dict(obj):
    if isinstance(obj, dict):
        return {k: sort_dict(obj[k]) for k in sorted(obj)}
    elif isinstance(obj, list) and all(isinstance(elem, dict) for elem in obj):
        return sorted([sort_dict(x) for x in obj], key=lambda d: sorted(d.keys())[0])
    elif isinstance(obj, list):
        return sorted(sort_dict(x) for x in obj)
    else:
        return obj

def parse_list_file(link, output_directory):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # 使用executor.map并行处理链接
        results = list(executor.map(parse_and_convert_to_dataframe, [link]))
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
    domain_entries = []

    for pattern, addresses in df.groupby('pattern')['address'].apply(list).to_dict().items():
        if pattern == 'domain_suffix':
            rule_entry = {pattern: ['.' + address.strip() for address in addresses]}
            result_rules["rules"].append(rule_entry)
            domain_entries.extend([address.strip() for address in addresses])
        elif pattern == 'domain':
            domain_entries.extend([address.strip() for address in addresses])
        else:
            rule_entry = {pattern: [address.strip() for address in addresses]}
            result_rules["rules"].append(rule_entry)
    # 删除 'domain_entries' 中的重复值
    domain_entries = list(set(domain_entries))
    if domain_entries:
        result_rules["rules"].insert(0, {'domain': domain_entries})

    # 使用 output_directory 拼接完整路径
    file_name = os.path.join(output_directory, f"{os.path.basename(link).split('.')[0]}.json")
    with open(file_name, 'w', encoding='utf-8') as output_file:
        json.dump(sort_dict(result_rules), output_file, ensure_ascii=False, indent=2)

    srs_path = file_name.replace(".json", ".srs")
    os.system(f"sing-box rule-set compile --output {srs_path} {file_name}")
    return file_name

# 读取 links.txt 中的每个链接并生成对应的 JSON 文件
with open("../links.txt", 'r') as links_file:
    links = links_file.read().splitlines()

links = [l for l in links if l.strip() and not l.strip().startswith("#")]

output_dir = "./"
result_file_names = []

for link in links:
    result_file_name = parse_list_file(link, output_directory=output_dir)
    result_file_names.append(result_file_name)

# 打印生成的文件名
# for file_name in result_file_names:
    # print(file_name)
