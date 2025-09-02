import os
import json
import dataiku
import os
from concurrent.futures import ThreadPoolExecutor

def get_directory_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path, followlinks=False):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            try:
                if not os.path.islink(file_path):  # skip symlinks
                    total_size += os.path.getsize(file_path)
            except FileNotFoundError:
                continue
    return total_size

def human_readable_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} EB"

def _process_code_env(code_env_info):
    try:
        # env_number += 1
        envName = code_env_info['envName']
        code_env = client.get_code_env(code_env_info['envLang'], envName)

        # rebuild it from scratch
        print(f'Starting rebuilding {envName} ...')
        env_path = os.path.join('/data/dataiku/dss_data/code-envs/python', envName)     
       
        size_before_bytes = get_directory_size(env_path)
        res = code_env.update_packages(force_rebuild_env=True)

        if not res['messages']['success']:
            failed_builds.append(envName)
            print(res)
        else:
            size_after_bytes = get_directory_size(env_path)
            # print(f"{envName} size before: {human_readable_size(size_before_bytes)}  Size after: {human_readable_size(size_after_bytes)}\n")
            sum_size_before_bytes += size_before_bytes
            sum_size_after_bytes  += size_after_bytes
       
    except Exception as e:
        print(f"Exception: {e}")
        # continue
        pass


client = dataiku.api_client()
code_envs = client.list_code_envs()

sum_size_before_bytes = 0
sum_size_after_bytes = 0
# env_number = 0
# total_num_env = len(code_envs)
failed_builds = []

max_workers = os.cpu_count() or 1
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    executor.map(_process_code_env, code_envs)
if len(failed_builds) > 0:
    print(f'Environments that failed to build: {failed_builds}')

print('Finished rebuilding all code environments from scratch\nTotal size before: {human_readable_size(sum_size_before_bytes)\n Total size after: {human_readable_size(sum_size_after_bytes)}\nChange: {(sum_size_after_bytes-sum_size_before_bytes)/sum_size_before_bytes}%')