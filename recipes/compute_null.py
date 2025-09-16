# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import os
import dataiku
from concurrent.futures import ThreadPoolExecutor

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Configuration
force_rebuild_env = False

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
client = dataiku.api_client()
code_envs = client.list_code_envs()

failed_builds = set()
successful_builds = set()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def _process_code_env(code_env_info):
    try:
        client = dataiku.api_client()
        envName = code_env_info['envName']
        code_env = client.get_code_env(code_env_info['envLang'], envName)

        # rebuild it
        # print(f'Starting rebuilding {envName} ...')
        # env_path = os.path.join('/data/dataiku/dss_data/code-envs/python', envName)

        res = code_env.update_packages(force_rebuild_env=force_rebuild_env)

        if res['messages']['success']:
            print(f'Success: {envName}')
            successful_builds.add(envName)
        else:
            print(f"FAILED: {envName}")
            failed_builds.add(envName)

    except Exception as e:
        try:
            if not force_rebuild_env:
                # print(f'Failed to build {envName} without force rebuild, trying again with force rebuild...')
                res = code_env.update_packages(force_rebuild_env=True)
                print(f'SUCCESS on second attempt when force rebuilding {envName}')
                pass
        except Exception as e:
            print(f"Still failed to build {envName}, even with force rebuild:\n   {e}")
            failed_builds.add(envName) # potential bug where this doesn't happen, should use a finally clause
            pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# >2 multi-threading causes /tmp to balloon in disk space from pip, causes / root file system to have 0% free space
# Fleet Manager instances only have a root file system size of 31 GB, 21 of which are used by base OS
# /tmp is mounted to /
max_workers = 1 # os.cpu_count() or 1
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    executor.map(_process_code_env, code_envs)
    
print('\nFinished.')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# This should probably be in the Finally clause above?
import os
import shutil
import glob

for d in glob.glob('/tmp/pip-*'):
    print(f'Deleting {d}...')
    if os.path.isdir(d):
        shutil.rmtree(d, ignore_errors=False)
    else:
        os.remove(d)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
if len(failed_builds) > 0:
    failed_str = "\n".join(sorted(failed_builds, key=str.lower))
    
    print(f"\n\n\nEnvironments that failed to build:\n\n{failed_str}")

print('\n\n\nFinished rebuilding code environments from scratch')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
successful_builds