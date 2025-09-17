# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Rebuild all code environments
# 
# This Python Notebook creates a list of all Code Environments, then attempts to update each of them. If one fails to update, then it will retry with the "Force Rebuild" enabled which will clear the entire code environment before rebuilding it.
# 
# This has been tested and validated on Dataiku DSS v14.1.2, running on Alma and Ubuntu Linux. It should work with the "Python 3 (ipykernel)" built-in code environment.
# 
# Written by Tim H - Sept 2025

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import shutil
import glob
import os
import dataiku
from concurrent.futures import ThreadPoolExecutor

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Configuration

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Leave this as false and it will automatically attempt to switch it to True if the build fails
force_rebuild_env = False

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
client = dataiku.api_client()
code_envs = client.list_code_envs()

failed_builds = set()
successful_builds = set()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def clear_pip_tmp():
    # This function deletes all the temporary files created by Pip during the installation process. They are not always cleared and when dealing with dozens of Code Environments, it can fill up the hard disk very quickly.
    
    for d in glob.glob('/tmp/pip-*'):
        # print(f'Deleting {d}...')
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)
        else:
            os.remove(d)

def _process_code_env(code_env_info):
    try:
        client = dataiku.api_client()
        envName = code_env_info['envName']
        code_env = client.get_code_env(code_env_info['envLang'], envName)

        # print(f'Starting rebuilding {envName} ...')
        # env_path = os.path.join('/data/dataiku/dss_data/code-envs/python', envName)

        # rebuild it
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
                print(f'Success: {envName} when Force rebuild')
                pass
        except Exception as e:
            print(f"FAILED: {envName}, even with force rebuild")
            failed_builds.add(envName) # potential bug where this doesn't happen, should use a finally clause
            pass

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# >2 multi-threading causes /tmp to balloon in disk space from pip, causes / root file system to have 0% free space
# Fleet Manager instances only have a root file system size of 31 GB, 21 of which are used by base OS
# /tmp is mounted to /
clear_pip_tmp() # in case I stopped the last run
max_workers = 1 # os.cpu_count() or 1
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    executor.map(_process_code_env, code_envs)
    
print('\nFinished.')
clear_pip_tmp() # clean up

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
failed_builds

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
successful_builds