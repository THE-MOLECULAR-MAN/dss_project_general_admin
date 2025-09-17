# update_all_dss_projects_on_github.py
# Tim H 2025
#
# This script iterates through all projects on a DSS instance and attempts to push all commits to GitHub
# for projects that are connected to GitHub.
# It is intended to be run as a scenario on a schedule
#
# https://developer.dataiku.com/latest/tutorials/devtools/using-api-with-git-project/index.html
# https://developer.dataiku.com/latest/api-reference/python/projects.html#dataikuapi.dss.project.DSSProjectGit.get_remote

import dataiku
from dataiku import pandasutils as pdu
import pandas as pd

client = dataiku.api_client()
projects = client.list_projects()

df_projects = pd.DataFrame(projects)

successful = set()
not_connected = set()
errored = set()

for iter_project_key in client.list_project_keys():
    proj = client.get_project(iter_project_key)
    project_git = proj.get_project_git()    
    r = project_git.get_remote()
    if r:
        # print(r)
        res_push = project_git.push()
        res_pull = project_git.pull()
        # print(res)
        if (not res_push.get('success',False)) or (not res_pull.get('success',False)):
            print(f"[ERROR] pushing {iter_project_key}")
            errored.add(iter_project_key)
            continue
        successful.add(iter_project_key)
    else:
        # print(f"{iter_project_key} is not connected to GitHub")
        not_connected.add(iter_project_key)
print(f"Successfully pushed {len(pushed)} projects: \n{pushed}\n")
print(f"{len(not_connected)} projects not connected to GitHub: \n{not_connected}")

if len(errored) > 0:
    print(f"Projects had errors when pushig:\n{errored}")
