# cat post_gen_project.py
import os
import shutil

print(os.getcwd())  # prints /absolute/path/to/{{cookiecutter.project_slug}}

def remove(filepath):
    if os.path.isfile(filepath):
        os.remove(filepath)
    elif os.path.isdir(filepath):
        shutil.rmtree(filepath)

cicd_tool = 'GitHub Actions'

if cicd_tool == 'GitHub Actions':
    # remove top-level file inside the generated folder
    remove('az_dev_ops')
if cicd_tool == 'Azure DevOps':
    # remove top-level file inside the generated folder
    remove('.github')

