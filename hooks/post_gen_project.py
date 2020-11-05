import os


if __name__ == '__main__':
    print(os.popen("git init").read())
    print(os.popen("git add .").read())
    print(os.popen("git commit -m 'Initial commit'").read())
    print("""
    To add your project to the Corning Toolchain GitLab, please execute the following commands:

    git remote add origin https://<your_id>:<your_token>@gitlab.toolchain.corning.com/kairos/applications/{{cookiecutter.project_slug}}.git
    
    git push -u origin master
            
    """)