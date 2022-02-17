import  git 
from git import Repo
"""repo.git.checkout('-b', 'C:\Devops\Devops_testing')
print("YES 1")
repo = git.Repo('C:\Devops\Devops_testing')
origin = repo.remote(name='origin')
origin.pull()
print("YES 2")
#repo.git.status()
repo.git.add('--all')
repo.git.commit('-m', 'commit message from python script')
origin = repo.remote(name='origin')
origin.push()"""

def git_commit_push():
    repo = git.Repo('C:\Devops\Devops_testing')
    origin = repo.remote(name='origin')
    origin.pull()
    print(repo.git.status())
    repo.git.add('--all')
    #changedFiles = repo.git.diff('HEAD~1..HEAD', name_only=True)
    t = repo.head.commit.tree
    changedFiles =repo.git.diff(t)
    #changedFiles = repo.git.diff()
    print("=====================================")
    print("changedFiles are :", changedFiles)    
    print("=====================================")  
    if (changedFiles):
        repo.git.commit('-m', 'SQL files')
        origin = repo.remote(name='origin')
        origin.push()
    else:
        print("No files updated")
git_commit_push()