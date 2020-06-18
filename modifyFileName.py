import os

path = 'xxxxx'
fileList = os.listdir(path)
for file in fileList:
    name = file.replace('xxxx', '')
    oldname = path + os.sep + file  # os.sep添加系统分隔符
    newname = newname = path + os.sep + name
    os.rename(oldname, newname)
    print(oldname, '=======>', newname)
