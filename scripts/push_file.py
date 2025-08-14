import mergin
import os
import random
import string

LOGIN="admin3"
PASSWORD="topsecret"
URL="http://localhost:8080"

client = mergin.MerginClient(
    url=URL,
    login=LOGIN,
    password=PASSWORD
)

PROJECT="mergin/hejj6"
LOCAL_FOLDER=f"/tmp/project{random.choices(string.ascii_letters + string.digits, k=10)}"
client.download_project(PROJECT, LOCAL_FOLDER)

# create dummy random named file to LOCAL_FOLDER
def create_dummy_file(folder, filename):
    file_path = os.path.join(folder, filename)
    with open(file_path, 'w') as f:
        f.write('This is a dummy file.\n' * (2 * (1024 * 1024)))
    return file_path

dummy_filename = ''.join(random.choices(string.ascii_letters + string.digits, k=10)) + '.txt'
dummy_file_path = create_dummy_file(LOCAL_FOLDER, dummy_filename)   
print(f"Dummy file created at: {dummy_file_path}")


client.push_project(LOCAL_FOLDER)
