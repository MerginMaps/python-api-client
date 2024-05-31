from mergin.client import MerginClient
import os
import shutil
import traceback

TESTING_PROJECT_DATA_DIR = "/home/marcelkocisek/Documents/mergin-python-api-client/test_data"
PROJECT_DIR = "./test/test_data/test_editor_project"
TESTING_PROJECT = "test/plugin"

# remove folder PROJECT_DIR
if os.path.exists(PROJECT_DIR):
    shutil.rmtree(PROJECT_DIR)

mc = MerginClient(login="merginuser", password="Top_secret", url="http://127.0.0.1:5004")
mc.download_project(TESTING_PROJECT, PROJECT_DIR)
try:
    qgs_file = os.path.join(TESTING_PROJECT_DATA_DIR, "qgs_project.qgs")
    # copy qgis file to project folder
    shutil.copy(qgs_file, PROJECT_DIR)
    mc.push_project(PROJECT_DIR)

    new_qgs_file = os.path.join(PROJECT_DIR, "qgs_project.qgs")
    os.remove(new_qgs_file)
    mc.push_project(PROJECT_DIR)
except Exception as e:
    traceback.print_exc(e)
finally:
    if os.path.exists(PROJECT_DIR):
        shutil.rmtree(PROJECT_DIR)
