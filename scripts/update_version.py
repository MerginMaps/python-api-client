import os
import re
import argparse


def replace_in_file(filepath, regex, sub):
    with open(filepath, "r") as f:
        content = f.read()

    content_new = re.sub(regex, sub, content, flags=re.M)

    with open(filepath, "w") as f:
        f.write(content_new)


dir_path = os.path.dirname(os.path.realpath(__file__))
parser = argparse.ArgumentParser()
parser.add_argument("--version", help="version to replace")
args = parser.parse_args()
ver = args.version
print("using version " + ver)

about_file = os.path.join(dir_path, os.pardir, "mergin", "version.py")
print("patching " + about_file)
replace_in_file(about_file, '__version__\s=\s".*', '__version__ = "' + ver + '"')

setup_file = os.path.join(dir_path, os.pardir, "setup.py")
print("patching " + setup_file)
replace_in_file(setup_file, "version='.*", "version='" + ver + "',")
