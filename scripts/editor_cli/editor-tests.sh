##!/usr/bin/env bash

set -eu

. config.sh

echo "Using mergin url: $MERGIN_URL"
PROJECT_DIR="${PROJECT_DIR:-$PWD/project}"
OLD_PWD=$PWD

if [[ -z "$MERGIN_PASSWORD" || -z "$MERGIN_USERNAME" || -z "$PROJECT" ]]; then
	echo "Define logins in variables MERGIN_PASSWORD and MERGIN_USERNAME. Define project into PROJECT variable."
	exit 1
fi

if [[ -z "$EDITOR_PASSWORD" || -z "$EDITOR_USERNAME" ]]; then
    echo "Define logins in variables EDITOR_PASSWORD and EDITOR_USERNAME."
    exit 1
fi

echo "Current project permissions:"
share=$("${MERGIN_CLI_PATH[0]}" share $PROJECT)
echo "$share"
if [[ `echo $share | grep -c "merginuser "` -gt 0 ]]; then
    echo -e "\e[91mFAILED: Mergin user in readers list"
else
    echo -e "\e[32mPASSED: No mergin user"
fi
tput sgr0

echo "Add reader permission for user merginuser"
"${MERGIN_CLI_PATH[0]}" share-add $PROJECT "merginuser" --permissions reader
share=$("${MERGIN_CLI_PATH[0]}" share $PROJECT)
echo "$share"
if [[ ! `echo $share | grep -c "merginuser .*reader$"` -gt 0 ]]; then
    echo -e "\e[91mFAILED: Mergin user in readers list"
else
    echo -e "\e[32mPASSED: Mergin user is reader"
fi
tput sgr0

echo "Add writer permission for user merginuser"
"${MERGIN_CLI_PATH[0]}" share-add $PROJECT "merginuser" --permissions writer
share=$("${MERGIN_CLI_PATH[0]}" share $PROJECT)
echo "$share"
if [[ ! `echo $share | grep -c "merginuser .*writer"` -gt 0 ]]; then
    echo -e "\e[91mFAILED: Mergin user in writers list"
else
    echo -e "\e[32mPASSED: Mergin user is writer"
fi
tput sgr0

echo "Add owner permission for user merginuser"
"${MERGIN_CLI_PATH[0]}" share-add $PROJECT "merginuser" --permissions owner
share=$("${MERGIN_CLI_PATH[0]}" share $PROJECT)
echo "$share"
if [[ ! `echo $share | grep -c "merginuser .*owner"` -gt 0 ]]; then
    echo -e "\e[91mFAILED: Mergin user in owners list"
else
    echo -e "\e[32mPASSED: Mergin user is owner"
fi
tput sgr0

echo "Remove permission for user merginuser"
"${MERGIN_CLI_PATH[0]}" share-remove $PROJECT "merginuser"
share=$("${MERGIN_CLI_PATH[0]}" share $PROJECT)
echo "$share"
if [ `echo $share | grep -c "merginuser .*[owner|reader]"` -gt 0 ]; then
    echo -e "\e[91mFAILED: Mergin user in owners list, not removed properly"
else
    echo -e "\e[32mPASSED: Mergin user removed"
fi
tput sgr0

echo -e "Migration of project data $PROJECT"
rm -rf $PROJECT_DIR
"${MERGIN_CLI_PATH[0]}" download $PROJECT $PROJECT_DIR

echo -e "Login as editor $EDITOR_USERNAME and starting "
DEFINED_USERNAME=$MERGIN_USERNAME
DEFINED_PASSWORD=$MERGIN_PASSWORD
MERGIN_USERNAME=$EDITOR_USERNAME
MERGIN_PASSWORD=$EDITOR_PASSWORD

cd $PROJECT_DIR

echo "Try to push .qgs file"
cp $OLD_PWD/test_data/qgs_project.qgs $PROJECT_DIR
result=$("${MERGIN_CLI_PATH[0]}" push)
if [ `echo $result | grep -c "You do not have permissions for this project"` -gt 0 ]; then
    echo -e "\e[32mPASSED: Mergin user with editor can not add .qgs file"
else
    echo -e "\e[91mFAILED: Mergin user with editor can not add .qgs file"
fi
tput sgr0

"${MERGIN_CLI_PATH[0]}" reset

echo "Try to push .qpkg file"
cp $OLD_PWD/test_data/data.gpkg $PROJECT_DIR
result=$("${MERGIN_CLI_PATH[0]}" push)
if [ `echo $result | grep -c "Done"` -gt 0 ]; then
    echo -e "\e[32mPASSED: Mergin user with editor can add .qpkg file"
else
    echo -e "\e[91mFAILED: Mergin user with editor can add .qpkg file"
fi
tput sgr0

echo "Try to collect data into data.gpkg my_table table"
sqlite3 $PROJECT_DIR/data.gpkg "INSERT INTO my_table VALUES (NULL, 'this is test');"
result=$("${MERGIN_CLI_PATH[0]}" push)
if [ `echo $result | grep -c "Done"` -gt 0 ]; then
    echo -e "\e[32mPASSED: Mergin user with editor added row to .qpkg file"
else
    echo -e "\e[91mFAILED: Mergin user with editor was not able to add row to .qpkg file"
fi
tput sgr0

echo "Try to remove .qpkg file"
rm data.gpkg
result=$("${MERGIN_CLI_PATH[0]}" push)
echo $result
if [ `echo $result | grep -c "You do not have permissions for this project"` -gt 0 ]; then
    echo -e "\e[32mPASSED: Mergin user with editor can not remove .qpkg file"
else
    echo -e "\e[91mFAILED: Mergin user with editor was able to remove .qpkg file"
fi
tput sgr0
"${MERGIN_CLI_PATH[0]}" reset

echo "Try to drop column in data.gpkg my_table table"
sqlite3 $PROJECT_DIR/data.gpkg "ALTER TABLE my_table DROP COLUMN text"
result=$("${MERGIN_CLI_PATH[0]}" push)
if [ `echo $result | grep -c "You do not have permissions for this project"` -gt 0 ]; then
    echo -e "\e[32mPASSED: Mergin user with editor can not make non geodiff changes in .qpkg file"
else
    echo -e "\e[91mFAILED: Mergin user with editor was able to drop column and push"
fi
tput sgr0
"${MERGIN_CLI_PATH[0]}" reset

echo "Try to push .jpg file"
cp $OLD_PWD/test_data/photo.jpg $PROJECT_DIR
result=$("${MERGIN_CLI_PATH[0]}" push)
if [ `echo $result | grep -c "Done"` -gt 0 ]; then
    echo -e "\e[32mPASSED: Mergin user with editor can add .jpg and other files"
else
    echo -e "\e[91mFAILED: Mergin user with editor can add .jpg file"
fi
rm photo.jpg
result=$("${MERGIN_CLI_PATH[0]}" push)
if [ `echo $result | grep -c "Done"` -gt 0 ]; then
    echo -e "\e[32mPASSED: Mergin user with editor can remove .jpg and other files"
else
    echo -e "\e[91mFAILED: Mergin user with editor can remove .jpg and other files"
fi
tput sgr0


# reset to old values
MERGIN_USERNAME=$DEFINED_USERNAME
MERGIN_PASSWORD=$DEFINED_PASSWORD
rm data.gpkg
"${MERGIN_CLI_PATH[0]}" push

cd $OLD_PWD
rm -rf $PROJECT_DIR
exit 0
