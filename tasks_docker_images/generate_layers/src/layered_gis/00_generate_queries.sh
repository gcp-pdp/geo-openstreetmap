#!/bin/sh

mkdir "../sql"
FOLDERS="$(find . -mindepth 1 -type d)"

for FOLDER in $FOLDERS; do
    cd $FOLDER || exit
    FILE="$(find *.sh)"
    echo "running " $FOLDER/$FILE
    bash $FILE
    cd ..
done