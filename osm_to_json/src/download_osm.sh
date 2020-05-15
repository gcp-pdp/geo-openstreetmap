#!/usr/bin/env bash
set -e

OSM_GCS_PATH="$1"
OSM_DEST_PATH="$2"

gsutil cp ${OSM_GCS_PATH} ${OSM_DEST_PATH}