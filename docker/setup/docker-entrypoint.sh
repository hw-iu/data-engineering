#!/usr/bin/env bash
set -e

# Name of the dataset ZIP file on Kaggle
DATA_FILE_NAME="the-emsx-dataset-historical-photovoltaic-and-load"
# /data is the mounted volume for the dataset
DATA_DIR="/data"
# The mapping of IDs to city names is generated from simplemaps data
DATA_DIR_SIMPLEMAPS="${DATA_DIR}/simplemaps"
# The mapppig of IDs to city names lies as JSON file here
DATA_DIR_CITIES_IDS="${DATA_DIR}/cities_ids"
# The raw dataset will be extracted to /data/emsx_dataset and sorted in place
DATA_DIR_EMSX_RAW="${DATA_DIR}/emsx_dataset"
# Sorting is necessary because of the CSV files partly have scrambled date order
DATA_DIR_ESMX_SORTED="${DATA_DIR_EMSX_RAW}/sorted_temp"
# The expected SHA256 checksum of the ZIP file to verify integrity
EMSX_SHA256SUM_WANTED="7dec68cd9fd00fa7ccca651e82640b8f7a2f67d7c4f4e67cec64ce563341362c"

# First setup the city mapping
echo "Setting up city ID to name mapping..."
mkdir -p ${DATA_DIR_SIMPLEMAPS}
mkdir -p ${DATA_DIR_CITIES_IDS}
# Download the simplemaps dataset for Germany, which contains the city names and their IDs
# Only possible with a browser due to Javascript
curl-browser.py https://simplemaps.com/static/data/country-cities/de/de.json > ${DATA_DIR_SIMPLEMAPS}/de.json
# Create te mapping of city IDs to city names to be used in Kafka topic data enrichment
create-cities-mapping.py ${DATA_DIR_SIMPLEMAPS}/de.json > ${DATA_DIR_CITIES_IDS}/mapping.json
exit 1

# Now check if the dataset ZIP file already exists and is valid
if [ ! -f ${DATA_DIR}/${DATA_FILE_NAME}.zip ]; then
    echo "Need to download the EMSX dataset from Kaggle..."
    DOWNLOAD_NEEDED=true
else
    echo "Verifying the integrity of the existing ZIP file..."
    SHA256SUM_CURRENT=$(sha256sum ${DATA_DIR}/${DATA_FILE_NAME}.zip | awk '{print $1}')
    if [ "${SHA256SUM_CURRENT}" == "${EMSX_SHA256SUM_WANTED}" ]; then
        echo "File is valid. No need to download."
        DOWNLOAD_NEEDED=false
    else
        echo "File ${DATA_DIR}/${DATA_FILE_NAME}.zip is corrupted. Removing it and downloading again."
        rm ${DATA_DIR}/${DATA_FILE_NAME}.zip
        DOWNLOAD_NEEDED=true
    fi
fi

if [ "$DOWNLOAD_NEEDED" = true ]; then
    echo
    echo "Downloading the EMSX dataset from Kaggle..."
    curl -L -o ${DATA_DIR}/${DATA_FILE_NAME}.zip  https://www.kaggle.com/api/v1/datasets/download/adri1g/${DATA_FILE_NAME}
    echo "Verifying the integrity of the downloaded ZIP file..."
    SHA256SUM_CURRENT=$(sha256sum ${DATA_DIR}/${DATA_FILE_NAME}.zip | awk '{print $1}')
    if [ "${SHA256SUM_CURRENT}" == "${EMSX_SHA256SUM_WANTED}" ]; then
        echo "File is valid. Download successful."
    else
        echo "File ${DATA_DIR}/${DATA_FILE_NAME}.zip is corrupted. Exiting."
        exit 1
    fi
fi

# extract downloaded dataset ZIP file
unzip -n ${DATA_DIR}/${DATA_FILE_NAME}.zip -d ${DATA_DIR_EMSX_RAW}

# enter the raw dataset directory and sort all CSV files in place, preserving headers
cd ${DATA_DIR_EMSX_RAW}
# unneeded, rather disturbing for sorting
rm -f metadata.csv pv.csv
# create temporary directory for sorted files, sort each CSV file and move it back to the raw dataset directory
mkdir -p ${DATA_DIR_ESX_SORTED}
for CSV_FILE in *.csv; do
  echo "Sorting ${CSV_FILE}"
  # Preserve CSV headers
  head -n1 "${CSV_FILE}" > "${DATA_DIR_ESX_SORTED}/${CSV_FILE}"
  sort "${CSV_FILE}" | head -n-1 >> "${DATA_DIR_ESX_SORTED}/${CSV_FILE}"
  mv "${DATA_DIR_ESX_SORTED}/${CSV_FILE}" "${DATA_DIR_EMSX_RAW}/${CSV_FILE}"
  done

# remove temporary sorting directory
rm -rf ${DATA_DIR_ESX_SORTED}

echo "Dataset is ready."