#!/usr/bin/env bash
# ©2026 Henri Wahl
#
# Runs the setup script to prepare the dataset and city mapping.
#

set -e

# Loads the environment variables from the .env file to get project version
. .env

# Actually run the setup in container, thereby creating the data directory
docker run \
        --interactive \
        --rm \
        --tty \
        --volume "$(pwd)/data:/data" \
        ghcr.io/hw-iu/data-engineering/setup:${PROJECT_VERSION}
