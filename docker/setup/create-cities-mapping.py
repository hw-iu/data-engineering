#!/usr/bin/env python3
#
# ©2026 Henri Wahl
#
# Create some random mapping between IDs and cities to be used for Kafka topic enrichment
#

import json
from sys import argv

# Input file name is needed
if len(argv) != 2:
    exit('Usage: create-cities-mapping.py <path-to-de.json>')

input_file = argv[1]

# Convert JSOn input to Python
with open(input_file) as json_input__file:
    cities_data = json.load(json_input__file)

id_counter = 0
cities_by_id = {}

# Create cheap dictionary mapping between IDs and cities
for city_data in cities_data:
    cities_by_id[id_counter + 1] = city_data.get('city')
    id_counter += 1

# Output for further processing
print(json.dumps(cities_by_id, indent=4, ensure_ascii=False))
