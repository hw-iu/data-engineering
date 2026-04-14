#!/usr/bin/env python3
#
# ©2026 Henri Wahl
#
# Slightly overkill but https://simplemaps.com/static/data/country-cities/de/de.json
# needs to be downloaded by a Javascript-capable browser
#

from playwright.sync_api import sync_playwright
from os import environ
from sys import (argv,
                 exit)

try:
    # either get URL from command line argument or from environment variable
    url = environ.get('URL', argv[1])
except IndexError:
    exit('Usage: curl-browser.py <URL> or set $URL first')

with sync_playwright() as playwright:
    # open playwright context...
    context = playwright.request.new_context()
    # ...request needed URL...
    result = context.get(url, timeout=60000)
    # ...return desired content of URL
    print(result.text())
    context.dispose()