# This script is used to get ALL historical match results from EuroFencing

# This script gets all XMLs from Eurofencing for both Individual and Teams. It saves all XMLs to a folder
# A script is then run to get all individual match results and save to .csv file 
# Another script is run to get all team results and save to .csv file
# Then script is run to push the .csv files into SQL database

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.options import Options
from selenium import webdriver
import time
import requests
from bs4 import BeautifulSoup
import json
import concurrent.futures
import warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime

import HISTORICAL_Eurofencing_XML_scrape_individuals
import HISTORICAL_Eurofencing_XML_scrape_teams
import HISTORICAL_EuroF_all_competitions
import HISTORICAL_EuroF_all_fencers

# Silence warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Initialize WebDriver and Options
options = Options()
options.headless = False

# Initialize the WebDriver
driver = webdriver.Edge(options=options)
driver.get("https://www.eurofencing.info/competitions/latest-results")

# Wait for the paginator and extract the maximum page number
wait = WebDriverWait(driver, 10)
page_elements = wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, '.paginator .page-link')))

# Extract maximum page number
max_page = max([int(page.text) for page in page_elements if page.text.isdigit()])

# Headers for the request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Referer': 'https://www.eurofencing.info/competitions/latest-results',
    'X-Requested-With': 'XMLHttpRequest',
    'Cookie': 'nette-samesite=1; PHPSESSID=il47n3nq8l4f7do99uegam1k77'
}

base_url = "https://www.eurofencing.info/ajax/_mod:EuroFencingApi/_handler:EuroFencingAjax/case:snippetHandler/structureId:3307/page:{}/?ajaxSnippet[]=EuroFencingApi_latestResult__latestResultGrid"

# Function to process each page
def fetch_page_data(page_num):
    url = base_url.format(page_num)
    print(f"Fetching data from {url}...")

    try:
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code == 200:
            json_data = json.loads(response.text)
            if "EuroFencingApi_latestResult__latestResultGrid" in json_data:
                html_content = json_data["EuroFencingApi_latestResult__latestResultGrid"]
                soup = BeautifulSoup(html_content, 'html.parser')
                a_tags = soup.find_all('a', href=True)
                hrefs = [a['href'] for a in a_tags if '/competitions/latest-results' in a['href']]
                return hrefs
        else:
            print(f"Failed to fetch page {page_num}, status code: {response.status_code}")
    except Exception as e:
        print(f"Error fetching page {page_num}: {e}")
    return []

# Use ThreadPoolExecutor for parallelization
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    # Create a list of tasks for each page
    results = executor.map(fetch_page_data, range(1, max_page + 1))

# Flatten the results and remove duplicates
all_hrefs = [href for result in results for href in result]
unique_hrefs = list(set(all_hrefs))

# Append the base URL to each href
unique_hrefs = ["https://www.eurofencing.info" + href for href in unique_hrefs]

# Close the WebDriver
driver.quit()

################################################################################################################

# Function to process each href, fetching the xml links
def fetch_xml_links(href):
    print(f"Fetching data from {href}...")
    try:
        # Send GET request to the URL
        response = requests.get(href, headers=headers, verify=False)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find the table with the class "table-fence"
            table = soup.find('table', class_='table-fence')

            if table:
                # Find all <a> tags with the class "xml"
                xml_links = []
                a_tags = table.find_all('a', class_='xml')
                for a_tag in a_tags:
                    xml_link = a_tag['href']
                    if not xml_link.startswith("http"):
                        xml_link = "https://www.eurofencing.info" + xml_link
                    xml_links.append(xml_link)
                return xml_links
            else:
                print(f"No table found in {href}")
        else:
            print(f"Failed to fetch {href}, status code: {response.status_code}")
    except Exception as e:
        print(f"Error fetching {href}: {e}")
        
        time.sleep(1)  # Add a 1-second delay after each request
    return []

# Use ThreadPoolExecutor for parallelization with 20 workers
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    # Create a list of tasks for each href
    results = executor.map(fetch_xml_links, unique_hrefs)

# Flatten the results
all_xml_links = [link for result in results for link in result]
all_xml_links = list(set(all_xml_links))

################################################################

# Directory where XML files will be stored
xml_dir = 'xml_links'

# Function to remove directory safely
def remove_directory_safely(directory):
    try:
        shutil.rmtree(directory)
        print(f"Directory '{directory}' removed successfully.")
    except PermissionError:
        print(f"PermissionError: The folder '{directory}' is being used by another process.")
        print("Retrying in 5 seconds...")
        time.sleep(5)
        try:
            shutil.rmtree(directory)
            print(f"Directory '{directory}' removed successfully on retry.")
        except Exception as e:
            print(f"Failed to remove directory: {e}")
    except Exception as e:
        print(f"Error removing directory: {e}")

# Check if the folder exists
if not os.path.exists(xml_dir):
    os.makedirs(xml_dir)
else:
    # If the folder exists, try to remove it safely
    remove_directory_safely(xml_dir)
    # Recreate the folder
    os.makedirs(xml_dir)

# Function to download a single XML file
def download_xml(link):
    file_name = os.path.join(xml_dir, os.path.basename(link))
    try:
        response = requests.get(link, verify=False)
        if response.status_code == 200:
            with open(file_name, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {file_name}")
        else:
            print(f"Failed to download {link}, status code: {response.status_code}")
    except Exception as e:
        print(f"Error downloading {link}: {e}")

# Use ThreadPoolExecutor to download the files in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    executor.map(download_xml, all_xml_links)

# Function to rename files with retry on PermissionError
def rename_file_with_retry(old_file_path, new_file_path, retries=5, wait_time=1):
    for attempt in range(retries):
        try:
            os.rename(old_file_path, new_file_path)
            print(f"Renamed: {old_file_path} -> {new_file_path}")
            break  # If rename is successful, exit the loop
        except PermissionError as e:
            print(f"Attempt {attempt + 1} failed. PermissionError: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Skipping file: {old_file_path}. Could not rename after {retries} attempts.")

# Rename the downloaded XML files
for file_name in os.listdir(xml_dir):
    if not file_name.lower().endswith('.xml'):
        old_file_path = os.path.join(xml_dir, file_name)
        new_file_name = file_name + '.xml'
        new_file_path = os.path.join(xml_dir, new_file_name)
        rename_file_with_retry(old_file_path, new_file_path)

print("All XML files have been downloaded.")

##################################################################

# Run all scrapes
HISTORICAL_Eurofencing_XML_scrape_individuals.process_files()
HISTORICAL_Eurofencing_XML_scrape_teams.process_files()

HISTORICAL_EuroF_all_competitions.process_files()
HISTORICAL_EuroF_all_fencers.process_files()
