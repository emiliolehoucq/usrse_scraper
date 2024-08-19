# Script to scrape US-RSE job board
# Emilio Lehoucq

####################################### Loading libraries #######################################

import os
from requests import get
from bs4 import BeautifulSoup
# LOCAL MACHINE
# from scrapers import get_selenium_response
# GITHUB ACTIONS
from scraper import get_selenium_response
from text_extractor import extract_text
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
from datetime import datetime
import logging
from time import sleep
import json

####################################### CONFIGURING LOGGING #######################################

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Logging configured")

####################################### SETTING PARAMETERS #######################################

ntries = 5
retry_delay = 10

####################################### DEFINING FUNCTION FOR THIS SCRIPT #######################################

def upload_file(element_id, file_suffix, content, folder_id, service, logger):
    """
    Function to upload a file to Google Drive.

    Inputs:
    - element_id: ID of the job post
    - file_suffix: suffix of the file name
    - content: content of the file
    - folder_id: ID of the folder in Google Drive
    - service: service for Google Drive
    - logger: logger

    Outputs: None

    Dependencies: from googleapiclient.http import MediaFileUpload, os
    """

    # Prepare the file name
    file_name = f"{element_id}_{file_suffix}.txt"
    logger.info(f"Inside upload_file: prepared the name of the file for the {file_suffix}")

    # Write the content to a temporary file
    with open(file_name, 'w') as temp_file:
        temp_file.write(content)
    logger.info(f"Inside upload_file: wrote the {file_suffix} string to a temporary file")

    # Prepare the file metadata
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    logger.info(f"Inside upload_file: prepared the file metadata for the {file_suffix}")

    # Prepare the file media
    media = MediaFileUpload(file_name, mimetype='text/plain')
    logger.info(f"Inside upload_file: prepared the file media for the {file_suffix}")

    # Upload the file to the Drive folder
    service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    logger.info(f"Inside upload_file: uploaded the file to the shared folder for the {file_suffix}")

    # Remove the temporary file after uploading
    os.remove(file_name)
    logger.info(f"Inside upload_file: removed the temporary file after uploading for the {file_suffix}")

    return None

logger.info("Defined function to upload files to Google Drive")

####################################### GET US-RSE JOB BOARD DATA #######################################

# Creating request for US-RSE job board
url_source = 'https://us-rse.org/jobs/'
response = get(url_source)
logger.info("Requested US-RSE job board data")

# Parse the HTML content of the website
soup = BeautifulSoup(response.text, 'html.parser')
logger.info("Parsed US-RSE job board data")

# Find the content that holds the job listings
content = soup.find_all('ol')
logger.info("Found US-RSE job board data")

# Get only the first two ordered lists, which are the job listings
job_lists = content[:2]
logger.info("Successfully got US-RSE job board data")

####################################### GET URLS THAT I ALREADY SCRAPED #######################################

# LOCAL MACHINE -- Set the environment variable for the service account credentials 
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

# Authenticate using the service account
# LOCAL MACHINE
# credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
# GITHUB ACTIONS
credentials = service_account.Credentials.from_service_account_info(json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')))
logger.info("Authenticated with Google Sheets")

# Create service
service = build("sheets", "v4", credentials=credentials)
logger.info("Created service for Google Sheets")

# Get the values from the Google Sheet
spreadsheet_id = "1cYG20-FbYPcxoseOuDL2BbwvKwA6UWK_xbLRN-7jEqE" # https://docs.google.com/spreadsheets/d/1cYG20-FbYPcxoseOuDL2BbwvKwA6UWK_xbLRN-7jEqE/edit?gid=0#gid=0
range_sheet="B1:B1000000" # B because I'm putting the URLs of the job posts in the second column
result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_sheet).execute()
rows = result.get("values", []) # Example output: [['test1'], ['abc'], ['123']]
logger.info("Got data from Google Sheets")

# Extracting the URLs from the list of lists
existing_urls = [row[0] for row in rows] # Example output: ['test1', 'abc', '123']
num_existing_urls = len(existing_urls)
logger.info("Successfully got URLs that I already scraped")

####################################### SCRAPE NEW DATA #######################################

# Creating list to store new data
data = []

# Extract data for each job listing
# Iterating over the two ordered lists (current RSE openings and related openings)
for job_list in job_lists:
    logger.info("Iterating over the two ordered lists")
    # Finding each of the items in the list (each job posting)
    job_postings = job_list.find_all('li')
    logger.info("Found each of the items in the list")
    # Iterating over each of the job postings for each of the two ordered lists
    for job_posting in job_postings:
        logger.info("Iterating over each of the job postings")
        # If one of the job postings fails, that's ok, move on to the next one, but try a couple of times
        for attempt in range(ntries):
            try:
                # Creating list to store each job posting
                job_data = []

                # Populating the list

                # ID
                id = num_existing_urls + len(data) + 1
                job_data.append(id)  # ID starts at 1
                logger.info(f"Populated ID: {id}")
                
                # URL job post
                url_job_post = job_posting.find('a')['href']
                job_data.append(url_job_post)
                logger.info(f"Populated URL job post: {url_job_post}")
                
                # Timestamp
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                job_data.append(ts)
                logger.info(f"Populated timestamp: {ts}")

                # If the URL is already in the dataset, skip it
                if url_job_post not in existing_urls:
                    logger.info("URL is not in the dataset")
                    
                    # Date posted
                    job_data.append(job_posting.find('em').text.replace("\xa0", " ").split(": ")[1])
                    logger.info("Populated date posted")
                    
                    # Position
                    job_data.append(job_posting.find('a').text)
                    logger.info("Populated position")
                    
                    # Organization
                    job_data.append(job_posting.text.split(": ")[1].replace("\xa0", " ").split(": ")[0].replace(" Posted", "").split(",")[0])
                    logger.info("Populated organization")
                    
                    # Location
                    location = ''.join(job_posting.text.split(": ")[1].replace("\xa0", " ").split(": ")[0].replace(" Posted", "").split(",")[1:])
                    job_data.append(location)
                    logger.info("Populated location")
                    
                    # Remote, flexible, and hybrid
                    remote = False
                    flexible = False
                    hybrid = False
                    if 'remote' in location.lower():
                        remote = True
                    if 'flexible' in location.lower():
                        flexible = True
                    if 'hybrid' in location.lower():
                        hybrid = True
                    job_data.append(remote)
                    job_data.append(flexible)
                    job_data.append(hybrid)
                    logger.info("Populated remote, flexible, and hybrid")
                    
                    # Source code
                    source_code = get_selenium_response(url_job_post, headless=True)
                    logger.info("Got source code")
                    job_data.append(source_code)
                    logger.info("Populated source code")
                    
                    # Text
                    text = extract_text(source_code)
                    logger.info("Got text")
                    job_data.append(text)
                    logger.info("Populated text")

                    # Appending the dictionary to the list with all the job postings
                    data.append(job_data)
                    logger.info("Appended the dictionary to the list with all the job postings")
                logger.info("Successfully populated the list with the job posting data. Breaking out of the loop.")
                break  # Break out of the loop if successful

            except Exception as e:
                logger.info(f"Attempt {attempt + 1} failed: {e}")
                if attempt < ntries - 1:  # If it's not the last attempt
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    sleep(retry_delay)
                else:
                    logger.info(f"Failed after {ntries} attempts. Moving on to the next job posting.")
                    continue
            
####################################### WRITE NEW DATA TO GOOGLE SHEETS #######################################

# Range to write the data
# "A"+str(len(existing_urls)+1) to make sure to not overwrite the existing data
# A through J because I have 10 columns in the dataset
range_sheet="A"+str(num_existing_urls+1)+":J1000000" 
logger.info("Prepared range to write the data")

# Body of the request
# The last two elements of each element in data are the source code and the text, which are not written to the Google Sheet
body={"values": [element[:-2] for element in data]}
logger.info("Prepared body of the request")

# Execute the request
result = service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    range=range_sheet,
    valueInputOption="USER_ENTERED",
    body=body
    ).execute()
logger.info("Wrote new data to Google Sheets")

####################################### WRITE NEW DATA TO GOOGLE DRIVE #######################################

# Note: if there's already a file with the same name in the folder, this code will add another with the same name

# Folder ID
folder_id = "1XI0Zy8aCBHIhGgff9VOSkSMSfr2BlcJT"  # https://drive.google.com/drive/u/3/folders/1XI0Zy8aCBHIhGgff9VOSkSMSfr2BlcJT

# Authenticate using the service account (for Google Drive, not Sheets)
service = build('drive', 'v3', credentials=credentials)
logger.info("Created service for Google Drive")
            
# Iterate over each of the job posts (list)
for element in data:
    logger.info("Iterating over each of the job posts")
    # Get the source code of the job post
    source_code = element[-2]
    logger.info("Got the source code of the job post")
    # Get the text of the job post
    text = element[-1]
    logger.info("Got the text of the job post")
    # Upload the source code to Google Drive
    upload_file(element[0], "source_code", source_code, folder_id, service, logger)
    # Upload the text to Google Drive
    upload_file(element[0], "text", text, folder_id, service, logger)

logger.info("Wrote new data (if available) to Google Drive. Script finished successfully.")
