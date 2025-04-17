import pandas as pd
import requests
import logging
import xml.etree.ElementTree as ET
import zipfile
import os
import re
import zipfile
import tempfile



# Tableau REST API Configuration
TABLEAU_SERVER_URL = "your companies tableau server url" #e.g. "https://example.tableau.com"
API_VERSION = "3.4" # Check if that's still up-to-date
PAT_NAME = "Your PAT Name" 
PAT_SECRET = "Your PAT Secret" 
SITE_ID = "abccompany"  
#workbook
WORKBOOK_NAME = "Your Workbook Name"  
WORKBOOK_PATH = "folder path you want to drop your workbooks in"
#mapping table
file_path = "file path and name of your mapping table"
df = pd.read_csv(file_path) 
#Name switchers old vs new workbook, replace if needed, e.g. "DEV" or "Pre-PROD"
curr_env = "QA"
new_env = "PROD"


# Authentication to Tableau Server / Tableau Cloud
def authenticate():
    url = f"{TABLEAU_SERVER_URL}/api/{API_VERSION}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": PAT_NAME,
            "personalAccessTokenSecret": PAT_SECRET,
            "site": {
                "contentUrl": SITE_ID
            }
        }
    }
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        
        # Log the response status code and raw response text for debugging
        logging.info(f"Response Status Code: {response.status_code}")
        logging.info(f"Response Text: {response.text}")  # This will show the raw XML response

        # Check if the response is successful (HTTP 200)
        if response.status_code != 200:
            raise Exception(f"Authentication failed: {response.status_code} {response.text}")
        
        # Parse the XML response
        root = ET.fromstring(response.text)
        
        # Extract the necessary information from the XML
        namespaces = {'ns': 'http://tableau.com/api'}
        credentials = root.find(".//ns:credentials", namespaces)
        if credentials is not None:
            auth_token = credentials.get("token")
            site_id = root.find(".//ns:site", namespaces).get("id")
            user_id = root.find(".//ns:user", namespaces).get("id")
            return auth_token, site_id, user_id
        else:
            raise Exception("Authentication failed: No credentials found in the response.")
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {str(e)}")
        raise Exception(f"Authentication request failed: {str(e)}")
    
    except ET.ParseError as e:
        logging.error(f"Failed to parse XML response: {str(e)}")
        raise Exception(f"Failed to parse XML response: {str(e)}")

def download_workbook(auth_token, site_id, workbook_name):
    url = f"{TABLEAU_SERVER_URL}/api/{API_VERSION}/sites/{site_id}/workbooks"
    headers = {'X-Tableau-Auth': auth_token}
    params = {'pageSize': 500} 
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        # Log the raw response to help debug
        logging.info(f"Response Status Code: {response.status_code}")
        logging.info(f"Response Text: {response.text}")  # This will show the raw XML response
        
        if response.status_code != 200:
            raise Exception(f"Error fetching workbooks: {response.status_code} {response.text}")
        
        # Parse the XML response
        try:
            root = ET.fromstring(response.text)
        except ET.ParseError as e:
            raise Exception(f"Failed to parse XML: {e}. Response: {response.text}")
        
        # Extract workbooks from the XML response
        namespaces = {'ns': 'http://tableau.com/api'}
        workbooks = root.findall(".//ns:workbook", namespaces)

        if not workbooks:
            raise Exception(f"Error: No workbooks found in the response: {response.text}")

        # Find the workbook with the specified name
        workbook_id = None
        for wb in workbooks:
            if wb.get('name') == workbook_name:
                workbook_id = wb.get('id')
                break

        if not workbook_id:
            raise Exception(f"Workbook '{workbook_name}' not found.")
        
        # Now download the workbook as a .twbx file
        download_url = f"{TABLEAU_SERVER_URL}/api/{API_VERSION}/sites/{site_id}/workbooks/{workbook_id}/content"
        download_response = requests.get(download_url, headers=headers)

        if download_response.status_code == 200:
            twbx_filename = os.path.join(WORKBOOK_PATH, workbook_name + ".twbx")
            with open(twbx_filename, "wb") as f:
                f.write(download_response.content)
            print(f"Workbook '{workbook_name}.twbx' downloaded successfully.")
            print(f"Workbook saved as: {twbx_filename}")
            return twbx_filename
        else:
            raise Exception(f"Error downloading workbook: {download_response.status_code} {download_response.text}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {str(e)}")
        raise Exception(f"Request failed while downloading workbook: {str(e)}")

    except ET.ParseError as e:
        logging.error(f"XML Parse error: {str(e)}")
        raise Exception(f"XML Parse error: {str(e)}")
    
def extract_twb_from_twbx(twbx_filename):
    with zipfile.ZipFile(twbx_filename, 'r') as zip_ref:
        # Extract all files in the .twbx
        extraction_folder = f"{twbx_filename}_updated"
        zip_ref.extractall(extraction_folder)

        # Locate the .twb file
        twb_filename = next((file for file in zip_ref.namelist() if file.endswith(".twb")), None)
        if twb_filename is None:
            raise Exception("No .twb file found inside the .twbx.")
        
        # Return the path to the .twb file
        twb_path = os.path.join(extraction_folder, twb_filename)
        print(f"Extracted '{twb_filename}' from '{twbx_filename}'.")
        return twb_path, extraction_folder
    

def update_twb_values(twb_filename, df):
    try:
        # Read the file as a raw text string to preserve formatting
        with open(twb_filename, "r", encoding="utf-8") as f:
            xml_string = f.read()
    except Exception as e:
        raise Exception(f"Failed to read file: {e}")

    replacements_made = []

    # Perform text-based replacements without changing formatting
    for index, row in df.iterrows():
        qa_caption = row['qa_caption']
        qa_iddbname = row['qa_iddbname']
        prod_caption = row['prod_caption']
        prod_iddbname = row['prod_iddbname']

        # Replace caption
        old_pattern = re.escape(f"caption='{qa_caption}'")
        new_pattern = f"caption='{prod_caption}'"
        if re.search(old_pattern, xml_string): #checks if the pattern exists in the XML string and only replaces it then 
            xml_string = re.sub(old_pattern, new_pattern, xml_string)
            replacements_made.append(("caption", qa_caption, prod_caption))

        # Replace id
        old_pattern = re.escape(f"id='{qa_iddbname}'")
        new_pattern = f"id='{prod_iddbname}'"
        if re.search(old_pattern, xml_string):
            xml_string = re.sub(old_pattern, new_pattern, xml_string)
            replacements_made.append(("id", qa_iddbname, prod_iddbname))

        # Replace dbname
        old_pattern = re.escape(f"dbname='{qa_iddbname}'")
        new_pattern = f"dbname='{prod_iddbname}'"
        if re.search(old_pattern, xml_string):
            xml_string = re.sub(old_pattern, new_pattern, xml_string)
            replacements_made.append(("dbname", qa_iddbname, prod_iddbname))    
    
    return twb_filename, xml_string
        

def write_updated_file(twb_filename, xml_string):
    try:
        with open(twb_filename, "w", encoding="utf-8") as f:
            f.write(xml_string)
    except Exception as e:
        raise Exception(f"Failed to write file: {e}")


def update_twbx_file(twbx_path, df, output_path=None):
    # Use the same name for output if none provided
    if output_path is None:
        base, _ = os.path.splitext(twbx_path)
        new_base = base.replace("QA", "PROD") if "QA" in base else base + "_PROD"
        output_path = new_base + ".twbx"

    # Create a temp directory 
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(twbx_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # Find the .twb file inside the extracted contents
        twb_filename = None
        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.endswith('.twb'):
                    twb_filename = os.path.join(root, file)
                    break

        if not twb_filename:
            raise FileNotFoundError("No .twb file found in the .twbx package.")

        # Update the .twb XML content
        _, updated_xml = update_twb_values(twb_filename, df)

        # Rename the .twb file to match new environment
        twb_dir = os.path.dirname(twb_filename)
        old_twb_name = os.path.basename(twb_filename)
        new_twb_name = old_twb_name.replace(curr_env, new_env) if curr_env in old_twb_name else "Updated_" + old_twb_name 
        new_twb_path = os.path.join(twb_dir, new_twb_name)

        # Step 3: Write updated XML into the new .twb file path
        write_updated_file(new_twb_path, updated_xml)

        # Optionally delete the old .twb if needed
        if new_twb_path != twb_filename and os.path.exists(twb_filename):
            os.remove(twb_filename)


        # Zip everything back into a new .twbx
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zip_out:
            for foldername, subfolders, filenames in os.walk(temp_dir):
                for filename in filenames:
                    file_path = os.path.join(foldername, filename)
                    arcname = os.path.relpath(file_path, temp_dir)  # Maintain original folder structure
                    zip_out.write(file_path, arcname)

    return output_path

# Main function
def main():
    # Step 1: Authenticate and get the auth token
    auth_token, site_id, user_id = authenticate()
    
    # Step 2: Download the workbook as .twbx
    twbx_filename = download_workbook(auth_token, site_id, WORKBOOK_NAME) 
    
    # Step 3: Update the .twbx file directly
    updated_twbx = update_twbx_file(twbx_filename, df)

    print(f"Process completed. Updated file: {updated_twbx}")
    

if __name__ == "__main__":
    main()