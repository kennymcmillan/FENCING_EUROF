import os
import re
import pandas as pd

# Path to the folder containing the XML files
folder_path = 'xml_links'
csv_file_path = 'HISTORICAL_Eurofencing_fencers.csv'
log_file_path = 'parse_errors.log'

# Batch size for processing and saving data
batch_size = 100  # Adjust batch size based on available memory

# If the CSV file already exists, delete it
if os.path.exists(csv_file_path):
    os.remove(csv_file_path)
    print(f"Deleted existing file: {csv_file_path}")

# If the log file already exists, delete it
if os.path.exists(log_file_path):
    os.remove(log_file_path)
    print(f"Deleted existing log file: {log_file_path}")

# Set to keep track of unique FencerIDs to avoid duplicates
unique_fencer_ids = set()

# List to store fencer data
fencer_data = []

# Function to log errors
def log_error(xml_file, error_message):
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Skipping file {xml_file}: {error_message}\n")

# Function to extract fencer data from the XML text
def extract_fencer_info(xml_text, xml_file):
    try:
        # Find all fencers in the XML using regular expressions
        tireurs = re.findall(r'<Tireur(.*?)\/>', xml_text)

        for tireur in tireurs:
            # Extract fencer attributes using regex groups
            fencer_id = re.search(r'ID="([^"]+)"', tireur)
            if fencer_id:
                fencer_id_value = fencer_id.group(1)
                # Skip duplicates based on FencerID
                if fencer_id_value in unique_fencer_ids:
                    continue
                unique_fencer_ids.add(fencer_id_value)
            else:
                fencer_id_value = None

            # Extract other fencer details (excluding Statut, Club, and Ligue)
            nom = re.search(r'Nom="([^"]+)"', tireur)
            prenom = re.search(r'Prenom="([^"]+)"', tireur)
            date_naissance = re.search(r'DateNaissance="([^"]+)"', tireur)
            sexe = re.search(r'Sexe="([^"]+)"', tireur)
            nation = re.search(r'Nation="([^"]+)"', tireur)
            licence = re.search(r'Licence="([^"]+)"', tireur)
            lateralite = re.search(r'Lateralite="([^"]+)"', tireur)
            classement = re.search(r'Classement="([^"]+)"', tireur)

            # Store the fencer data in a dictionary
            fencer_info = {
                'FencerID': fencer_id_value,
                'Nom': nom.group(1) if nom else None,
                'Prenom': prenom.group(1) if prenom else None,
                'DateNaissance': date_naissance.group(1) if date_naissance else None,
                'Sexe': sexe.group(1) if sexe else None,
                'Nation': nation.group(1) if nation else None,
                'Licence': licence.group(1) if licence else None,
                'Lateralite': lateralite.group(1) if lateralite else None,
                'Classement': classement.group(1) if classement else None,
                'FileName': xml_file  # For reference to the XML file the fencer was found in
            }

            # Append the fencer info to the list
            fencer_data.append(fencer_info)

    except Exception as e:
        log_error(xml_file, str(e))

# Function to save data and reset the batch
def save_and_reset(batch_data):
    # Append new data to the existing CSV
    df_batch = pd.DataFrame(batch_data)
    if os.path.exists(csv_file_path):
        df_batch.to_csv(csv_file_path, mode='a', header=False, index=False)
    else:
        df_batch.to_csv(csv_file_path, mode='w', header=True, index=False)

    # Clear batch data from memory after saving
    batch_data.clear()

# Main function to process XML files
def process_files():
    # Get a list of all XML files in the folder
    xml_files = [f for f in os.listdir(folder_path) if f.endswith('.xml')]

    # Print the total number of files to process
    total_files = len(xml_files)
    print(f"Total files to process: {total_files}")

    # Loop through each XML file and process it
    for index, xml_file in enumerate(xml_files, start=1):
        file_path = os.path.join(folder_path, xml_file)

        # Print the current status
        print(f"Processing file {index}/{total_files}: {xml_file}")

        try:
            # Read the XML file as text
            with open(file_path, 'r', encoding='utf-8') as file:
                xml_text = file.read()

            # Extract fencer information from the text
            extract_fencer_info(xml_text, xml_file)

        except Exception as e:
            log_error(xml_file, str(e))
            continue

        # Save data in batches
        if len(fencer_data) >= batch_size:
            save_and_reset(fencer_data)

    # Save any remaining data after the loop completes
    if fencer_data:
        save_and_reset(fencer_data)

    print(f"Fencer database saved to {csv_file_path}")
    print(f"Check {log_file_path} for any errors.")

# Entry point
if __name__ == "__main__":
    process_files()
