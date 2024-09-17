import os
import re
import pandas as pd

# Path to the folder containing the XML files
folder_path = 'xml_links'
csv_file_path = 'HISTORICAL_Eurofencing_competitions.csv'
log_file_path = 'parse_errors.log'

# If the CSV file already exists, delete it
if os.path.exists(csv_file_path):
    os.remove(csv_file_path)
    print(f"Deleted existing file: {csv_file_path}")

# If the log file already exists, delete it
if os.path.exists(log_file_path):
    os.remove(log_file_path)
    print(f"Deleted existing log file: {log_file_path}")

# List to store competition data
competition_data = []

# Function to log errors
def log_error(xml_file, error_message):
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"Skipping file {xml_file}: {error_message}\n")

# Function to extract competition data from the XML text
def extract_competition_info(xml_text, xml_file):
    try:
        # Search for the root tag to determine competition type
        if '<CompetitionIndividuelle' in xml_text or '<BaseCompetitionIndividuelle' in xml_text:
            competition_type = 'Individual'
        elif '<CompetitionParEquipes' in xml_text:
            competition_type = 'Team'
        else:
            raise ValueError("Unknown competition type")

        # Extract competition attributes using regular expressions
        competition_id = re.search(r'ID="([^"]+)"', xml_text)
        arme = re.search(r'Arme="([^"]+)"', xml_text)
        sexe = re.search(r'Sexe="([^"]+)"', xml_text)
        domaine = re.search(r'Domaine="([^"]+)"', xml_text)
        federation = re.search(r'Federation="([^"]+)"', xml_text)
        categorie = re.search(r'Categorie="([^"]+)"', xml_text)
        date = re.search(r'Date="([^"]+)"', xml_text)
        titre_court = re.search(r'TitreCourt="([^"]+)"', xml_text)
        titre_long = re.search(r'TitreLong="([^"]+)"', xml_text)

        # Store the results in a dictionary
        competition_info = {
            'CompetitionID': competition_id.group(1) if competition_id else None,
            'Arme': arme.group(1) if arme else None,
            'Sexe': sexe.group(1) if sexe else None,
            'Domaine': domaine.group(1) if domaine else None,
            'Federation': federation.group(1) if federation else None,
            'Categorie': categorie.group(1) if categorie else None,
            'Date': date.group(1) if date else None,
            'TitreCourt': titre_court.group(1) if titre_court else None,
            'TitreLong': titre_long.group(1) if titre_long else None,
            'CompetitionType': competition_type,
            'FileName': xml_file
        }

        return competition_info

    except Exception as e:
        log_error(xml_file, str(e))
        return None

# Main function to process XML files
def process_files():
    # Get a list of all XML files in the folder
    xml_files = [f for f in os.listdir(folder_path) if f.endswith('.xml')]

    # Loop through each XML file and process it
    for index, xml_file in enumerate(xml_files, start=1):
        file_path = os.path.join(folder_path, xml_file)

        try:
            # Read the XML file as text
            with open(file_path, 'r', encoding='utf-8') as file:
                xml_text = file.read()

            # Extract competition information from the text
            competition_info = extract_competition_info(xml_text, xml_file)

            if competition_info:
                # Append the competition info to the list
                competition_data.append(competition_info)

        except Exception as e:
            log_error(xml_file, str(e))
            continue

    # Create a DataFrame from the list of competition data
    df_competitions = pd.DataFrame(competition_data)

    # Save the DataFrame to CSV
    df_competitions.to_csv(csv_file_path, mode='w', header=True, index=False)
    print(f"Competition information saved to {csv_file_path}")
    print(f"Check {log_file_path} for any errors.")

# Entry point
if __name__ == "__main__":
    process_files()

