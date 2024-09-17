import os
import xml.etree.ElementTree as ET
import pandas as pd

# Path to the folder containing the XML files
folder_path = 'xml_links'
csv_file_path = 'HISTORICAL_EuroF_individual_match_data.csv'

if os.path.exists(csv_file_path):
    os.remove(csv_file_path)
    print(f"Deleted existing file: {csv_file_path}")

# Function to clean up unwanted characters before <?xml> and normalize Tireur/Tireurs
def clean_xml(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        xml_content = file.read()
    
    # Find the position of the "<?xml" declaration and remove anything before it
    xml_declaration_pos = xml_content.find('<?xml')
    
    if xml_declaration_pos > 0:
        # Strip unwanted characters before the XML declaration
        xml_content = xml_content[xml_declaration_pos:]
    
    # Normalize "tireur" and "tireurs" to be "Tireur" and "Tireurs"
    xml_content = xml_content.replace('<tireur', '<Tireur').replace('</tireur', '</Tireur')
    xml_content = xml_content.replace('<tireurs', '<Tireurs').replace('</tireurs', '</Tireurs')

    return xml_content

# Main function to process XML files
def process_files():
    # Get a list of all XML files in the folder
    xml_files = [f for f in os.listdir(folder_path) if f.endswith('.xml')]

    # Loop through each XML file and process it
    for index, xml_file in enumerate(xml_files, start=1):
        file_path = os.path.join(folder_path, xml_file)
        
        try:
            # Clean up unwanted characters and parse the XML string
            cleaned_xml = clean_xml(file_path)
            root = ET.fromstring(cleaned_xml)
        except ET.ParseError as e:
            print(f"Skipping file {index} ({xml_file}) due to a ParseError: {e}")
            continue

        # Check if the root tag is 'CompetitionIndividuelle' before proceeding
        if root.tag == 'CompetitionIndividuelle':
            print(f"Processing file {index} out of {len(xml_files)}: {xml_file}")

            # Extract specific competition attributes with fallback to None if they don't exist
            competition_info = {
                'Championnat': root.get('Championnat', None),
                'CompetitionID': root.get('ID', None),
                'Annee': root.get('Annee', None),
                'Arme': root.get('Arme', None),
                'Sexe': root.get('Sexe', None),
                'Domaine': root.get('Domaine', None),
                'Federation': root.get('Federation', None),
                'Categorie': root.get('Categorie', None),
                'Date': root.get('Date', None),
                'TitreCourt': root.get('TitreCourt', None),
                'TitreLong': root.get('TitreLong', None)
            }

            ########################### GET TABLE OF FENCERS ##########################

            tireurs = root.find('Tireurs')
            tireur_data = []

            # Check if the 'Tireurs' element exists before processing it
            if tireurs is not None:
                # Loop through each Tireur element and extract the relevant attributes
                for tireur in tireurs.findall('Tireur'):
                    tireur_data.append({
                        'ID': tireur.get('ID'),
                        'Nom': tireur.get('Nom'),
                        'Prenom': tireur.get('Prenom'),
                        'DateNaissance': tireur.get('DateNaissance'),
                        'Sexe': tireur.get('Sexe'),
                        'Lateralite': tireur.get('Lateralite'),
                        'Nation': tireur.get('Nation'),
                        'Club': tireur.get('Club'),
                        'Licence': tireur.get('Licence'),
                        'Classement': tireur.get('Classement'),
                        'Statut': tireur.get('Statut')
                    })
            else:
                print(f"Skipping fencer data for file {xml_file} as no 'Tireurs' element was found.")

            df_tireurs = pd.DataFrame(tireur_data)

            # Drop duplicates based on the 'ID' column to ensure uniqueness
            df_tireurs = df_tireurs.drop_duplicates(subset='ID')

            ################### GET POULES SUMMARY DATA , WHO QUALIFIED ################

            # Check if the Phases element exists
            phases = root.find('Phases')
            if phases is not None:
                tour_de_poules_data = []

                # Loop through each 'TourDePoules' element and extract relevant information
                for tour_de_poules in phases.findall('TourDePoules'):
                    for poule in tour_de_poules.findall('Poule'):
                        for tireur in poule.findall('Tireur'):
                            tour_de_poules_data.append({
                                'PhaseID': tour_de_poules.get('PhaseID'),
                                'PouleID': poule.get('ID'),
                                'TireurREF': tireur.get('REF'),
                                'NbVictoires': tireur.get('NbVictoires'),
                                'NbMatches': tireur.get('NbMatches'),
                                'TD': tireur.get('TD'),
                                'TR': tireur.get('TR'),
                                'RangPoule': tireur.get('RangPoule')
                            })

                # Convert the extracted data into a DataFrame
                df_poules_qualify = pd.DataFrame(tour_de_poules_data)

                ################ TABLEAUX SUMMARY ##############################

                tableaux_data = []

                # Loop through each 'PhaseDeTableaux' element and extract the relevant data
                for phase in phases.findall('PhaseDeTableaux'):
                    for tireur in phase.findall('Tireur'):
                        tableaux_data.append({
                            'PhaseID': phase.get('PhaseID'),
                            'Tireur_REF': tireur.get('REF'),
                            'RankInitial': tireur.get('RangInitial'),
                            'RankFinal': tireur.get('RangFinal')
                        })

                # Convert the extracted data into a DataFrame
                df_tableaux_summary = pd.DataFrame(tableaux_data)

                ################### GET POULES MATCHES #################
                poule_match_data = []

                # Loop through each 'TourDePoules' element and extract the matches
                for tour_de_poules in phases.findall('TourDePoules'):
                    for poule in tour_de_poules.findall('Poule'):
                        for match in poule.findall('Match'):
                            tireurs = match.findall('Tireur')

                            # Ensure there are at least two tireurs for the match
                            fencer_ref = tireurs[0].get('REF') if len(tireurs) > 0 else None
                            fencer_score = tireurs[0].get('Score') if len(tireurs) > 0 else None
                            fencer_status = tireurs[0].get('Statut') if len(tireurs) > 0 else None

                            opponent_ref = tireurs[1].get('REF') if len(tireurs) > 1 else None
                            opponent_score = tireurs[1].get('Score') if len(tireurs) > 1 else None
                            opponent_status = tireurs[1].get('Statut') if len(tireurs) > 1 else None

                            poule_match_data.append({
                                'MatchID': match.get('ID'),
                                'MatchSourceID': poule.get('ID'),
                                'Fencer_REF': fencer_ref,
                                'Fencer_Score': fencer_score,
                                'Fencer_Status': fencer_status,
                                'Opponent_REF': opponent_ref,
                                'Opponent_Score': opponent_score,
                                'Opponent_Status': opponent_status,
                                'MatchType': 'Poule'
                            })

                df_poule_matches = pd.DataFrame(poule_match_data)

                ################### GET TABLEAU MATCHES #################
                tableau_match_data = []

                # Loop through each 'SuiteDeTableaux' element and extract the matches
                for suite in root.findall('.//SuiteDeTableaux'):
                    for tableau in suite.findall('Tableau'):
                        for match in tableau.findall('Match'):
                            tireurs = match.findall('Tireur')

                            # Ensure there are at least two tireurs for the match
                            fencer_ref = tireurs[0].get('REF') if len(tireurs) > 0 else None
                            fencer_score = tireurs[0].get('Score') if len(tireurs) > 0 else None
                            fencer_status = tireurs[0].get('Statut') if len(tireurs) > 0 else None

                            opponent_ref = tireurs[1].get('REF') if len(tireurs) > 1 else None
                            opponent_score = tireurs[1].get('Score') if len(tireurs) > 1 else None
                            opponent_status = tireurs[1].get('Statut') if len(tireurs) > 1 else None

                            tableau_match_data.append({
                                'MatchID': match.get('ID'),
                                'MatchSourceID': tableau.get('ID'),
                                'Fencer_REF': fencer_ref,
                                'Fencer_Score': fencer_score,
                                'Fencer_Status': fencer_status,
                                'Opponent_REF': opponent_ref,
                                'Opponent_Score': opponent_score,
                                'Opponent_Status': opponent_status,
                                'MatchType': 'Tableau'
                            })

                df_tableau_matches = pd.DataFrame(tableau_match_data)

                ################### COMBINE POULES AND TABLEAU MATCHES ###################
                # Concatenate the poule and tableau matches into one DataFrame
                df_combined_matches = pd.concat([df_poule_matches, df_tableau_matches], ignore_index=True)
            else:
                print(f"Skipping match data for file {xml_file} as no 'Phases' element was found.")
                df_combined_matches = pd.DataFrame()

            ################### ADD FENCERS NAMES ###################
            # Create a lookup dictionary for Fencers from the df_tireurs DataFrame
            tireurs_lookup = df_tireurs.set_index('ID')[['Nom', 'Prenom']].to_dict('index')

            # Define a function to lookup and return the full name
            def get_fencer_name(ref):
                fencer = tireurs_lookup.get(ref)
                if fencer:
                    return f"{fencer['Prenom']} {fencer['Nom']}"
                else:
                    return None

            if not df_combined_matches.empty:
                # Apply the lookup function to add Fencer names in the combined DataFrame
                df_combined_matches['Fencer_Name'] = df_combined_matches['Fencer_REF'].apply(get_fencer_name)
                df_combined_matches['Opponent_Name'] = df_combined_matches['Opponent_REF'].apply(get_fencer_name)

                ################### ADD COMPETITION INFO ###################
                # Add the competition information as new columns
                for key, value in competition_info.items():
                    df_combined_matches[key] = value

                ################### SAVE OR APPEND TO CSV ###################
                # If the CSV file does not exist, create it and write the header
                if not os.path.isfile(csv_file_path):
                    df_combined_matches.to_csv(csv_file_path, mode='w', header=True, index=False)
                else:
                    # If the CSV exists, append the data without writing the header again
                    df_combined_matches.to_csv(csv_file_path, mode='a', header=False, index=False)
        else:
            print(f"Skipping file {index} ({xml_file}) as the root tag is not 'CompetitionIndividuelle'.")

# Entry point
if __name__ == "__main__":
    process_files()
