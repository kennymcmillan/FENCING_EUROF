import os
import xml.etree.ElementTree as ET
import pandas as pd

# Path to the folder containing the XML files
folder_path = 'xml_links'
csv_file_path = 'HISTORICAL_EuroF_team_match_data.csv'

if os.path.exists(csv_file_path):
    os.remove(csv_file_path)
    print(f"Deleted existing file: {csv_file_path}")

# Function to clean up unwanted characters before <?xml>
def clean_xml(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        xml_content = file.read()
    
    # Find the position of the "<?xml" declaration and remove anything before it
    xml_declaration_pos = xml_content.find('<?xml')
    
    if xml_declaration_pos > 0:
        xml_content = xml_content[xml_declaration_pos:]
    
    return xml_content

# Main function to process XML files
def process_files():
    # Get a list of all XML files in the folder
    xml_files = [f for f in os.listdir(folder_path) if f.endswith('.xml')]

    # Loop through each XML file and process it
    for index, xml_file in enumerate(xml_files, start=1):
        file_path = os.path.join(folder_path, xml_file)
        
        try:
            cleaned_xml = clean_xml(file_path)
            root = ET.fromstring(cleaned_xml)
        except ET.ParseError as e:
            print(f"Skipping file {index} ({xml_file}) due to a ParseError: {e}")
            continue

        if root.tag == 'CompetitionParEquipes':
            print(f"Processing file {index} out of {len(xml_files)}: {xml_file}")

            # Extract competition-level data from the root
            competition_info = {
                'CompetitionID': root.get('ID', None),
                'Arme': root.get('Arme', None),
                'Sexe': root.get('Sexe', None),
                'Domaine': root.get('Domaine', None),
                'Federation': root.get('Federation', None),
                'Categorie': root.get('Categorie', None),
                'Date': root.get('Date', None),
                'TitreCourt': root.get('TitreCourt', None),
                'TitreLong': root.get('TitreLong', None),
                'Team': True  # Indicate this is a team competition
            }

            ########################### GET FENCER NAMES AND TEAMS ###########################

            # Dictionary to store fencer IDs and their names
            fencer_lookup = {}

            equipes = root.find('Equipes')

            if equipes is not None:
                # Loop through each Equipe element and extract fencer and team information
                for equipe in equipes.findall('Equipe'):
                    equipe_id = equipe.get('ID')
                    team_name = equipe.get('Club')
                    team_nation = equipe.get('Nation')
                    team_classement = equipe.get('Classement')

                    # Loop through the fencers in the team
                    for tireur in equipe.findall('Tireur'):
                        fencer_id = tireur.get('ID')
                        fencer_name = f"{tireur.get('Prenom')} {tireur.get('Nom')}"
                        # Add the fencer ID, name, and their team-level data (team name, nation, etc.)
                        fencer_lookup[fencer_id] = {
                            'name': fencer_name,
                            'team_name': team_name,
                            'team_nation': team_nation,
                            'team_classement': team_classement
                        }

            ########################### GET TEAM MATCH DATA ##########################

            match_data = []
            phases = root.find('Phases')

            if phases is not None:
                # Loop through each 'SuiteDeTableaux' element and extract the team matches
                for suite in phases.findall('.//SuiteDeTableaux'):
                    for tableau in suite.findall('Tableau'):
                        for match in tableau.findall('Match'):
                            equipes = match.findall('Equipe')

                            # Extract data for the two competing teams
                            team_ref = equipes[0].get('REF') if len(equipes) > 0 else None
                            team_score = equipes[0].get('Score') if len(equipes) > 0 else None
                            team_status = equipes[0].get('Statut') if len(equipes) > 0 else None

                            opponent_ref = equipes[1].get('REF') if len(equipes) > 1 else None
                            opponent_score = equipes[1].get('Score') if len(equipes) > 1 else None
                            opponent_status = equipes[1].get('Statut') if len(equipes) > 1 else None

                            # Extract individual bouts (Assauts) for the match
                            for assaut in match.findall('Assaut'):
                                tireurs = assaut.findall('Tireur')

                                # Ensure there are at least two fencers for the bout
                                if len(tireurs) < 2:
                                    print(f"Skipping Assaut {assaut.get('ID')} in match {match.get('ID')} due to incomplete data.")
                                    continue

                                fencer_ref = tireurs[0].get('REF')
                                fencer_score = tireurs[0].get('Score')

                                opponent_fencer_ref = tireurs[1].get('REF')
                                opponent_fencer_score = tireurs[1].get('Score')

                                # Lookup fencer names and team-level data from the dictionary
                                fencer_info = fencer_lookup.get(fencer_ref, {
                                    'name': 'Unknown',
                                    'team_name': 'Unknown',
                                    'team_nation': 'Unknown',
                                    'team_classement': 'Unknown'
                                })
                                opponent_info = fencer_lookup.get(opponent_fencer_ref, {
                                    'name': 'Unknown',
                                    'team_name': 'Unknown',
                                    'team_nation': 'Unknown',
                                    'team_classement': 'Unknown'
                                })

                                # Append individual match-like data for each bout
                                match_data.append({
                                    'MatchID': match.get('ID'),
                                    'MatchSourceID': tableau.get('ID'),
                                    'Fencer_REF': fencer_ref,
                                    'Fencer_Score': fencer_score,
                                    'Fencer_Name': fencer_info['name'],
                                    'Fencer_Team': fencer_info['team_name'],
                                    'Fencer_Team_Nation': fencer_info['team_nation'],
                                    'Fencer_Team_Classement': fencer_info['team_classement'],
                                    'Opponent_REF': opponent_fencer_ref,
                                    'Opponent_Score': opponent_fencer_score,
                                    'Opponent_Name': opponent_info['name'],
                                    'Opponent_Team': opponent_info['team_name'],
                                    'Opponent_Team_Nation': opponent_info['team_nation'],
                                    'Opponent_Team_Classement': opponent_info['team_classement'],
                                    'Team_REF': team_ref,
                                    'Team_Score': team_score,
                                    'Opponent_Team_REF': opponent_ref,
                                    'Opponent_Team_Score': opponent_score,
                                    'MatchType': 'Team',
                                    'Assaut_ID': assaut.get('ID'),  # Individual bout ID
                                    # Include competition information in each row
                                    'CompetitionID': competition_info['CompetitionID'],
                                    'Arme': competition_info['Arme'],
                                    'Sexe': competition_info['Sexe'],
                                    'Domaine': competition_info['Domaine'],
                                    'Federation': competition_info['Federation'],
                                    'Categorie': competition_info['Categorie'],
                                    'Date': competition_info['Date'],
                                    'TitreCourt': competition_info['TitreCourt'],
                                    'TitreLong': competition_info['TitreLong']
                                })

            df_combined_matches = pd.DataFrame(match_data)

            ################### SAVE OR APPEND TO CSV ###################
            if not df_combined_matches.empty:
                # Add a "Team" column to the DataFrame to indicate this is team data
                df_combined_matches['Team'] = 'Yes'

                # If the CSV file does not exist, create it and write the header
                if not os.path.isfile(csv_file_path):
                    df_combined_matches.to_csv(csv_file_path, mode='w', header=True, index=False)
                else:
                    # If the CSV exists, append the data without writing the header again
                    df_combined_matches.to_csv(csv_file_path, mode='a', header=False, index=False)

        else:
            print(f"Skipping file {index} ({xml_file}) as the root tag is not 'CompetitionParEquipes'.")

# Entry point
if __name__ == "__main__":
    process_files()
    



