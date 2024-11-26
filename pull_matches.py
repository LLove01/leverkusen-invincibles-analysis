import os
import pandas as pd
from statsbombpy import sb
import json
import warnings

def create_data_directory(base_path='matches'):
    """
    Create base directory for storing match data if it doesn't exist.
    
    Args:
        base_path (str): Base directory path for storing match data
    
    Returns:
        str: Path to the created base directory
    """
    os.makedirs(base_path, exist_ok=True)
    return base_path

def sanitize_filename(filename):
    """
    Sanitize filename by removing invalid characters.
    
    Args:
        filename (str): Original filename
    
    Returns:
        str: Sanitized filename
    """
    return "".join(c for c in filename if c.isalnum() or c in (' ', '_', '-')).rstrip()

def sanitize_team_name(team_name):
    """
    Sanitize team name by removing spaces and special characters.
    
    Args:
        team_name (str): Original team name
    
    Returns:
        str: Sanitized team name
    """
    return "".join(c for c in team_name if c.isalnum())

def download_leverkusen_data(base_path):
    """
    Download data for a single Bundesliga 2023/2024 match from StatsBomb.
    
    Args:
        base_path (str): Base directory path for storing match data
    """
    # Suppress StatsBomb API warnings
    warnings.filterwarnings("ignore", category=UserWarning)
    
    try:
        # First get the list of matches using the correct competition and season IDs
        matches = sb.matches(
            competition_id=9,  # Corrected competition ID for Bundesliga 
            season_id=281      # Corrected season ID for 2023/2024
        )
        
        # Get first match ID (or you can specify a particular one)
        match_row = matches.iloc[0]
        match_id = int(match_row['match_id'])
        
        # Create descriptive match directory name
        home_team = sanitize_team_name(match_row['home_team'])
        away_team = sanitize_team_name(match_row['away_team'])
        home_score = match_row.get('home_score', 0)
        away_score = match_row.get('away_score', 0)
        match_week = match_row.get('match_week', 0)
        
        match_dir_name = f"GW{match_week}_{home_team}_{home_score}-{away_score}_{away_team}"
        match_dir = os.path.join(base_path, match_dir_name)
        os.makedirs(match_dir, exist_ok=True)
        
        # Fetch events, frames, and lineups for this specific match
        events_df = sb.events(match_id=match_id)
        frames_df = sb.frames(match_id=match_id)
        
        # Fetch lineups
        lineups = sb.lineups(match_id=match_id)
        
        # Save lineups as CSV with sanitized team names
        for team, lineup_df in lineups.items():
            sanitized_team = sanitize_team_name(team)
            lineup_df.to_csv(os.path.join(match_dir, f'{sanitized_team}_lineups.csv'), index=False)
        
        # Print column names to debug
        print("Events DataFrame Columns:", list(events_df.columns))
        print("Frames DataFrame Columns:", list(frames_df.columns))
        
        # Merge event and frame data if needed
        if not frames_df.empty:
            frames_df.rename(columns={'event_uuid': 'id'}, inplace=True)
            merged_df = pd.merge(frames_df, events_df, how="left", on=["match_id", "id"])
            # Save 360 frames as CSV
            merged_df.to_csv(os.path.join(match_dir, '360_frames.csv'), index=False)
        
        # Save events as CSV
        events_df.to_csv(os.path.join(match_dir, 'events.csv'), index=False)
        
        # Fetch comprehensive match metadata
        match_metadata = {
            # Basic match identification
            'match_id': int(match_id),
            'match_date': str(match_row.get('match_date', 'Unknown')),
            'kick_off': str(match_row.get('kick_off', 'Unknown')),
            
            # Competition details
            'competition_id': 9,
            'season_id': 281,
            'competition': str(match_row.get('competition', '1. Bundesliga')),
            'season': str(match_row.get('season', '2023/2024')),
            
            # Teams
            'home_team': str(match_row.get('home_team', 'Unknown')),
            'away_team': str(match_row.get('away_team', 'Unknown')),
            'home_score': int(match_row.get('home_score', 0)),
            'away_score': int(match_row.get('away_score', 0)),
            
            # Match status
            'match_status': str(match_row.get('match_status', 'Unknown')),
            'match_status_360': str(match_row.get('match_status_360', 'Unknown')),
            
            # Update information
            'last_updated': str(match_row.get('last_updated', None)),
            'last_updated_360': str(match_row.get('last_updated_360', None)),
            
            # Additional match details
            'match_week': int(match_row.get('match_week', 0)),
            'competition_stage': str(match_row.get('competition_stage', 'Unknown')),
            'stadium': str(match_row.get('stadium', 'Unknown')),
            'referee': str(match_row.get('referee', 'Unknown')),
            
            # Managers
            'home_managers': match_row.get('home_managers', []),
            'away_managers': match_row.get('away_managers', []),
            
            # Data versions
            'data_version': str(match_row.get('data_version', 'Unknown')),
            'shot_fidelity_version': str(match_row.get('shot_fidelity_version', 'Unknown')),
            'xy_fidelity_version': str(match_row.get('xy_fidelity_version', 'Unknown'))
        }
        
        with open(os.path.join(match_dir, 'metadata.json'), 'w') as f:
            json.dump(match_metadata, f, indent=4)
        
        print(f"Processed {match_dir_name}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def main():
    """
    Main function to orchestrate data download and organization.
    """
    base_path = create_data_directory()
    download_leverkusen_data(base_path)
    print("Data download and organization complete!")

if __name__ == "__main__":
    main()