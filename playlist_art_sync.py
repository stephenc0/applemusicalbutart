import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import applemusicpy
import requests
from PIL import Image
from io import BytesIO
from fuzzywuzzy import fuzz
from loguru import logger
import time
from typing import Dict, Tuple, Optional

# Configure logger
logger.add("sync_log.log", rotation="1 day", retention="1 week")

# Load environment variables
load_dotenv()

class PlaylistArtSync:
    def __init__(self, fuzzy_match_threshold: int = 85):
        """
        Initialize the PlaylistArtSync with API clients and settings
        
        Args:
            fuzzy_match_threshold: Minimum similarity score for fuzzy matching (0-100)
        """
        self.fuzzy_match_threshold = fuzzy_match_threshold
        self._init_spotify_client()
        self._init_apple_client()

    def _init_spotify_client(self):
        """Initialize Spotify client with error handling"""
        try:
            self.spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(
                client_id=os.getenv('SPOTIFY_CLIENT_ID'),
                client_secret=os.getenv('SPOTIFY_CLIENT_SECRET'),
                redirect_uri=os.getenv('SPOTIFY_REDIRECT_URI'),
                scope='playlist-modify-public playlist-modify-private ugc-image-upload'
            ))
            logger.info("Spotify client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Spotify client: {str(e)}")
            raise

    def _init_apple_client(self):
        """Initialize Apple Music client with error handling"""
        try:
            key_id = os.getenv('APPLE_KEY_ID')
            team_id = os.getenv('APPLE_TEAM_ID')
            secret_key = os.getenv('APPLE_SECRET_KEY')
            
            if not all([key_id, team_id, secret_key]):
                raise ValueError("Missing required Apple Music credentials")
                
            self.am = applemusicpy.AppleMusic(secret_key, key_id, team_id)
            logger.info("Apple Music client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Apple Music client: {str(e)}")
            raise

    def find_matching_playlist(self, name: str, playlists_dict: Dict[str, str]) -> Optional[str]:
        """
        Find the best matching playlist using fuzzy matching
        
        Args:
            name: Name to match
            playlists_dict: Dictionary of playlist names to IDs
            
        Returns:
            Playlist ID if match found, None otherwise
        """
        best_match = None
        best_score = 0
        
        for playlist_name in playlists_dict:
            score = fuzz.ratio(name.lower(), playlist_name.lower())
            if score > best_score and score >= self.fuzzy_match_threshold:
                best_score = score
                best_match = playlists_dict[playlist_name]
        
        if best_match:
            logger.debug(f"Found match for '{name}' with {best_score}% similarity")
        return best_match

    def download_playlist_artwork(self, url: str, max_retries: int = 3) -> Optional[Image.Image]:
        """
        Download playlist artwork with retry logic
        
        Args:
            url: Artwork URL
            max_retries: Maximum number of download attempts
            
        Returns:
            PIL Image if successful, None otherwise
        """
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return Image.open(BytesIO(response.content))
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                continue
            except Exception as e:
                logger.error(f"Failed to process artwork: {str(e)}")
                return None
        return None

    def sync_artwork(self):
        """Main function to sync artwork between platforms with error handling"""
        try:
            apple_playlists = self.get_apple_music_playlists()
            spotify_playlists = self.get_spotify_playlists()

            spotify_dict = {
                playlist['name']: playlist['id'] 
                for playlist in spotify_playlists['items']
            }

            success_count = 0
            fail_count = 0

            for apple_playlist in apple_playlists:
                playlist_name = apple_playlist['name']
                try:
                    spotify_playlist_id = self.find_matching_playlist(playlist_name, spotify_dict)
                    
                    if not spotify_playlist_id:
                        logger.warning(f"No matching Spotify playlist found for '{playlist_name}'")
                        fail_count += 1
                        continue

                    artwork_url = apple_playlist['artwork']['url']
                    artwork = self.download_playlist_artwork(artwork_url)
                    
                    if not artwork:
                        logger.error(f"Failed to download artwork for '{playlist_name}'")
                        fail_count += 1
                        continue

                    # Convert image to JPEG and get bytes
                    img_byte_arr = BytesIO()
                    artwork.convert('RGB').save(img_byte_arr, format='JPEG')
                    img_byte_arr = img_byte_arr.getvalue()

                    self.spotify.playlist_upload_cover_image(spotify_playlist_id, img_byte_arr)
                    logger.success(f"Successfully updated artwork for '{playlist_name}'")
                    success_count += 1

                except Exception as e:
                    logger.error(f"Failed to process playlist '{playlist_name}': {str(e)}")
                    fail_count += 1
                    continue

            logger.info(f"Sync complete. Success: {success_count}, Failed: {fail_count}")
            
        except Exception as e:
            logger.error(f"Sync process failed: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        syncer = PlaylistArtSync()
        syncer.sync_artwork()
    except Exception as e:
        logger.error(f"Application failed: {str(e)}") 