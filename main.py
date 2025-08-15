import os
import gspread
import requests
import openai
import json
from urllib.parse import urlparse
from thefuzz import fuzz
from datetime import datetime, timezone

# --- Configuration ---
# These will be set as environment variables in the cloud
NEWSDATA_API_KEY = os.environ.get("NEWSDATA_API_KEY") 
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
SPREADSHEET_NAME = os.environ.get("SPREADSHEET_NAME")
SERVICE_ACCOUNT_FILE = "service_account.json"
GOOGLE_SHEETS_CREDENTIALS_JSON = os.environ.get("GOOGLE_SHEETS_CREDENTIALS_JSON")

# Configure the OpenAI client
if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

def get_ai_summary(article_title, article_description):
    """Uses OpenAI to generate a one-sentence summary."""
    if not all([article_description, openai.api_key]):
        return "Summary not available."
    try:
        prompt = f"Summarize the following news article in a single, insightful sentence. Title: '{article_title}'. Description: '{article_description}'"
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": "You are an expert news summarizer."},{"role": "user", "content": prompt}],
            temperature=0.5, max_tokens=60
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return "AI summary failed."

def main(event=None, context=None):
    """Main function with advanced duplicate checking"""
    print("Starting news pipeline with your preferred query...")

    # --- Authenticate with Google Sheets ---
    try:
        if GOOGLE_SHEETS_CREDENTIALS_JSON:
            # Use credentials from GitHub Secrets (in the cloud)
            creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON)
            gc = gspread.service_account_from_dict(creds_dict)
        else:
            # Fallback to local file for testing
            gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        
        sh = gc.open(SPREADSHEET_NAME)
        worksheet = sh.sheet1
        print(f"Connected to Google Sheet: {SPREADSHEET_NAME}")
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return

    # --- Fetch News using your chosen URL parameters ---
    print("Fetching news from Newsdata.io...")
    api_url = "https://newsdata.io/api/1/latest"
    params = {'apikey': NEWSDATA_API_KEY, 'q': 'AI, openai, chatgpt, google', 'language': 'en', 'limit': 4}
    
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        articles = response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching from Newsdata.io API: {e}")
        return

    # --- Process and Add to Sheet with Advanced Duplicate Check ---
    print(f"Found {len(articles)} articles. Processing and checking for duplicates...")
    existing_links = set(worksheet.col_values(6)) # Assumes source link is in column F
    existing_titles = set(worksheet.col_values(2)) # <-- FIX: Corrected variable name from 'works' to 'worksheet'
    
    SIMILARITY_THRESHOLD = 50
    rows_to_add = []

    for article in reversed(articles):
        source_link = article.get('link')
        title = article.get('title')

        if source_link and source_link not in existing_links:
            is_semantically_duplicate = False
            for existing_title in existing_titles:
                similarity_score = fuzz.token_sort_ratio(title, existing_title)
                if similarity_score > SIMILARITY_THRESHOLD:
                    is_semantically_duplicate = True
                    print(f"Skipping similar article: '{title}' (Score: {similarity_score})")
                    break
            
            if not is_semantically_duplicate:
                ai_summary = get_ai_summary(title, article.get('description'))
                domain = urlparse(source_link).netloc
                timestamp = article.get('pubDate', datetime.now(timezone.utc).isoformat())
                row = ["", title, ai_summary, article.get('image_url'), article.get('source_id'), source_link, f"https://www.google.com/s2/favicons?domain={domain}&sz=64", timestamp, "Published"]
                rows_to_add.append(row)
                
                existing_links.add(source_link)
                existing_titles.add(title)
    
    if rows_to_add:
        worksheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
        print(f"Process complete. Added {len(rows_to_add)} new articles.")
    else:
        print("Process complete. No new articles to add.")

# Use this block for local testing only
if __name__ == "__main__":
    # PASTE YOUR KEYS HERE FOR THE LOCAL TEST
    NEWSDATA_API_KEY = "YOUR_NEWSDATA_KEY"
    OPENAI_API_KEY = "YOUR_OPENAI_KEY"
    SPREADSHEET_NAME = "AI News Feed"
    
    if OPENAI_API_KEY:
        openai.api_key = OPENAI_API_KEY
    
    main()
