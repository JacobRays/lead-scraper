import asyncio
import aiohttp
import pandas as pd
import random
import time
import logging
import sys
import os
from fake_useragent import UserAgent
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import sentry_sdk
from urllib.parse import urljoin
import json
from typing import List, Dict, Optional

# Initialize Sentry for error monitoring
sentry_sdk.init(
    dsn="YOUR_SENTRY_DSN",
    traces_sample_rate=1.0,
    environment="production"
)

# Setup logging
logging.basicConfig(
    filename='/app/data/scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize user agent rotator
ua = UserAgent()

# Configuration
CONFIG = {
    "categories": ["Doctors", "Pharmacists", "Lawyers", "Contractors", "Dentists", "Restaurants", "Plumbers", "Electricians", "Accountants", "Realtors"],
    "locations": ["New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ"],
    "leads_per_category": 100,  # 100 leads per category per location
    "cycle_interval": 86400,  # 24 hours
    "yelp_api_key": "YOUR_YELP_API_KEY",
    "google_api_key": "YOUR_GOOGLE_API_KEY",
    "proxy_list": ["http://proxy1:port", "http://proxy2:port"],  # Replace with Bright Data proxies
    "2captcha_api_key": "YOUR_2CAPTCHA_API_KEY"
}

# Google Drive setup
SCOPES = ['https://www.googleapis.com/auth/drive.file']
DRIVE_FOLDER_ID = "YOUR_GOOGLE_DRIVE_FOLDER_ID"

async def get_drive_service() -> Optional[build]:
    try:
        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        logging.error(f"Error setting up Google Drive: {e}")
        sentry_sdk.capture_exception(e)
        return None

async def upload_to_drive(filename: str) -> None:
    try:
        service = await get_drive_service()
        if not service:
            return
        file_metadata = {
            'name': os.path.basename(filename),
            'parents': [DRIVE_FOLDER_ID]
        }
        media = MediaFileUpload(filename)
        service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        logging.info(f"Uploaded {filename} to Google Drive")
    except Exception as e:
        logging.error(f"Error uploading to Drive: {e}")
        sentry_sdk.capture_exception(e)

# Load proxies
def load_proxies(file_path: str = 'proxies.txt') -> List[str]:
    try:
        with open(file_path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logging.error("proxies.txt not found. Using CONFIG proxies.")
        return CONFIG["proxy_list"]

# Solve CAPTCHA using 2Captcha
async def solve_captcha(session: aiohttp.ClientSession, site_key: str, url: str) -> Optional[str]:
    try:
        async with session.post(
            'http://2captcha.com/in.php',
            data={
                'key': CONFIG['2captcha_api_key'],
                'method': 'userrecaptcha',
                'googlekey': site_key,
                'pageurl': url
            }
        ) as resp:
            captcha_id = (await resp.text()).split('|')[-1]
        for _ in range(10):
            async with session.get(
                f'http://2captcha.com/res.php?key={CONFIG["2captcha_api_key"]}&action=get&id={captcha_id}'
            ) as resp:
                result = await resp.text()
                if 'CAPCHA_NOT_READY' not in result:
                    return result.split('|')[-1]
            await asyncio.sleep(5)
        logging.error("CAPTCHA solving timed out")
        return None
    except Exception as e:
        logging.error(f"Error solving CAPTCHA: {e}")
        sentry_sdk.capture_exception(e)
        return None

# Fetch leads from Yelp API
async def fetch_yelp_leads(session: aiohttp.ClientSession, category: str, location: str, max_leads: int) -> List[Dict]:
    leads = []
    try:
        headers = {'Authorization': f'Bearer {CONFIG["yelp_api_key"]}'}
        params = {
            'term': category,
            'location': location,
            'limit': 50,
            'offset': 0
        }
        while len(leads) < max_leads:
            async with session.get(
                'https://api.yelp.com/v3/businesses/search',
                headers=headers,
                params=params,
                proxy=random.choice(load_proxies())
            ) as resp:
                if resp.status == 429:
                    logging.warning("Yelp rate limit hit. Waiting...")
                    await asyncio.sleep(60)
                    continue
                data = await resp.json()
                if 'businesses' not in data:
                    break
                for biz in data['businesses']:
                    if len(leads) >= max_leads:
                        break
                    lead = {
                        'Name': biz.get('name', 'N/A'),
                        'Category': category,
                        'Address': ', '.join(biz.get('location', {}).get('display_address', ['N/A'])),
                        'Phone': biz.get('phone', 'N/A'),
                        'Website': biz.get('url', 'N/A').split('?')[0]
                    }
                    leads.append(lead)
                    logging.info(f"Scraped Yelp lead: {lead['Name']}")
                params['offset'] += 50
                if len(data['businesses']) < 50:
                    break
                await asyncio.sleep(random.uniform(1, 3))
    except Exception as e:
        logging.error(f"Error fetching Yelp leads: {e}")
        sentry_sdk.capture_exception(e)
    return leads

# Fetch leads from Google Maps API
async def fetch_google_leads(session: aiohttp.ClientSession, category: str, location: str, max_leads: int) -> List[Dict]:
    leads = []
    try:
        params = {
            'query': f"{category} in {location}",
            'key': CONFIG['google_api_key']
        }
        async with session.get(
            'https://maps.googleapis.com/maps/api/place/textsearch/json',
            params=params,
            proxy=random.choice(load_proxies())
        ) as resp:
            data = await resp.json()
            if 'results' not in data:
                return leads
            for place in data['results'][:max_leads]:
                lead = {
                    'Name': place.get('name', 'N/A'),
                    'Category': category,
                    'Address': place.get('formatted_address', 'N/A'),
                    'Phone': 'N/A',  # Requires additional API call
                    'Website': place.get('website', 'N/A')
                }
                leads.append(lead)
                logging.info(f"Scraped Google lead: {lead['Name']}")
                await asyncio.sleep(random.uniform(0.5, 1.5))
    except Exception as e:
        logging.error(f"Error fetching Google leads: {e}")
        sentry_sdk.capture_exception(e)
    return leads

# Save leads to CSV
def save_leads(leads: List[Dict], filename: str) -> None:
    try:
        df = pd.DataFrame(leads)
        df.to_csv(filename, index=False, mode='a', header=not os.path.exists(filename))
        logging.info(f"Saved {len(leads)} leads to {filename}")
        asyncio.create_task(upload_to_drive(filename))
    except Exception as e:
        logging.error(f"Error saving leads: {e}")
        sentry_sdk.capture_exception(e)

# Generate sample leads for marketing
def generate_samples(leads: List[Dict], categories: List[str], samples_per_category: int = 10) -> None:
    try:
        df = pd.DataFrame(leads)
        for category in categories:
            category_leads = df[df['Category'] == category].head(samples_per_category)
            category_leads.to_csv(f'/app/data/sample_{category.lower()}_leads.csv', index=False)
            logging.info(f"Generated sample for {category}")
            asyncio.create_task(upload_to_drive(f'/app/data/sample_{category.lower()}_leads.csv'))
    except Exception as e:
        logging.error(f"Error generating samples: {e}")
        sentry_sdk.capture_exception(e)

# Main scraping function
async def scrape_leads(category: str, location: str, max_leads: int) -> List[Dict]:
    async with aiohttp.ClientSession() as session:
        leads = []
        try:
            # Fetch from Yelp
            yelp_leads = await fetch_yelp_leads(session, category, location, max_leads // 2)
            leads.extend(yelp_leads)
            # Fetch from Google
            google_leads = await fetch_google_leads(session, category, location, max_leads - len(leads))
            leads.extend(google_leads)
        except Exception as e:
            logging.error(f"Error in scrape_leads: {e}")
            sentry_sdk.capture_exception(e)
        return leads[:max_leads]

# Main function with 24/7 loop
async def main():
    while True:
        all_leads = []
        logging.info("Starting new scraping cycle")
        for location in CONFIG["locations"]:
            for category in CONFIG["categories"]:
                logging.info(f"Scraping {category} in {location}")
                try:
                    leads = await scrape_leads(category, location, CONFIG["leads_per_category"])
                    all_leads.extend(leads)
                    save_leads(leads, f'/app/data/leads_{category.lower()}_{location.replace(", ", "_").lower()}.csv')
                    await asyncio.sleep(random.uniform(5, 10))
                except Exception as e:
                    logging.error(f"Error scraping {category} in {location}: {e}")
                    sentry_sdk.capture_exception(e)
        save_leads(all_leads, '/app/data/all_leads.csv')
        generate_samples(all_leads, CONFIG["categories"])
        logging.info(f"Total leads scraped: {len(all_leads)}")
        logging.info(f"Sleeping for {CONFIG['cycle_interval']} seconds")
        await asyncio.sleep(CONFIG["cycle_interval"])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sentry_sdk.capture_exception(e)
        sys.exit(1)
