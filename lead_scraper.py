import asyncio
import logging
import os
import pandas as pd
import random
import sys
import time
from apify_client import ApifyClient
from typing import List, Dict

# Setup logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration
CONFIG = {
    "categories": [
        "Plumbers",
        "Electricians",
        "Roofers",
        "Landscapers",
        "Cleaning Services",
        "HVAC",
        "Painters",
        "Carpenters"
    ],
    "locations": [
        "New York, NY",
        "Los Angeles",
        "Chicago, IL",
        "Houston, TX",
        "Phoenix, AZ"
    ],
    "leads_per_category": 25,
    "cycle_interval": 86400,  # 24 hours
    "apify_api_token": "apify_api_BKlqlMyFRCdUwHLVNy8poYbmCMIzDB0zyape"  # Replace with your token
}

# Initialize Apify client
apify_client = ApifyClient(CONFIG["apify_api_token"])

# Scrape leads using Apify Google Maps Extractor
async def scrape_leads(category: str, location: str, max_leads: int) -> List[Dict]:
    leads = []
    try:
        run_input = {
            "queries": [f"{category} in {location}"],
            "maxResults": max_leads,
            "includeWeb": True,
            "skipClosed": True,
            "lang": "en",
            "reviewsSort": "newest",
            "maxImages": 0,
            "maxReviews": 0
        }
        run = apify_client.actor("apify/google-maps-extractor").call(run_input=run_input)
        dataset = apify_client.dataset(run["defaultDatasetId"]).list_items().items
        for item in dataset:
            lead = {
                "Name": item.get("title", "N/A"),
                "Category": category,
                "Address": item.get("fullAddress", "N/A"),
                "Phone": item.get("phone", "N/A"),
                "Website": item.get("website", "N/A")
            }
            leads.append(lead)
            logging.info(f"Scraped lead: {lead['Name']}")
        logging.info(f"Scraped {len(leads)} leads for {category} in {location}")
    except Exception as e:
        logging.error(f"Error scraping {category} in {location}: {e}")
    return leads[:max_leads]

# Save leads to CSV
def save_leads(leads: List[Dict], filename: str) -> None:
    try:
        df = pd.DataFrame(leads)
        df.to_csv(filename, index=False, mode='a', header=not os.path.exists(filename))
        logging.info(f"Saved {len(leads)} leads to {filename}")
    except Exception as e:
        logging.error(f"Error saving leads: {e}")

# Generate sample leads for marketing
def generate_samples(leads: List[Dict], categories: List[str], samples_per_category: int = 10) -> None:
    try:
        df = pd.DataFrame(leads)
        for category in categories:
            category_leads = df[df['Category'] == category].head(samples_per_category)
            sample_filename = f'sample_{category.lower()}_leads.csv'
            category_leads.to_csv(sample_filename, index=False)
            logging.info(f"Generated sample for {category}: {sample_filename}")
    except Exception as e:
        logging.error(f"Error generating samples: {e}")

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
                    filename = f'leads_{category.lower()}_{location.replace(", ", "_").lower()}.csv'
                    save_leads(leads, filename)
                    await asyncio.sleep(random.uniform(5, 10))
                except Exception as e:
                    logging.error(f"Error in main loop for {category} in {location}: {e}")
        save_leads(all_leads, 'all_leads.csv')
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
        sys.exit(1)
