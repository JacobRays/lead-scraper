import os  
import requests  
from bs4 import BeautifulSoup  
from selenium import webdriver  
from selenium.webdriver.chrome.options import Options  
from fake_useragent import UserAgent  
import pandas as pd  
import random  
import time  
import logging  
import sys  
from urllib.parse import urljoin  

# Create the directory if it doesn't exist  
log_dir = '/app/data/'  
if not os.path.exists(log_dir):  
    os.makedirs(log_dir)  

# Setup logging  
logging.basicConfig(filename=os.path.join(log_dir, 'scraper.log'), level=logging.INFO,  
                    format='%(asctime)s - %(levelname)s - %(message)s')  

# Initialize user agent rotator  
ua = UserAgent()  

# Load proxies from file  
def load_proxies(file_path='proxies.txt'):  
    try:  
        with open(file_path, 'r') as f:  
            proxies = [line.strip() for line in f if line.strip()]  
        return proxies  
    except FileNotFoundError:  
        logging.error("proxies.txt not found. Please create it with proxy IPs.")  
        return []  

# Initialize Selenium driver with proxy and user agent  
def init_driver(proxy=None):  
    chrome_options = Options()  
    chrome_options.add_argument('--headless')  # Run in background  
    chrome_options.add_argument('--no-sandbox')  # Required for Render  
    chrome_options.add_argument('--disable-dev-shm-usage')  # Optimize for low RAM  
    chrome_options.add_argument(f'user-agent={ua.random}')  
    if proxy:  
        chrome_options.add_argument(f'--proxy-server={proxy}')  
    driver = webdriver.Chrome(options=chrome_options)  
    return driver  

# Check robots.txt compliance  
def check_robots_txt(url):  
    try:  
        robots_url = urljoin(url, '/robots.txt')  
        response = requests.get(robots_url, timeout=5)  
        if 'Disallow: /search' in response.text:  
            logging.warning(f"Scraping {url} may be restricted by robots.txt")  
            return False  
        return True  
    except Exception as e:  
        logging.error(f"Error checking robots.txt: {e}")  
        return True  # Proceed cautiously  

# Scrape Yellow Pages for leads  
def scrape_yellow_pages(search_term, location, max_leads=100):  
    base_url = "https://www.yellowpages.com"  
    search_url = f"{base_url}/search?search_terms={search_term}&geo_location_terms={location}"  
    
    if not check_robots_txt(base_url):  
        logging.error("Scraping blocked by robots.txt. Exiting.")  
        return []  
    
    proxies = load_proxies()  
    leads = []  
    driver = None  
    
    try:  
        # Initialize Selenium with lightweight settings  
        proxy = random.choice(proxies) if proxies else None  
        driver = init_driver(proxy)  
        driver.get(search_url)  
        
        # Random delay to mimic human  
        time.sleep(random.uniform(5, 10))  
        
        # Scroll to load content  
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  
        time.sleep(2)  
        
        # Parse page  
        soup = BeautifulSoup(driver.page_source, 'html.parser')  
        listings = soup.find_all('div', class_='result')  
        
        for listing in listings:  
            if len(leads) >= max_leads:  
                break  
                
            try:  
                name = listing.find('a', class_='business-name').text.strip() if listing.find('a', class_='business-name') else 'N/A'  
                category = search_term  
                address = listing.find('div', class_='street-address').text.strip() if listing.find('div', class_='street-address') else 'N/A'  
                phone = listing.find('div', class_='phones').text.strip() if listing.find('div', class_='phones') else 'N/A'  
                website = listing.find('a', class_='track-visit-website')['href'] if listing.find('a', class_='track-visit-website') else 'N/A'  
                
                lead = {  
                    'Name': name,  
                    'Category': category,  
                    'Address': address,  
                    'Phone': phone,  
                    'Website': website  
                }  
                leads.append(lead)  
                logging.info(f"Scraped lead: {name}")  
                
                time.sleep(random.uniform(2, 5))  
                
            except Exception as e:  
                logging.error(f"Error parsing listing: {e}")  
                continue  
        
        # Handle pagination  
        while len(leads) < max_leads:  
            try:  
                next_button = driver.find_element("css selector", 'a.next')  
                if not next_button:  
                    break  
                next_button.click()  
                time.sleep(random.uniform(5, 10))  
                soup = BeautifulSoup(driver.page_source, 'html.parser')  
                listings = soup.find_all('div', class_='result')  
                
                for listing in listings:  
                    if len(leads) >= max_leads:  
                        break  
                    try:  
                        name = listing.find('a', class_='business-name').text.strip() if listing.find('a', class_='business-name') else 'N/A'  
                        category = search_term  
                        address = listing.find('div', class_='street-address').text.strip() if listing.find('div', class_='street-address') else 'N/A'  
                        phone = listing.find('div', class_='phones').text.strip() if listing.find('div', class_='phones') else 'N/A'  
                        website = listing.find('a', class_='track-visit-website')['href'] if listing.find('a', class_='track-visit-website') else 'N/A'  
                        
                        lead = {  
                            'Name': name,  
                            'Category': category,  
                            'Address': address,  
                            'Phone': phone,  
                            'Website': website  
                        }  
                        leads.append(lead)  
                        logging.info(f"Scraped lead: {name}")  
                        time.sleep(random.uniform(2, 5))  
                        
                    except Exception as e:  
                        logging.error(f"Error parsing listing: {e}")  
                        continue  
                        
            except Exception as e:  
                logging.info(f"No more pages or error in pagination: {e}")  
                break  
                
    except Exception as e:  
        logging.error(f"Error scraping {search_url}: {e}")  
        
    finally:  
        if driver:  
            driver.quit()  
    
    return leads  

# Save leads to CSV  
def save_leads(leads, filename='/app/data/leads.csv'):  
    try:  
        df = pd.DataFrame(leads)  
        df.to_csv(filename, index=False, mode='a', header=not os.path.exists(filename))  
        logging.info(f"Saved {len(leads)} leads to {filename}")  
    except Exception as e:  
        logging.error(f"Error saving leads: {e}")  

# Generate sample leads for marketing  
def generate_samples(leads, categories, samples_per_category=10):  
    try:  
        df = pd.DataFrame(leads)  
        for category in categories:  
            category_leads = df[df['Category'] == category].head(samples_per_category)  
            category_leads.to_csv(f'/app/data/sample_{category.lower()}_leads.csv', index=False)  
            logging.info(f"Generated sample for {category}")  
    except Exception as e:  
        logging.error(f"Error generating samples: {e}")  

# Main function with 24/7 loop  
def main():  
    categories = ['Doctors', 'Pharmacists']  
    location = 'New York, NY'  # Change as needed  
    leads_per_category = 200  # 200 per category/day  
    cycle_interval = 86400  # 24 hours in seconds  
    
    while True:  
        all_leads = []  
        logging.info("Starting new scraping cycle")  
        
        for category in categories:  
            logging.info(f"Scraping {category} in {location}")  
            leads = scrape_yellow_pages(category, location, max_leads=leads_per_category)  
            all_leads.extend(leads)  
            save_leads(leads, f'/app/data/leads_{category.lower()}.csv')  
            time.sleep(random.uniform(10, 20))  
        
        save_leads(all_leads, '/app/data/all_leads.csv')  
        generate_samples(all_leads, categories)  
        logging.info(f"Total leads scraped: {len(all_leads)}")  
        
        # Wait 24 hours before next cycle  
        logging.info(f"Sleeping for {cycle_interval} seconds")  
        time.sleep(cycle_interval)  

if __name__ == '__main__':  
    try:  
        main()  
    except KeyboardInterrupt:  
        logging.info("Bot stopped by user")  
        sys.exit(0)  
    except Exception as e:  
        logging.error(f"Fatal error: {e}")  
        sys.exit(1)  
