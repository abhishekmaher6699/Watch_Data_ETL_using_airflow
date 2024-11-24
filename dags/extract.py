import aiohttp
import asyncio
import random
from bs4 import BeautifulSoup
import logging

base_url = "https://www.ethoswatches.com"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]

def get_random_delay():
    return random.uniform(1, 3)

async def fetch(session, url):
    try:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        async with session.get(url, headers=headers) as response:
            if response.status == 429: 
                logging.warning(f"Rate limit hit, sleeping before retrying {url}")
                await asyncio.sleep(5) 
                return await fetch(session, url)  
            return await response.text()
    except asyncio.TimeoutError:
        logging.error(f"Timeout error occurred while fetching {url}")
        return None
    except aiohttp.ClientOSError as e:
        logging.error(f"ClientOSError occurred while fetching {url}: {e}")
        return None
    except aiohttp.client_exceptions.ServerDisconnectedError as e:
        logging.error(f"ServerDisconnectedError occurred while fetching {url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error occurred while fetching {url}: {e}")
        return None

async def extract_single_page(session, url):
    try:
        html = await fetch(session, url)
        if html is None:
            return [] 
        soup = BeautifulSoup(html, "lxml")
        links = [i.a['href'] for i in soup.find_all(class_="product_image")]
        return links
    except Exception as e:
        logging.error(f"Unexpected error occurred while fetching {url}: {e}")
        return []

async def extract_links(total_pages=int(6020 / 50), batch_size=20):
    try:
        urls = [f"{base_url}/brands.html?p={i + 1}" for i in range(total_pages)]
        
        all_links = []
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(urls), batch_size):
                batch_urls = urls[i:i+batch_size]
                tasks = [extract_single_page(session, url) for url in batch_urls]
                results = await asyncio.gather(*tasks)
                
                all_links.extend([link for sublist in results for link in sublist])
                
                logging.info(f"Batch {i // batch_size + 1} completed. Sleeping for 3 seconds.")
                await asyncio.sleep(3)
        
        return all_links
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        return []

async def extract_single_watch(session, url):
    try:
        html = await fetch(session, url)
        soup = BeautifulSoup(html, "lxml")
        name = soup.find(class_="color_9D font_24 fFamily_mrsEaves fStyle_italic text-transform-none fWeight_regular d-block").text.strip()
        price = soup.find(class_="price").text

        data = {
            "name": name,
            "price": price
        }

        for row in soup.find(class_="specColWidth specFirstRow").find_all(class_ = 'specCol'):
            name = row.find(class_="specName").text.strip()
            value = row.find(class_="specValue").text.strip()
            data[name] = value

        for row in soup.find_all(class_="specRow"):
            name = row.find(class_="specName").text.strip()
            value = row.find(class_="specValue").text.strip()
            data[name] = value

        for row in soup.find_all(class_="calibre_sepcColumn specRow"):
            name = row.find(class_="specName").text.strip()
            value = row.find(class_="specValue").text.strip()
            data[name] = value

        return data
    except Exception as e:
        logging.error(f"Unexpected error occurred while fetching {url}: {e}")
        return None

async def extract_watch_data(links, batch_size=10):
    try:
        data = []
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(links), batch_size):
                batch_links = links[i:i+batch_size]
                tasks = [extract_single_watch(session, url) for url in batch_links]
                results = await asyncio.gather(*tasks)
                
                data.extend([watch_data for watch_data in results if watch_data is not None])
                
                logging.info(f"Batch {i // batch_size + 1} completed. Sleeping for 5 seconds.")
                await asyncio.sleep(5)
        
        return data
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        return []