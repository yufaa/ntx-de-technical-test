import os
import json
import polars as pl
import httpx
import asyncio
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm  


BASE_URL = "https://www.fortiguard.com"
DATA_PATH = "ntx-de-technical-test/"
folder_name = "datasets"


if not os.path.exists(DATA_PATH+folder_name):
    os.makedirs(DATA_PATH+folder_name)
print("Directory check passed.")


# Function to crawl data from a URL
async def crawl_data(client, url):
    results = []
    try:
        response = await client.get(url)
        response.raise_for_status()  # Check if request was successful
        html = response.text

        # Parse the HTML
        soup = BeautifulSoup(html, 'html.parser')

        # Find all rows
        rows = soup.find_all('div', class_='row')
        
        # Extract information
        for row in rows:
            try:
                onclick_text = row['onclick']
                link = onclick_text.split("location.href = '")[1].split("'")[0]
                title = row.find('div', class_='col-lg').find('b').text
                results.append({'title': title, 'link': BASE_URL + link})
            except (KeyError, AttributeError):
                # Handle error if data extraction fails
                pass
    except Exception as e:
        print(f"Request failed: {e}")
    
    return results

# Function to scrape data for a single level
async def scrape_level(level, max_pages, client):
    all_results = []
    skipped_pages = []
    
    pages = range(1, max_pages[level-1] + 1)
    
    # Use tqdm to show progress
    for page in tqdm(pages, desc=f"Scraping level {level}", unit="page"):
        url = f'https://www.fortiguard.com/encyclopedia?type=ips&risk={level}&page={page}'
        try:
            print(f"Scraping level {level}, page {page}...")
            results = await crawl_data(client, url)
            if results:
                all_results.extend(results)
            else:
                print(f"No results found on page {page}.")
        except Exception as e:
            print(f"Error scraping page {page}: {e}")
            skipped_pages.append(page)
        await asyncio.sleep(1)  # Be polite to the server

    # Convert results to polars DataFrame
    df = pl.DataFrame(all_results)
    output_file = f'{DATA_PATH}datasets/forti_lists_{level}.csv'
    df.write_csv(output_file)
    print(f"Data for level {level} saved to {output_file}.")

    return skipped_pages

# Function to scrape data for all levels
async def scrape_all_levels(max_pages):
    all_skipped_pages = {}
    
    async with httpx.AsyncClient() as client:
        tasks = [scrape_level(level, max_pages, client) for level in range(1, 6)]
        results = await asyncio.gather(*tasks)

        # Collect skipped pages information
        for level, skipped_pages in enumerate(results, start=1):
            if skipped_pages:
                all_skipped_pages[level] = skipped_pages

    # Save skipped pages to JSON
    skipped_file = DATA_PATH + 'datasets/skipped.json'
    with open(skipped_file, 'w') as f:
        json.dump(all_skipped_pages, f)
    print(f"Skipped pages information saved to {skipped_file}.")

# Define max pages for each level
max_pages = [8, 5, 5, 5, 5]  # Replace with actual max pages if known

# Function to run the asynchronous main function
def run_main():
    loop = asyncio.get_event_loop()
    if loop.is_running():
        # Run the async code in a new task if the event loop is already running
        asyncio.ensure_future(main())
    else:
        # Run the async code normally
        loop.run_until_complete(main())

async def main():
    await scrape_all_levels(max_pages)

if __name__ == "__main__":
    asyncio.run(main())  