from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import random
from queue import Queue
import threading

# ======================================
# CONFIGURATION
# ======================================
CATEGORIES = [
    "computer shop",
    "laptop shop",
    "mobile shop",
    "electronics wholesale",
    "CCTV dealer",
    "printer dealer",
    "computer repair",
    "laptop repair",
    "networking equipment dealer",
    "computer accessories shop",
    "gaming store",
    "camera shop",
    "electrical shop",
    "LED lighting dealer",
    "appliance showroom",
    "printing shop"
]

AREA = "lamington road mumbai"

MAX_THREADS = 10
SCROLL_PAUSE_TIME = 2.0  # Increased for proper rendering
MAX_NO_CHANGE = 3  # Increased to be more patient
MIN_SCROLL_ITERATIONS = 15  # Force minimum scrolls
BROWSER_POOL_SIZE = 7

# ======================================
# CHROME OPTIONS
# ======================================
def get_options():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-logging")
    opts.add_argument("--log-level=3")
    opts.add_argument("--silent")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    # Remove page_load_strategy to ensure full page load
    return opts

# ======================================
# BROWSER POOL
# ======================================
class BrowserPool:
    def __init__(self, size=5):
        self.pool = Queue(maxsize=size)
        self.lock = threading.Lock()
        print(f"🌐 Initializing {size} browsers...")
        for i in range(size):
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=get_options()
            )
            self.pool.put(driver)
            print(f"  ✓ Browser {i+1}/{size} ready")
    
    def get(self):
        return self.pool.get()
    
    def put(self, driver):
        self.pool.put(driver)
    
    def close_all(self):
        while not self.pool.empty():
            driver = self.pool.get()
            driver.quit()

# ======================================
# SCROLL + GET LINKS (FIXED)
# ======================================
def get_links_for_query(query, browser_pool):
    driver = browser_pool.get()
    wait = WebDriverWait(driver, 15)

    print(f"\n🔍 Searching: {query}")
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}/"
    
    try:
        driver.get(search_url)
        time.sleep(5)  # Initial load time

        results_container = wait.until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@aria-label, 'Results for')]"))
        )
        print("✅ Container loaded")

        scroll_box = results_container
        previous_count = 0
        no_change_counter = 0
        scroll_iteration = 0
        
        print("📜 Scrolling to load all listings...")

        while True:
            # Scroll down
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight;", 
                scroll_box
            )
            time.sleep(SCROLL_PAUSE_TIME)
            scroll_iteration += 1

            # Count current listings
            listings = driver.find_elements(By.XPATH, "//a[contains(@href, '/maps/place/')]")
            current_count = len(listings)
            
            # Check for "You've reached the end" message
            try:
                end_message = driver.find_element(By.XPATH, 
                    "//span[contains(text(), \"You've reached the end\") or contains(text(), 'reached the end')]")
                if end_message:
                    print("🏁 Reached end of results")
                    break
            except:
                pass
            
            if current_count == previous_count:
                no_change_counter += 1
                print(f"⏳ No new results... ({no_change_counter}/{MAX_NO_CHANGE}) [Iteration {scroll_iteration}]")
                
                # Only exit if we've scrolled enough AND no changes
                if no_change_counter >= MAX_NO_CHANGE and scroll_iteration >= MIN_SCROLL_ITERATIONS:
                    print(f"✋ Stopping after {scroll_iteration} scrolls")
                    break
            else:
                print(f"📍 {current_count} listings... [Iteration {scroll_iteration}]")
                no_change_counter = 0
                previous_count = current_count
            
            # Safety limit to prevent infinite loops
            if scroll_iteration > 100:
                print("⚠️ Max iterations reached")
                break

        # Collect unique links
        links = []
        seen = set()
        listings = driver.find_elements(By.XPATH, "//a[contains(@href, '/maps/place/')]")
        
        for l in listings:
            href = l.get_attribute("href")
            if href and href not in seen:
                links.append(href)
                seen.add(href)

        print(f"✅ Found {len(links)} unique listings for '{query}'")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        links = []
    finally:
        browser_pool.put(driver)
    
    return links

# ======================================
# SCRAPE DETAILS FROM ONE LINK
# ======================================
def scrape_listing(link, browser_pool):
    driver = browser_pool.get()
    wait = WebDriverWait(driver, 10)
    result = {"listing_url": link}

    try:
        driver.get(link)
        time.sleep(1)  # Brief wait for initial load
        wait.until(EC.presence_of_element_located((By.XPATH, "//h1[contains(@class, 'DUwDvf')]")))

        def safe(xpath):
            try:
                return driver.find_element(By.XPATH, xpath).text
            except:
                return None

        result.update({
            "name": safe("//h1[contains(@class, 'DUwDvf')]"),
            "category": safe("//button[contains(@aria-label,'category')]/div/div[2]"),
            # "rating": safe("//span[contains(@aria-label,'stars')]"),
            # "reviews": safe("//span[contains(text(),'review') or contains(text(),'Ratings')]"),
            "address": safe("//button[contains(@data-item-id,'address')]/div/div[2]"),
            "phone": safe("//button[contains(@data-item-id,'phone:tel')]/div/div[2]"),
            "website": safe("//a[contains(@data-item-id,'authority')]/div/div[2]")
        })
    except Exception as e:
        result["error"] = str(e)
    finally:
        browser_pool.put(driver)

    return result

# ======================================
# MAIN SCRIPT
# ======================================
if __name__ == "__main__":
    start_time = time.time()
    
    print("🚀 Starting Google Maps Scraper")
    print("="*60)
    browser_pool = BrowserPool(size=BROWSER_POOL_SIZE)
    
    all_results = []
    all_links = {}

    # PHASE 1: Collect all links
    print("\n" + "="*60)
    print("PHASE 1: Collecting all links")
    print("="*60)
    
    for i, category in enumerate(CATEGORIES, 1):
        print(f"\n[Category {i}/{len(CATEGORIES)}]")
        query = f"{category} in {AREA}"
        links = get_links_for_query(query, browser_pool)
        if links:
            all_links[category] = links
        time.sleep(random.uniform(3, 5))  # Pause between categories

    # PHASE 2: Scrape all listings in parallel
    print("\n" + "="*60)
    print("PHASE 2: Scraping listing details")
    print("="*60)
    
    total_links = sum(len(links) for links in all_links.values())
    print(f"📊 Total listings to scrape: {total_links}\n")
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {}
        for category, links in all_links.items():
            for link in links:
                future = executor.submit(scrape_listing, link, browser_pool)
                futures[future] = (category, link)
        
        completed = 0
        for future in as_completed(futures):
            category, link = futures[future]
            res = future.result()
            res["search_query"] = category
            all_results.append(res)
            
            completed += 1
            name = res.get("name", "Unknown")
            if res.get("error"):
                print(f"⚠️ [{completed}/{total_links}] Error: {name[:40]}")
            else:
                print(f"✅ [{completed}/{total_links}] {name[:40]}")

    # Cleanup
    print("\n🧹 Closing browsers...")
    browser_pool.close_all()

    # ======================================
    # REMOVE DUPLICATES & SAVE
    # ======================================
    df = pd.DataFrame(all_results)
    initial_count = len(df)
    df.drop_duplicates(subset=["name", "address"], inplace=True, ignore_index=True)
    
    # Remove entries with no name (failed scrapes)
    df = df[df['name'].notna()]
    
    df.to_csv("lamington_it_places_complete.csv", index=False, encoding="utf-8-sig")

    elapsed = time.time() - start_time
    print("\n" + "="*60)
    print(f"✅ SCRAPING COMPLETE!")
    print(f"📊 {initial_count} total → {len(df)} unique listings")
    print(f"⏱️ Time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print(f"💾 Saved to: lamington_it_places_complete.csv")
    print("="*60)