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
    "gold jewelry shop",
    "diamond jewelry shop",
    "silver jewelry shop",
    "gemstone dealer",
    "jewelry manufacturer",
    "jewelry wholesaler",
    "goldsmith workshop",
    "jewelry casting service",
    "jewelry design studio",
    "jewelry machinery supplier",
    "jewelry tools supplier",
    "bullion dealer",
    "precious metal refiner",
    "assaying and hallmarking center",
    "safe and locker supplier",
    "CCTV and security system dealer",
    "packaging material supplier",
    "display and showcase manufacturer",
    "weighing scale dealer",
    "accounting and taxation consultant",
    "courier and logistics service",
    "office furniture dealer",
    "printing and branding shop",
    "travel agent",
    "tea and snack café"
]


AREA = "zaveri bazaar mumbai"


MAX_THREADS = 10
MAX_LINK_COLLECTION_THREADS = 3
SCROLL_PAUSE_TIME = 2.0
MAX_NO_CHANGE = 3
MIN_SCROLL_ITERATIONS = 15
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
# SCROLL + GET LINKS
# ======================================
def get_links_for_query(query, category, browser_pool):
    """
    Returns tuple: (category, links_list)
    """
    driver = browser_pool.get()
    wait = WebDriverWait(driver, 15)

    print(f"\n🔍 Searching: {query}")
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}/"

    try:
        driver.get(search_url)
        time.sleep(5)

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
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight;", 
                scroll_box
            )
            time.sleep(SCROLL_PAUSE_TIME)
            scroll_iteration += 1

            listings = driver.find_elements(By.XPATH, "//a[contains(@href, '/maps/place/')]")
            current_count = len(listings)

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

                if no_change_counter >= MAX_NO_CHANGE and scroll_iteration >= MIN_SCROLL_ITERATIONS:
                    print(f"✋ Stopping after {scroll_iteration} scrolls")
                    break
            else:
                print(f"📍 {current_count} listings... [Iteration {scroll_iteration}]")
                no_change_counter = 0
                previous_count = current_count

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

    return (category, links)


# ======================================
# SCRAPE DETAILS FROM ONE LINK
# ======================================
def scrape_listing(link, category, browser_pool, stats_counter, stats_lock):
    driver = browser_pool.get()
    wait = WebDriverWait(driver, 10)
    result = {"listing_url": link}

    try:
        driver.get(link)
        time.sleep(1)
        wait.until(EC.presence_of_element_located((By.XPATH, "//h1[contains(@class, 'DUwDvf')]")))

        def safe(xpath):
            try:
                return driver.find_element(By.XPATH, xpath).text
            except:
                return None

        result.update({
            "name": safe("//h1[contains(@class, 'DUwDvf')]"),
            "category": safe("//button[contains(@aria-label,'category')]/div/div[2]"),
            "address": safe("//button[contains(@data-item-id,'address')]/div/div[2]"),
            "phone": safe("//button[contains(@data-item-id,'phone:tel')]/div/div[2]"),
            "website": safe("//a[contains(@data-item-id,'authority')]/div/div[2]"),
            "search_query": category
        })

        # Update stats
        with stats_lock:
            stats_counter['completed'] += 1
            stats_counter['successful'] += 1
            name = result.get("name", "Unknown")[:40]
            print(f"✅ [{stats_counter['completed']}/{stats_counter['total']}] {name}")

    except Exception as e:
        result["error"] = str(e)
        result["search_query"] = category

        with stats_lock:
            stats_counter['completed'] += 1
            stats_counter['failed'] += 1
            print(f"⚠️ [{stats_counter['completed']}/{stats_counter['total']}] Error")
    finally:
        browser_pool.put(driver)

    return result


# ======================================
# MAIN SCRIPT - PROGRESSIVE SCRAPING
# ======================================
if __name__ == "__main__":
    start_time = time.time()

    print("🚀 Starting Google Maps Scraper (Progressive Mode)")
    print("="*60)
    browser_pool = BrowserPool(size=BROWSER_POOL_SIZE)

    all_results = []
    all_links_global = set()  # Global set to track all unique links
    results_lock = threading.Lock()

    # Stats tracking
    stats_counter = {'completed': 0, 'total': 0, 'successful': 0, 'failed': 0}
    stats_lock = threading.Lock()

    print("\n" + "="*60)
    print("🔄 PROGRESSIVE MODE: Scraping starts as links are collected")
    print("="*60)

    # Single executor for all tasks
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as scraping_executor:
        # Track all scraping futures
        scraping_futures = {}

        # Start link collection in parallel
        with ThreadPoolExecutor(max_workers=MAX_LINK_COLLECTION_THREADS) as collection_executor:
            collection_futures = {
                collection_executor.submit(get_links_for_query, f"{cat} in {AREA}", cat, browser_pool): cat 
                for cat in CATEGORIES
            }

            categories_processed = 0

            # Process link collections as they complete
            for future in as_completed(collection_futures):
                category = collection_futures[future]
                categories_processed += 1

                try:
                    returned_category, links = future.result()

                    # Remove duplicates globally before scraping
                    new_links = []
                    duplicate_count = 0

                    for link in links:
                        if link not in all_links_global:
                            all_links_global.add(link)
                            new_links.append(link)
                        else:
                            duplicate_count += 1

                    print(f"\n✅ [{categories_processed}/{len(CATEGORIES)}] {returned_category}")
                    print(f"   📍 Found: {len(links)} links | New: {len(new_links)} | Duplicates: {duplicate_count}")

                    if new_links:
                        # Update total count
                        with stats_lock:
                            stats_counter['total'] += len(new_links)

                        # Immediately submit scraping tasks for new links
                        print(f"   🚀 Starting scraping for {len(new_links)} new links...")

                        for link in new_links:
                            future = scraping_executor.submit(
                                scrape_listing, link, returned_category, browser_pool, 
                                stats_counter, stats_lock
                            )
                            scraping_futures[future] = (returned_category, link)

                except Exception as e:
                    print(f"\n❌ [{categories_processed}/{len(CATEGORIES)}] Error collecting {category}: {e}")

                # Small delay between category completions
                time.sleep(random.uniform(2, 4))

        print(f"\n{'='*60}")
        print(f"📊 Link collection complete!")
        print(f"   Total unique links to scrape: {stats_counter['total']}")
        print(f"   Waiting for all scraping tasks to complete...")
        print(f"{'='*60}\n")

        # Wait for all scraping tasks to complete
        for future in as_completed(scraping_futures):
            category, link = scraping_futures[future]
            try:
                result = future.result()
                with results_lock:
                    all_results.append(result)
            except Exception as e:
                print(f"⚠️ Exception during scraping: {e}")

    # Cleanup
    print("\n🧹 Closing browsers...")
    browser_pool.close_all()

    # ======================================
    # REMOVE DUPLICATES & SAVE
    # ======================================
    df = pd.DataFrame(all_results)
    initial_count = len(df)

    # Remove duplicates by name and address
    df.drop_duplicates(subset=["name", "address"], inplace=True, ignore_index=True)

    # Remove entries with no name (failed scrapes)
    df = df[df['name'].notna()]

    df.to_csv("lamington_it_places_complete.csv", index=False, encoding="utf-8-sig")

    elapsed = time.time() - start_time
    print("\n" + "="*60)
    print(f"✅ SCRAPING COMPLETE!")
    print(f"📊 Results:")
    print(f"   • Total scraped: {initial_count}")
    print(f"   • After deduplication: {len(df)}")
    print(f"   • Successful: {stats_counter['successful']}")
    print(f"   • Failed: {stats_counter['failed']}")
    print(f"⏱️ Time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print(f"💾 Saved to: lamington_it_places_complete.csv")
    print("="*60)


