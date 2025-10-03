import os
import time
import random
import requests
from math import ceil
from typing import Dict, Any, Optional
from db.db_operations import init_db, save_url, load_saved_urls
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime
import json
from dotenv import load_dotenv

load_dotenv()

# ---------- CONFIG ----------
EXCEL_FILE = "sheets/Catawiki4_14.xlsx"
CHECKPOINT_FILE = "crawler_checkpoint.json"
LOG_FILE = "crawler.log"

# Oxylabs credentials
OXYLABS_USERNAME = os.getenv("OXYLABS_USERNAME")
OXYLABS_PASSWORD = os.getenv("OXYLABS_PASSWORD")

RESULTS_PER_PAGE = 10
DEFAULT_MAX_START = 3000
REQUEST_SLEEP = (1.0, 2.0)
FIRST_SLEEP = (2.0, 4.0)
MAX_RETRIES = 5
MAX_CONSECUTIVE_EMPTY = 2
OXYLABS_ENDPOINT = "https://realtime.oxylabs.io/v1/queries"

# ---------- LOGGING ----------
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

setup_logging()
logger = logging.getLogger("crawler")

# ---------- CHECKPOINT ----------
def load_checkpoint() -> Optional[Dict[str, Any]]:
    if Path(CHECKPOINT_FILE).exists():
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            logger.exception("Failed to read checkpoint file; starting fresh.")
    return None

def save_checkpoint(state: Dict[str, Any], log: bool = False):
    tmp = CHECKPOINT_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, CHECKPOINT_FILE)
        if log:
            logger.info(f"Checkpoint saved: sheet={state.get('sheet')} | category={state.get('category')} | "
                        f"subcategory={state.get('subcategory')} | brand={state.get('brand')} | start={state.get('start')}")
    except Exception:
        logger.exception("Failed to save checkpoint.")

def clear_checkpoint():
    try:
        if Path(CHECKPOINT_FILE).exists():
            Path(CHECKPOINT_FILE).unlink()
            logger.info("Checkpoint cleared.")
    except Exception:
        logger.exception("Failed to clear checkpoint file.")

# ---------- EXCEL LOADER ----------
def load_subcategories_from_excel(file_path: str, sheet_name: str):
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception:
        logger.exception(f"Failed to read Excel sheet: {sheet_name}")
        raise
    if df.empty:
        raise ValueError(f"Excel sheet '{sheet_name}' is empty!")
    CATEGORY = str(df.iloc[0]['Category']).strip()
    subcategories = {}
    row_mapping = {}
    for idx, row in df.iterrows():
        subcategory = str(row['SubCategories']).strip()
        brand = str(row['Brand']).strip()
        subcategories.setdefault(subcategory, [])
        if brand not in subcategories[subcategory]:
            subcategories[subcategory].append(brand)
        row_mapping[(subcategory, brand)] = idx + 2
    return CATEGORY, subcategories, row_mapping

# ---------- OXYLABS SEARCH ----------
def oxylabs_search(session: requests.Session, query: str, start: int = 0, num: int = 100):
    """
    Perform Google search via Oxylabs Realtime API.
    Oxylabs uses 'start_page' parameter (1-indexed) instead of 'start' offset.
    """
    # Convert start offset to page number (1-indexed)
    page = (start // num) + 1
    
    payload = {
        'source': 'google_search',
        'query': query,
        'start_page': page,
        'pages': 1,
        'parse': True,
        'context': [
            {'key': 'filter', 'value': 0},  # Disable auto-filtering
            {'key': 'results_language', 'value': 'en'}
        ]
    }
    
    return session.post(
        OXYLABS_ENDPOINT,
        auth=(OXYLABS_USERNAME, OXYLABS_PASSWORD),
        json=payload,
        timeout=60
    )

def extract_results_from_oxylabs(data: dict):
    """
    Extract organic results from Oxylabs response.
    Oxylabs structure: data['results'][0]['content']['results']['organic']
    """
    try:
        results = data.get('results', [])
        if not results:
            return []
        
        content = results[0].get('content', {})
        organic = content.get('results', {}).get('organic', [])
        return organic
    except (IndexError, KeyError, TypeError) as e:
        logger.warning(f"Failed to extract organic results: {e}")
        return []

def extract_total_results(data: dict) -> Optional[int]:
    """
    Extract total results count from Oxylabs response.
    """
    try:
        results = data.get('results', [])
        if not results:
            return None
        
        content = results[0].get('content', {})
        search_info = content.get('results', {}).get('search_information', {})
        total = search_info.get('total_results_count')
        
        if total:
            # Remove commas and convert to int
            return int(str(total).replace(',', ''))
    except (IndexError, KeyError, ValueError, TypeError):
        pass
    
    return None

def extract_link_from_result(result: dict) -> str:
    """Extract URL from Oxylabs result."""
    for key in ("url", "link"):
        val = result.get(key)
        if isinstance(val, str) and val.startswith("http"):
            return val
    return ""

# ---------- CRAWLER ----------
def crawl(sheet_name: str, resume_state: Optional[Dict[str, Any]] = None):
    logger.info(f"\n{'#'*100}\nProcessing sheet: {sheet_name}\n{'#'*100}")
    CATEGORY, SUBCATEGORIES, ROW_MAPPING = load_subcategories_from_excel(EXCEL_FILE, sheet_name)

    if not OXYLABS_USERNAME or not OXYLABS_PASSWORD:
        logger.error("Oxylabs credentials not set in environment variables.")
        return

    init_db()
    total_urls_found = 0
    session = requests.Session()
    log_stats = []

    ck = {
        "sheet": sheet_name,
        "category": CATEGORY,
        "subcategory": None,
        "brand": None,
        "start": 0,
        "page_num": 0,
        "timestamp": None
    }

    if resume_state and resume_state.get("sheet") == sheet_name:
        ck.update({
            "subcategory": resume_state.get("subcategory"),
            "brand": resume_state.get("brand"),
            "start": resume_state.get("start", 0),
            "page_num": resume_state.get("page_num", 0)
        })
        logger.info(f"Resuming from checkpoint: {ck}")

    # Track if we're still looking for the resume point
    skip_until_resume = resume_state is not None

    try:
        for subcategory, brands in SUBCATEGORIES.items():
            # Skip entire subcategories before the checkpoint
            if skip_until_resume:
                resume_sub = resume_state.get("subcategory")
                if subcategory != resume_sub:
                    logger.info(f"Skipping subcategory '{subcategory}' (before checkpoint)")
                    continue
            
            for brand in brands:
                row_number = ROW_MAPPING.get((subcategory, brand), "Unknown")
                page_num = 0
                brand_urls = 0
                consecutive_empty = 0
                start = 0
                max_start = DEFAULT_MAX_START

                try:
                    # Handle resume logic
                    if skip_until_resume:
                        resume_sub = resume_state.get("subcategory")
                        resume_brand = resume_state.get("brand")
                        
                        # Within the checkpoint subcategory, skip brands before the checkpoint
                        if subcategory == resume_sub and brand != resume_brand:
                            # Check if this brand comes before the resume brand in the list
                            brands_in_sub = SUBCATEGORIES[subcategory]
                            try:
                                if brands_in_sub.index(brand) < brands_in_sub.index(resume_brand):
                                    logger.info(f"Skipping brand '{brand}' (before checkpoint)")
                                    continue
                            except ValueError:
                                # If brand not in list (shouldn't happen), process it
                                pass
                        
                        # Exact match - resume from checkpoint
                        if subcategory == resume_sub and brand == resume_brand:
                            start = resume_state.get("start", 0)
                            page_num = resume_state.get("page_num", 0)
                            logger.info(f"Resuming brand '{brand}' from page {page_num}, start={start}")
                            skip_until_resume = False  # Clear flag after resuming

                    ck["subcategory"] = subcategory
                    ck["brand"] = brand
                    ck["start"] = start
                    ck["page_num"] = page_num

                    logger.info(f"\n{'='*100}\nCrawling -> Row: {row_number} | Category: '{CATEGORY}' | "
                                f"Subcategory: '{subcategory}' | Brand: '{brand}'")

                    time.sleep(random.uniform(*FIRST_SLEEP))
                    seen_urls = load_saved_urls(CATEGORY, subcategory, brand)

                    query = f'site:catawiki.com/en/l/ "{subcategory.strip()}" "{brand.strip()}" ("Sold" OR "Final bid")'

                    # First request to get total results estimate
                    try:
                        first_resp = oxylabs_search(session, query, start=0, num=RESULTS_PER_PAGE)
                        first_resp.raise_for_status()
                        first_data = first_resp.json()
                        total_results = extract_total_results(first_data)
                        
                        if total_results and total_results > 0:
                            estimated_pages = ceil(total_results / RESULTS_PER_PAGE)
                            max_start = (estimated_pages - 1) * RESULTS_PER_PAGE
                            logger.info(f"Estimated total results: {total_results} | Estimated pages: {estimated_pages} | MAX_START={max_start}")
                        else:
                            logger.info(f"Total results estimate: N/A | Using default MAX_START: {DEFAULT_MAX_START}")
                    except Exception as e:
                        logger.warning(f"First request failed: {e}. Using default MAX_START.")

                    # Pagination
                    while start <= max_start:
                        try:
                            ck.update({"start": start, "page_num": page_num, "timestamp": datetime.utcnow().isoformat()})
                            save_checkpoint(ck)

                            time.sleep(random.uniform(*REQUEST_SLEEP))
                            data = None
                            
                            for attempt in range(1, MAX_RETRIES + 1):
                                try:
                                    resp = oxylabs_search(session, query, start=start, num=RESULTS_PER_PAGE)
                                    resp.raise_for_status()
                                    data = resp.json()
                                    break
                                except requests.RequestException as e:
                                    backoff = (2 ** attempt) + random.random()
                                    logger.warning(f"Request error (attempt {attempt}/{MAX_RETRIES}) | start={start}: {e}. Backing off {backoff:.1f}s")
                                    time.sleep(backoff)

                            if data is None:
                                logger.error(f"Skipping start={start} after {MAX_RETRIES} failed attempts.")
                                consecutive_empty += 1
                                start += RESULTS_PER_PAGE
                                continue

                            organic = extract_results_from_oxylabs(data)
                            total_results = extract_total_results(data)
                            logger.info(f"start={start} (page {page_num+1}) | Organic results: {len(organic)} | Total results estimate: {total_results or 'N/A'}")

                            found_this_page = 0
                            for result in organic:
                                link = extract_link_from_result(result)
                                if "catawiki.com/en/l/" in link:
                                    clean = link.split("&")[0]
                                    if clean not in seen_urls:
                                        seen_urls.add(clean)
                                        try:
                                            if save_url(CATEGORY, subcategory, brand, clean):
                                                total_urls_found += 1
                                                found_this_page += 1
                                                brand_urls += 1
                                        except Exception:
                                            logger.exception(f"Failed to save URL: {clean}")

                            logger.info(f"Page {page_num+1} | Found this page: {found_this_page}")
                            page_num += 1

                            if found_this_page == 0:
                                consecutive_empty += 1
                                if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                                    logger.info(f"{MAX_CONSECUTIVE_EMPTY} consecutive empty pages. Stopping pagination for brand='{brand}'")
                                    break
                            else:
                                consecutive_empty = 0

                            if start + RESULTS_PER_PAGE > max_start:
                                logger.info(f"Reached MAX_START ({max_start}) for brand='{brand}'. Stopping pagination.")
                                break

                            start += RESULTS_PER_PAGE

                        except KeyboardInterrupt:
                            logger.warning("Interrupted! Saving checkpoint and exiting...")
                            save_checkpoint(ck, log=True)
                            raise

                except Exception:
                    logger.exception(f"Exception processing brand='{brand}'. Continuing with next brand...")

                finally:
                    log_stats.append({
                        "Row": row_number,
                        "Category": CATEGORY,
                        "Subcategory": subcategory,
                        "Brand": brand,
                        "Pages Crawled": page_num,
                        "Total URLs": brand_urls
                    })
                    
                    save_checkpoint(ck)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Saving checkpoint and exiting...")
        save_checkpoint(ck, log=True)
        raise

    finally:
        logger.info("\n" + "="*100)
        logger.info(f"{'Row':<5} {'Category':<30} {'Subcategory':<30} {'Brand':<15} {'Pages Crawled':<15} {'Total URLs':<10}")
        logger.info("-"*100)
        for stat in log_stats:
            logger.info(f"{stat['Row']:<5} {stat['Category']:<30} {stat['Subcategory']:<30} {stat['Brand']:<15} "
                        f"{stat['Pages Crawled']:<15} {stat['Total URLs']:<10}")
        logger.info("="*100)
        logger.info(f"Total new URLs found this run: {total_urls_found}")

# ---------- RUN ----------
if __name__ == "__main__":
    resume = load_checkpoint()
    try:
        xls = pd.ExcelFile(EXCEL_FILE)
    except Exception:
        logger.exception(f"Failed to open Excel workbook: {EXCEL_FILE}")
        raise

    for sheet in xls.sheet_names:
        try:
            if resume and resume.get("sheet") != sheet:
                logger.info(f"Skipping sheet '{sheet}' because checkpoint exists for sheet '{resume.get('sheet')}'.")
                continue
            if resume and resume.get("sheet") == sheet:
                crawl(sheet, resume_state=resume)
                resume = None
            else:
                crawl(sheet)
        except Exception:
            logger.exception(f"Stopped processing sheet '{sheet}'. Check logs and checkpoint to resume.")
            break

    logger.info("Crawler finished. Re-run script to resume if checkpoint exists.")