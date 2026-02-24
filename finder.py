# ============================================================
#  CONFIGURATION — Update all paths and settings here
# ============================================================

# --- Storage Paths ---
# Root volume where all output folders will be created
VOLUME_ROOT       = "/Volumes/your_catalog/your_schema/your_volume"

# Subdirectory for full subaward JSON files
SUBAWARDS_DIR     = f"{VOLUME_ROOT}/subawards"

# Subdirectory for company → URL key pair map files
URL_MAP_DIR       = f"{VOLUME_ROOT}/url_maps"

# Subdirectory for bulk run summary logs
SUMMARY_LOG_DIR   = f"{VOLUME_ROOT}/logs"

# --- Delta Table ---
# Fully qualified table name to write final data into
DELTA_TABLE       = "your_catalog.your_schema.contract_subawards"

# --- Scraping Settings ---
# Seconds to wait between Bing search requests (avoid rate limiting)
URL_RESOLVE_DELAY = 1.2

# Seconds to wait between USASpending API pages
API_PAGE_DELAY    = 0.5

# Seconds to wait between contracts in a bulk run
BULK_RUN_DELAY    = 2.0

# Number of subaward records to fetch per API page
API_PAGE_SIZE     = 50

# ============================================================
print("Configuration loaded.")
print(f"  VOLUME_ROOT    : {VOLUME_ROOT}")
print(f"  SUBAWARDS_DIR  : {SUBAWARDS_DIR}")
print(f"  URL_MAP_DIR    : {URL_MAP_DIR}")
print(f"  SUMMARY_LOG_DIR: {SUMMARY_LOG_DIR}")
print(f"  DELTA_TABLE    : {DELTA_TABLE}")

# Step 3 

import requests
from bs4 import BeautifulSoup
import json
import time
import os
from datetime import datetime

BASE_URL   = "https://api.usaspending.gov/api/v2"
API_HEADERS = {"Content-Type": "application/json"}

WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

Step 4

def ensure_directory(path: str):
    """Create a directory if it doesn't already exist."""
    os.makedirs(path, exist_ok=True)


def save_json(data: dict, directory: str, filename: str) -> str:
    """
    Save a dict as a JSON file to a given directory.
    Creates the directory automatically if it doesn't exist.
    Returns the full filepath.
    """
    ensure_directory(directory)
    filepath = f"{directory}/{filename}"
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Saved → {filepath}")
    return filepath


def save_summary_log(summary: list, label: str = "bulk_run") -> str:
    """Save a bulk run summary log to the SUMMARY_LOG_DIR."""
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename  = f"summary_{label}_{timestamp}.json"
    return save_json({"run_at": datetime.utcnow().isoformat(), "results": summary},
                     SUMMARY_LOG_DIR, filename)

#Step 5 

def get_award_id_from_piid(piid: str) -> tuple:
    """Resolve a PIID to USASpending's internal award ID."""
    url = f"{BASE_URL}/search/spending_by_award/"
    payload = {
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "keywords": [piid]
        },
        "fields": [
            "Award ID", "internal_id", "Recipient Name",
            "Award Amount", "Start Date", "End Date"
        ],
        "limit": 5,
        "page": 1,
        "sort": "Award Amount",
        "order": "desc",
        "subawards": False
    }

    response = requests.post(url, json=payload, headers=API_HEADERS, timeout=15)
    response.raise_for_status()
    results = response.json().get("results", [])

    if not results:
        raise ValueError(f"No award found for PIID: {piid}")

    award = results[0]
    print(f"  Award found  : {award.get('Award ID')}")
    print(f"  Prime vendor : {award.get('Recipient Name')}")
    print(f"  Award amount : ${award.get('Award Amount'):,.2f}")
    return award.get("internal_id"), award


def fetch_all_subawards(internal_id: str) -> list:
    """Fetch all subaward records for an award, handling pagination."""
    url           = f"{BASE_URL}/awards/{internal_id}/subawards/"
    all_subawards = []
    page          = 1

    while True:
        payload = {
            "award_id": internal_id,
            "limit": API_PAGE_SIZE,
            "page": page,
            "sort": "subaward_amount",
            "order": "desc"
        }
        response = requests.post(url, json=payload, headers=API_HEADERS, timeout=15)
        response.raise_for_status()
        data    = response.json()
        results = data.get("results", [])

        if not results:
            break

        all_subawards.extend(results)
        print(f"  Page {page}: fetched {len(results)} subawards (running total: {len(all_subawards)})")

        if not data.get("page_metadata", {}).get("hasNext", False):
            break

        page += 1
        time.sleep(API_PAGE_DELAY)

    return all_subawards

# Step 6 

def resolve_company_url(company_name: str) -> str | None:
    """Search Bing for a company's website. Falls back to DuckDuckGo."""
    if not company_name:
        return None

    query      = f"{company_name} official website"
    search_url = f"https://www.bing.com/search?q={requests.utils.quote(query)}&count=5"

    SKIP_DOMAINS = ["linkedin.com", "bloomberg.com", "wikipedia.org", ".pdf",
                    "zoominfo.com", "dnb.com", "glassdoor.com"]
    try:
        response = requests.get(search_url, headers=WEB_HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

        for li in soup.select("li.b_algo"):
            link_tag = li.select_one("h2 a")
            if link_tag and link_tag.get("href"):
                url = link_tag["href"]
                if not any(skip in url for skip in SKIP_DOMAINS):
                    return url

        # DuckDuckGo fallback
        ddg_response = requests.get(
            f"https://duckduckgo.com/html/?q={requests.utils.quote(query)}",
            headers=WEB_HEADERS, timeout=15
        )
        ddg_soup     = BeautifulSoup(ddg_response.text, "lxml")
        first_result = ddg_soup.select_one("a.result__a")
        if first_result and first_result.get("href"):
            return first_result["href"]

    except Exception as e:
        print(f"    URL resolution failed for '{company_name}': {e}")

    return None


def resolve_all_company_urls(company_names: list) -> dict:
    """
    Resolve a deduplicated list of company names to URLs.
    Returns dict: { company_name: url }
    """
    unique_names = list(set(n for n in company_names if n))
    url_map      = {}

    print(f"\n  Resolving URLs for {len(unique_names)} unique companies...")
    for name in unique_names:
        print(f"    → {name}")
        url_map[name] = resolve_company_url(name)
        time.sleep(URL_RESOLVE_DELAY)

    return url_map

# Step 7

def normalize_subaward(subaward: dict, piid: str, parent_award: dict, url_map: dict) -> dict:
    """Normalize a raw subaward record and inject the resolved subawardee URL."""
    recipient_name = subaward.get("recipient_name")

    return {
        # Identifiers
        "piid"                    : piid,
        "subaward_number"         : subaward.get("subaward_number"),
        "subaward_id"             : subaward.get("id"),
        "parent_award_id"         : parent_award.get("Award ID"),
        "parent_internal_id"      : parent_award.get("internal_id"),

        # Subawardee Info
        "recipient_name"          : recipient_name,
        "recipient_unique_id"     : subaward.get("recipient_unique_id"),
        "recipient_uei"           : subaward.get("recipient_uei"),
        "recipient_location_city" : subaward.get("recipient_location", {}).get("city_name"),
        "recipient_location_state": subaward.get("recipient_location", {}).get("state_code"),
        "recipient_location_zip"  : subaward.get("recipient_location", {}).get("zip5"),
        "recipient_location_country": subaward.get("recipient_location", {}).get("country_name"),

        # Resolved URL
        "subawardee_url"          : url_map.get(recipient_name),
        "subawardee_url_map"      : {recipient_name: url_map.get(recipient_name)},

        # Award Details
        "subaward_amount"         : subaward.get("subaward_amount"),
        "action_date"             : subaward.get("action_date"),
        "description"             : subaward.get("description"),
        "award_type"              : subaward.get("award_type"),

        # Prime Contract Context
        "prime_recipient_name"    : parent_award.get("Recipient Name"),
        "prime_award_amount"      : parent_award.get("Award Amount"),
        "prime_award_start_date"  : parent_award.get("Start Date"),
        "prime_award_end_date"    : parent_award.get("End Date"),

        # Metadata
        "scraped_at"              : datetime.utcnow().isoformat(),
        "source"                  : "USASpending.gov API v2"
    }

# Step 8

def fetch_subawards_for_contract(piid: str) -> dict:
    """
    Full pipeline for a single PIID:
    1. Resolve PIID → internal award ID
    2. Fetch all subawards
    3. Resolve subawardee company names → URLs
    4. Normalize all records
    5. Save subaward JSON and URL map JSON to their configured directories
    """
    print(f"\n{'='*60}")
    print(f"  Contract : {piid}")
    print(f"{'='*60}")

    # 1. Resolve PIID
    internal_id, parent_award = get_award_id_from_piid(piid)

    # 2. Fetch subawards
    print(f"\n  Fetching subawards...")
    raw_subawards = fetch_all_subawards(internal_id)

    if not raw_subawards:
        print(f"  No subawards found for {piid}.")
        return {"piid": piid, "total_subawards": 0, "subawards": []}

    # 3. Resolve URLs
    company_names = [s.get("recipient_name") for s in raw_subawards]
    url_map       = resolve_all_company_urls(company_names)

    # 4. Normalize
    normalized = [normalize_subaward(s, piid, parent_award, url_map) for s in raw_subawards]

    result = {
        "piid"              : piid,
        "internal_award_id" : internal_id,
        "parent_award"      : parent_award,
        "total_subawards"   : len(normalized),
        "subawardee_url_map": url_map,
        "subawards"         : normalized,
        "scraped_at"        : datetime.utcnow().isoformat()
    }

    # 5. Save files
    safe_piid = piid.replace("/", "_").replace(" ", "_")
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

    print(f"\n  Saving output files...")
    save_json(result,  SUBAWARDS_DIR, f"subawards_{safe_piid}_{timestamp}.json")
    save_json(url_map, URL_MAP_DIR,   f"url_map_{safe_piid}_{timestamp}.json")

    print(f"\n  Complete — {len(normalized)} subawards processed.")
    return result


#Step 9 

# ============================================================
#  RUN — Enter your contract number(s) below
# ============================================================

# Single contract
contract_number = input("Enter Contract ID (PIID): ").strip()

if not contract_number:
    raise ValueError("No contract number entered. Please provide a PIID.")

data = fetch_subawards_for_contract(contract_number)


# ── Optional: bulk mode ───────────────────────────────────────
# Uncomment and add PIIDs to run multiple contracts at once

# contract_numbers = [
#     "W912BU21C0003",
#     "FA8732-20-C-0001",
# ]
# summary = []
# for piid in contract_numbers:
#     try:
#         result = fetch_subawards_for_contract(piid)
#         summary.append({"piid": piid, "status": "success", "total_subawards": result["total_subawards"]})
#     except Exception as e:
#         summary.append({"piid": piid, "status": "failed", "error": str(e)})
#     time.sleep(BULK_RUN_DELAY)
# save_summary_log(summary, label="bulk_run")
# print(json.dumps(summary, indent=2))

#Step 10 

from pyspark.sql.functions import explode, col

df_raw = spark.read.option("multiline", "true").json(f"{SUBAWARDS_DIR}/*.json")

df_subawards = df_raw.select(
    col("piid"),
    explode("subawards").alias("sub")
).select(
    col("piid"),
    col("sub.subaward_number"),
    col("sub.subaward_id"),
    col("sub.recipient_name"),
    col("sub.recipient_uei"),
    col("sub.recipient_location_city"),
    col("sub.recipient_location_state"),
    col("sub.subaward_amount"),
    col("sub.action_date"),
    col("sub.description"),
    col("sub.subawardee_url"),
    col("sub.subawardee_url_map"),
    col("sub.prime_recipient_name"),
    col("sub.prime_award_amount"),
    col("sub.scraped_at")
)

df_subawards.display()

df_subawards.write.format("delta").mode("append").saveAsTable(DELTA_TABLE)
print(f"Written to Delta table: {DELTA_TABLE}")
