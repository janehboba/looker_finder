import requests
from bs4 import BeautifulSoup
import json
import time
import os
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

BASE_URL = "https://api.usaspending.gov/api/v2"
API_HEADERS = {"Content-Type": "application/json"}

# ── Output directory ──────────────────────────────────────────────────────────
OUTPUT_DIR = "/Volumes/your_catalog/your_schema/your_volume/subawards"


def resolve_company_url(company_name: str, delay: float = 1.0) -> dict:
    """
    Search Bing for a company name and return a {company_name: url} key pair.
    Bing is used as it's more scrape-tolerant than Google.
    Falls back to a DuckDuckGo redirect if Bing returns nothing.
    """
    if not company_name:
        return {company_name: None}

    # Clean up name for searching
    query = f"{company_name} official website"
    search_url = f"https://www.bing.com/search?q={requests.utils.quote(query)}&count=5"

    try:
        response = requests.get(search_url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")

        # Grab first organic result
        for li in soup.select("li.b_algo"):
            link_tag = li.select_one("h2 a")
            if link_tag and link_tag.get("href"):
                url = link_tag["href"]
                # Skip PDF links and known non-company domains
                if not any(skip in url for skip in ["linkedin.com", "bloomberg.com", ".pdf", "wikipedia"]):
                    return {company_name: url}

        # DuckDuckGo fallback
        ddg_url = f"https://duckduckgo.com/html/?q={requests.utils.quote(query)}"
        ddg_response = requests.get(ddg_url, headers=HEADERS, timeout=15)
        ddg_soup = BeautifulSoup(ddg_response.text, "lxml")
        first_result = ddg_soup.select_one("a.result__a")
        if first_result and first_result.get("href"):
            return {company_name: first_result["href"]}

    except Exception as e:
        print(f"    URL resolution failed for '{company_name}': {e}")

    time.sleep(delay)
    return {company_name: None}


def resolve_all_company_urls(company_names: list, delay: float = 1.2) -> dict:
    """
    Resolve a list of company names to URLs.
    Returns a dict: { company_name: url, ... }
    De-duplicates so each company is only searched once.
    """
    unique_names = list(set([n for n in company_names if n]))
    url_map = {}

    print(f"\n  Resolving URLs for {len(unique_names)} unique companies...")

    for name in unique_names:
        print(f"    Searching: {name}")
        result = resolve_company_url(name)
        url_map.update(result)
        time.sleep(delay)

    return url_map


# Step 3

def get_award_id_from_piid(piid: str) -> tuple:
    url = f"{BASE_URL}/search/spending_by_award/"
    payload = {
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "keywords": [piid]
        },
        "fields": ["Award ID", "internal_id", "Recipient Name", "Award Amount", "Start Date", "End Date"],
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
    print(f"  Award: {award.get('Award ID')} | Recipient: {award.get('Recipient Name')} | Amount: ${award.get('Award Amount'):,.2f}")
    return award.get("internal_id"), award


def fetch_all_subawards(internal_id: str, page_size: int = 50) -> list:
    url = f"{BASE_URL}/awards/{internal_id}/subawards/"
    all_subawards = []
    page = 1

    while True:
        payload = {
            "award_id": internal_id,
            "limit": page_size,
            "page": page,
            "sort": "subaward_amount",
            "order": "desc"
        }
        response = requests.post(url, json=payload, headers=API_HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if not results:
            break

        all_subawards.extend(results)
        print(f"  Page {page}: {len(results)} subawards (total: {len(all_subawards)})")

        if not data.get("page_metadata", {}).get("hasNext", False):
            break

        page += 1
        time.sleep(0.5)

    return all_subawards

# Step 4

def normalize_subaward(subaward: dict, piid: str, parent_award: dict, url_map: dict) -> dict:
    """
    Normalize subaward and inject the resolved subawardee_url from the url_map.
    """
    recipient_name = subaward.get("recipient_name")

    return {
        # Identifiers
        "piid": piid,
        "subaward_number": subaward.get("subaward_number"),
        "subaward_id": subaward.get("id"),
        "parent_award_id": parent_award.get("Award ID"),
        "parent_internal_id": parent_award.get("internal_id"),

        # Recipient / Awardee Info
        "recipient_name": recipient_name,
        "recipient_unique_id": subaward.get("recipient_unique_id"),
        "recipient_uei": subaward.get("recipient_uei"),
        "recipient_location_city": subaward.get("recipient_location", {}).get("city_name"),
        "recipient_location_state": subaward.get("recipient_location", {}).get("state_code"),
        "recipient_location_zip": subaward.get("recipient_location", {}).get("zip5"),
        "recipient_location_country": subaward.get("recipient_location", {}).get("country_name"),

        # ── Resolved URL key pair ─────────────────────────────────────────────
        "subawardee_url": url_map.get(recipient_name),           # resolved URL
        "subawardee_url_map": {recipient_name: url_map.get(recipient_name)},  # key pair

        # Award Details
        "subaward_amount": subaward.get("subaward_amount"),
        "action_date": subaward.get("action_date"),
        "description": subaward.get("description"),
        "award_type": subaward.get("award_type"),

        # Parent Contract Context
        "prime_recipient_name": parent_award.get("Recipient Name"),
        "prime_award_amount": parent_award.get("Award Amount"),
        "prime_award_start_date": parent_award.get("Start Date"),
        "prime_award_end_date": parent_award.get("End Date"),

        # Metadata
        "scraped_at": datetime.utcnow().isoformat(),
        "source": "USASpending.gov API v2"
    }

# step 5 

def ensure_directory(path: str):
    """Create output directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)
    print(f"  Output directory ready: {path}")


def save_subawards_json(data: dict, piid: str) -> str:
    """
    Save subaward JSON to the output directory.
    Files are organized as: OUTPUT_DIR/PIID/subawards_PIID_TIMESTAMP.json
    """
    safe_piid = piid.replace("/", "_").replace(" ", "_")
    piid_dir = f"{OUTPUT_DIR}/{safe_piid}"
    ensure_directory(piid_dir)

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f"subawards_{safe_piid}_{timestamp}.json"
    filepath = f"{piid_dir}/{filename}"

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"  Saved: {filepath}")
    return filepath


def save_url_map_json(url_map: dict, piid: str) -> str:
    """
    Save the company → URL key pair map as a separate reference file.
    """
    safe_piid = piid.replace("/", "_").replace(" ", "_")
    piid_dir = f"{OUTPUT_DIR}/{safe_piid}"
    ensure_directory(piid_dir)

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f"url_map_{safe_piid}_{timestamp}.json"
    filepath = f"{piid_dir}/{filename}"

    with open(filepath, "w") as f:
        json.dump(url_map, f, indent=2)

    print(f"  URL map saved: {filepath}")
    return filepath

# Step 6

def fetch_subawards_for_contract(piid: str) -> dict:
    """
    Full pipeline:
    1. Resolve PIID → internal award ID
    2. Fetch all subawards
    3. Resolve each subawardee company name → URL
    4. Normalize all records with URL injected
    5. Save JSON files to organized directory
    """
    print(f"\n{'='*60}")
    print(f"Processing contract: {piid}")
    print(f"{'='*60}")

    # 1. Resolve PIID
    internal_id, parent_award = get_award_id_from_piid(piid)

    # 2. Fetch subawards
    print(f"\n  Fetching subawards...")
    raw_subawards = fetch_all_subawards(internal_id)

    if not raw_subawards:
        print(f"  No subawards found for {piid}")
        return {"piid": piid, "total_subawards": 0, "subawards": []}

    # 3. Resolve company URLs (deduplicated)
    company_names = [s.get("recipient_name") for s in raw_subawards]
    url_map = resolve_all_company_urls(company_names)

    # 4. Normalize with URL injected
    normalized = [normalize_subaward(s, piid, parent_award, url_map) for s in raw_subawards]

    result = {
        "piid": piid,
        "internal_award_id": internal_id,
        "parent_award": parent_award,
        "total_subawards": len(normalized),
        "subawardee_url_map": url_map,       # full company → url reference
        "subawards": normalized,
        "scraped_at": datetime.utcnow().isoformat()
    }

    # 5. Save files
    print(f"\n  Saving files...")
    save_subawards_json(result, piid)
    save_url_map_json(url_map, piid)

    print(f"\n  Done. {len(normalized)} subawards processed.")
    return result


# ── Run single contract ───────────────────────────────────────────────────────
piid = "W912BU21C0003"
data = fetch_subawards_for_contract(piid)


# ── Run bulk contracts ────────────────────────────────────────────────────────
def run_bulk(piid_list: list) -> list:
    summary = []
    for piid in piid_list:
        try:
            data = fetch_subawards_for_contract(piid)
            summary.append({
                "piid": piid,
                "status": "success",
                "total_subawards": data["total_subawards"]
            })
        except Exception as e:
            print(f"  ERROR for {piid}: {e}")
            summary.append({"piid": piid, "status": "failed", "error": str(e)})
        time.sleep(2)
    return summary

piids = ["W912BU21C0003", "FA8732-20-C-0001"]
summary = run_bulk(piids)
print(json.dumps(summary, indent=2))

Step 7 

from pyspark.sql.functions import explode, col

df_raw = spark.read.option("multiline", "true").json(f"{OUTPUT_DIR}/*/*.json")

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
    col("sub.subawardee_url"),              # resolved URL column
    col("sub.subawardee_url_map"),          # key pair column
    col("sub.prime_recipient_name"),
    col("sub.prime_award_amount"),
    col("sub.scraped_at")
)

df_subawards.display()

df_subawards.write.format("delta").mode("append").saveAsTable(
    "your_catalog.your_schema.contract_subawards"
)
```

---

