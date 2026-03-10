"""
USASpending.gov Award ID Lookup Tool
Fetches detailed award data from the USASpending.gov API using an Award ID.
"""

import requests
import json
import sys
from datetime import datetime


BASE_URL = "https://api.usaspending.gov/api/v2"


def fetch_award_data(award_id: str) -> dict:
    """
    Fetch award data from USASpending.gov using the Award ID.
    Tries the /awards/ endpoint first, which accepts both PIID and generated IDs.
    """
    endpoint = f"{BASE_URL}/awards/{award_id}/"
    headers = {"Content-Type": "application/json"}

    print(f"\n🔍 Fetching award data for ID: {award_id}")
    print(f"   Endpoint: {endpoint}\n")

    response = requests.get(endpoint, headers=headers, timeout=30)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        # Try searching via the /awards/last_updated/ or search endpoint
        print(f"   Direct lookup returned 404. Trying search fallback...\n")
        return search_award_by_id(award_id)
    else:
        response.raise_for_status()


def search_award_by_id(award_id: str) -> dict:
    """
    Fallback: Search for an award using the spending-by-award search endpoint.
    """
    endpoint = f"{BASE_URL}/search/spending_by_award/"
    headers = {"Content-Type": "application/json"}

    payload = {
        "filters": {
            "award_ids": [award_id]
        },
        "fields": [
            "Award ID", "Recipient Name", "Start Date", "End Date",
            "Award Amount", "Awarding Agency", "Awarding Sub Agency",
            "Contract Award Type", "Award Type", "Funding Agency",
            "Funding Sub Agency", "Period of Performance Current End Date",
            "Place of Performance City Code", "Place of Performance State Code",
            "Description", "Base Obligation Date"
        ],
        "page": 1,
        "limit": 5,
        "order": "desc",
        "sort": "Award Amount"
    }

    response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
    response.raise_for_status()

    data = response.json()
    results = data.get("results", [])

    if not results:
        return {"error": f"No award found for ID: {award_id}"}

    return {"search_results": results, "total_count": data.get("page_metadata", {}).get("count", 0)}


def display_award_data(data: dict, award_id: str):
    """
    Pretty-print the award data to the console.
    """
    if "error" in data:
        print(f"❌ Error: {data['error']}")
        return

    print("=" * 70)
    print(f"  AWARD DATA — {award_id}")
    print("=" * 70)

    # Handle direct award lookup response
    if "search_results" not in data:
        fields = {
            "Award ID":               data.get("award_id") or data.get("piid") or award_id,
            "Award Type":             data.get("category", "N/A").replace("_", " ").title(),
            "Description":            data.get("description", "N/A"),
            "Total Obligation":       format_currency(data.get("total_obligation")),
            "Base Obligation Date":   data.get("date_signed", "N/A"),
            "Period of Performance":  f"{data.get('period_of_performance', {}).get('start_date', 'N/A')} → "
                                      f"{data.get('period_of_performance', {}).get('end_date', 'N/A')}",
            "Recipient Name":         data.get("recipient", {}).get("recipient_name", "N/A"),
            "Recipient Location":     build_location(data.get("recipient", {}).get("location", {})),
            "Awarding Agency":        data.get("awarding_agency", {}).get("toptier_agency", {}).get("name", "N/A"),
            "Awarding Sub-Agency":    data.get("awarding_agency", {}).get("subtier_agency", {}).get("name", "N/A"),
            "Funding Agency":         data.get("funding_agency", {}).get("toptier_agency", {}).get("name", "N/A"),
            "Place of Performance":   build_location(data.get("place_of_performance", {})),
        }

        for label, value in fields.items():
            print(f"  {label:<28} {value}")

        # Show NAICS / PSC if present (contracts)
        naics = data.get("latest_transaction_contract_data", {})
        if naics:
            print("\n  --- Contract Details ---")
            print(f"  {'NAICS Code':<28} {naics.get('naics', 'N/A')} — {naics.get('naics_description', '')}")
            print(f"  {'PSC Code':<28} {naics.get('product_or_service_code', 'N/A')} — "
                  f"{naics.get('product_or_service_co_desc', '')}")
            print(f"  {'Type of Contract':<28} {naics.get('type_of_contract_pricing', 'N/A')}")
            print(f"  {'Solicitation ID':<28} {naics.get('solicitation_identifier', 'N/A')}")

    else:
        # Handle search fallback results
        results = data["search_results"]
        print(f"  Found {data['total_count']} result(s). Showing top match(es):\n")
        for i, result in enumerate(results, 1):
            print(f"  Result #{i}")
            print(f"  {'-' * 40}")
            for key, value in result.items():
                if value is not None:
                    label = key.replace("_", " ").title()
                    if "amount" in key.lower() or "obligation" in key.lower():
                        value = format_currency(value)
                    print(f"  {label:<35} {value}")
            print()

    print("=" * 70)
    print(f"  Source: https://www.usaspending.gov/award/{award_id}/")
    print("=" * 70)


def format_currency(value) -> str:
    """Format a number as a USD currency string."""
    if value is None:
        return "N/A"
    try:
        return f"${float(value):,.2f}"
    except (ValueError, TypeError):
        return str(value)


def build_location(loc: dict) -> str:
    """Build a readable location string from a location dict."""
    if not loc:
        return "N/A"
    parts = [
        loc.get("city_name", ""),
        loc.get("state_code", ""),
        loc.get("country_name", ""),
    ]
    result = ", ".join(p for p in parts if p)
    return result or "N/A"


def save_to_json(data: dict, award_id: str):
    """Optionally save the raw response to a JSON file."""
    safe_id = award_id.replace("/", "_").replace("\\", "_")
    filename = f"award_{safe_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\n💾 Raw data saved to: {filename}")


def main():
    print("=" * 70)
    print("       USASpending.gov — Award ID Lookup Tool")
    print("=" * 70)
    print("  Enter an Award ID (Contract PIID, Grant ID, etc.)")
    print("  Example: HHSN316201200083W  |  W912DQ21C0005  |  75D30121C11549")
    print("=" * 70)

    award_id = input("\n  Enter Award ID: ").strip()

    if not award_id:
        print("❌ No Award ID entered. Exiting.")
        sys.exit(1)

    try:
        data = fetch_award_data(award_id)
        display_award_data(data, award_id)

        # Optional: save raw JSON
        save_choice = input("\n  Save raw JSON response? (y/n): ").strip().lower()
        if save_choice == "y":
            save_to_json(data, award_id)

    except requests.exceptions.HTTPError as e:
        print(f"\n❌ HTTP Error: {e}")
        print(f"   Response: {e.response.text[:500] if e.response else 'No response body'}")
    except requests.exceptions.ConnectionError:
        print("\n❌ Connection error. Check your internet connection.")
    except requests.exceptions.Timeout:
        print("\n❌ Request timed out. USASpending API may be slow — try again.")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()
