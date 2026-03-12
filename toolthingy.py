import requests
import json

BASE = "https://api.usaspending.gov/api/v2"

piid = input("Enter Contract Number (PIID): ").strip()

print(f"\nSearching for PIID: {piid} ...\n")

response = requests.post(f"{BASE}/search/spending_by_award/", json={
    "filters": {
        "award_type_codes": ["A", "B", "C", "D"],
        "award_ids": [piid]
    },
    "fields": [
        "Award ID", "Recipient Name", "Award Amount",
        "Start Date", "End Date", "Awarding Agency", "Description",
        "generated_internal_id"
    ],
    "page": 1,
    "limit": 10,
    "sort": "Award Amount",
    "order": "desc"
}, timeout=30)

if response.status_code != 200:
    print(f"Search error {response.status_code}: {response.text}")
else:
    raw_results = response.json().get("results", [])
    if not raw_results:
        print("No awards found.")
    else:
        seen_award_ids = set()
        award_results = []

        for award in raw_results:
            generated_id = award.get("generated_internal_id") or award.get("Award ID")

            if generated_id in seen_award_ids:
                continue
            seen_award_ids.add(generated_id)

            print(f"Award: {award.get('Award ID')} | Recipient: {award.get('Recipient Name')}")

            # Fetch ALL subawards — keep going until the page comes back empty
            subaward_list = []
            seen_sub_numbers = set()
            page = 1

            while True:
                sub_resp = requests.post(f"{BASE}/subawards/", json={
                    "award_id": generated_id,
                    "page": page,
                    "limit": 100,
                    "sort": "amount",
                    "order": "desc"
                }, timeout=30)

                if sub_resp.status_code != 200:
                    print(f"  Subawards error page {page}: {sub_resp.text[:300]}")
                    break

                sub_data = sub_resp.json()
                subawards = sub_data.get("results", [])
                total_reported = sub_data.get("page_metadata", {}).get("total", 0)

                print(f"  Page {page} — got {len(subawards)} records (API reports {total_reported} total)")

                if not subawards:
                    break

                for sub in subawards:
                    sub_number = sub.get("subaward_number")
                    if sub_number in seen_sub_numbers:
                        continue
                    seen_sub_numbers.add(sub_number)

                    subaward_list.append({
                        "id": sub.get("id"),
                        "subaward_number": sub_number,
                        "recipient_name": sub.get("recipient_name"),
                        "amount": sub.get("amount"),
                        "action_date": sub.get("action_date"),
                        "description": sub.get("description"),
                        "recipient_duns": sub.get("recipient_duns"),
                        "recipient_uei": sub.get("recipient_uei"),
                        "prime_award_piid": piid,
                        "recipient_url": sub.get("recipient_url")
                    })

                # Stop only when the page is empty or we've hit the last page
                if len(subawards) < 100:
                    break

                page += 1

            print(f"  Total unique subawards collected: {len(subaward_list)}\n")

            award_results.append({
                "award_id": award.get("Award ID"),
                "recipient_name": award.get("Recipient Name"),
                "award_amount": award.get("Award Amount"),
                "generated_internal_id": generated_id,
                "piid": piid,
                "subawards": subaward_list
            })

        output = {
            "spending_level": "awards",
            "limit": 10,
            "results": award_results
        }

        total_subs = sum(len(a["subawards"]) for a in award_results)
        print(f"Total awards: {len(award_results)}")
        print(f"Total unique subawards: {total_subs}\n")
        print(json.dumps(output, indent=2))

        save = input("\nSave to file? (y/n): ").strip().lower()
        if save == "y":
            filename = f"subawards_{piid}.json"
            with open(filename, "w") as f:
                json.dump(output, f, indent=2)
            print(f"Saved to {filename}")
