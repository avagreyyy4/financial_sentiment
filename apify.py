import requests
import json
from pathlib import Path
from datetime import date

# Replace this with your working dataset ID
KNOWN_DATASET_ID = "vnBPGagn8nZSmpnnw"  # example
APIFY_TOKEN = "apify_api_7ua5RrN817059hOmIroWLsfQk1mxi13J01K1"

def pull_dataset(dataset_id):
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?format=json&clean=true&token={APIFY_TOKEN}"
    res = requests.get(url)
    if res.status_code != 200:
        print(f"❌ Failed to fetch dataset {dataset_id}: {res.status_code}")
        print(res.text)
        return

    data = res.json()
    if not data:
        print("⚠️ Pulled dataset but it was empty.")
        return

    out_dir = Path(f"data/pull_{date.today().isoformat()}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "manual_dataset_pull.json"
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"✅ Saved {len(data)} items to {out_path.resolve()}")

pull_dataset(KNOWN_DATASET_ID)

