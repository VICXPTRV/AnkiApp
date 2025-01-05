import requests
from  datetime import datetime
from notion_client import Client
from tqdm import tqdm
import os
import csv
import re

API_KEY = 'secret_key'
nclient = Client(auth=API_KEY)

def query_database_with_filter(db_id, filter=None):
	"""Query the Notion database with a filter."""
	results = []
	start_cursor = None

	while True:
		payload = {}
		if filter:
			payload['filter'] = filter  # Apply filter
		if start_cursor:
			payload['start_cursor'] = start_cursor

		# Pass the filter to the query
		response = nclient.databases.query(database_id=db_id, **payload)
		results.extend(response.get('results', []))
		if not response.get('has_more'):
			break
		start_cursor = response.get('next_cursor')

	return results

def download_image(file_url, folder, name):
	if not os.path.exists(folder):
		os.makedirs(folder)

	# Sanitize the name to create a valid filename
	safe_name = "".join(c if c.isalnum() else "_" for c in name)
	file_path = os.path.join(folder, f"{safe_name}.jpg")

	# Ensure the filename is unique
	counter = 1
	while os.path.exists(file_path):
		file_path = os.path.join(folder, f"{safe_name}_{counter}.jpg")
		counter += 1

	# Download and save the image
	response = requests.get(file_url)
	if response.status_code == 200:
		with open(file_path, "wb") as f:
			f.write(response.content)

	return file_path


def export_to_csv_with_images(db_id, csv_file, filter=None, folder=None):
	rows = []
	notion_data = query_database_with_filter(db_id, filter=filter)

	for page in notion_data:
		row = {}
		properties = page.get("properties", {})
		props = ["ENG", "TRM", "RUS", "IMG", "POS", "SYN", "MEM", "RMK"]
		term = ""
		if ("TRM" in properties):
			term = properties.get("TRM", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "image")
		for key in props:
			value = properties.get(key)
			if value:
				if value["type"] == "title":
					row[key] = value["title"][0]["plain_text"] if value["title"] else ""
				elif value["type"] == "rich_text":
					row[key] = value["rich_text"][0]["plain_text"] if value["rich_text"] else ""
				elif value["type"] == "multi_select":
					row[key] = ", ".join([option["name"] for option in value["multi_select"]])
				elif value["type"] == "files":
					if value["files"]:
						file_url = value["files"][0].get("file", {}).get("url")
						if file_url:
							local_image = download_image(file_url, folder, term)
							row[key] = local_image
						else:
							row[key] = ""
				else:
					row[key] = str(value.get(value["type"], ""))
			else:
				row[key] = ""  # Fallback for missing properties

		rows.append(row)

	# Export rows to CSV
	if rows:
		with open(csv_file, "w", newline="", encoding="utf-8") as f:
			writer = csv.DictWriter(f, fieldnames=rows[0].keys())
			writer.writeheader()
			writer.writerows(rows)

def download_all(notion_objects, filter=None, folders=None):
	with tqdm(total=len(notion_objects), desc="Downloading", ncols=100) as pbar:
		for name, db_id in notion_objects:
			csv_file = f"{name}_export.csv"
			print(f"Exporting {name} database to CSV...")
			folder = folders[notion_objects.index((name, db_id))]  # Get folder based on index
			export_to_csv_with_images(db_id, csv_file, filter=filter, folder=folder)
			print(f"CSV for {name} exported as {csv_file}")
			pbar.update(1)
	print(f"All databases processed.")

if __name__ == "__main__":
    notion_objects = [
        ("vcards", "database_id"),
        ("icards", "database_id"),
    ]

    filter = {
        "property": "STS",  # Property name
        "status": {
            "equals": "SYN"  # The value to filter by
        }
    }
    folders = ["vimages", "iimages"]
    download_all(notion_objects, filter=filter, folders=folders)
