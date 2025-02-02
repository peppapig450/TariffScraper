import requests
from bs4 import BeautifulSoup, Tag

import pandas as pd
import json

url = "https://www.canada.ca/en/department-finance/news/2025/02/list-of-products-from-the-united-states-subject-to-25-per-cent-tariffs-effective-february-4-2025.html"
response = requests.get(url)
if not response.ok:
    raise Exception(f"Failed to fetch the webpage: {response.status_code}")
html_content = response.text

# Parse the HTML
soup = BeautifulSoup(html_content, 'html.parser')

# Initialize dictionary to store our grouped data
grouped_data = {}

# Extract data from the table rows
if table := soup.find('table'):
    if tbody := table.find('body'):
        if isinstance(tbody, Tag):
            for row in tbody.find_all('tr'):
                # Get the tariff item
                tariff_item = row.find('th').get_text(strip=True)
                
                # Get the HS heading and Indicative description
                cells = row.find_all('td')
                hs_heading = cells[0].get_text(" ", strip=True)
                
                # Extract the description (handle lists in <ul>)
                description_cell = cells[1]
                list_items = description_cell.find_all('li')
                if list_items:
                    description = "; ".join(li.get_text(" ", strip=True) for li in list_items)
                else:
                    description = description_cell.get_text(" ", strip=True)
            
                # Group headings under the same HS Heading
                if hs_heading not in grouped_data:
                    grouped_data[hs_heading] = {"Tariff Items": [], "Descriptions": []}
                
                grouped_data[hs_heading]["Tariff Items"].append(tariff_item)
                grouped_data[hs_heading]["Descriptions"].append(description)

# Convert grouped data to  JSON-like structure
output_list = []
for hs_heading, data in grouped_data.items():
    output_list.append({
        "HS Heading": hs_heading,
        "Tariff Items": data["Tariff Items"],
        "Descriptions": data["Descriptions"]
    })
    
output_file = "grouped_tariff_data.json"
with open(output_file, "w") as f:
    json.dump(output_list, f, indent=2)

print(f"Data successfully saved to {output_file}")