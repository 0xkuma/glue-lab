import pandas as pd
import json

input_file_name = './Mapping.xlsx'
input_book = pd.read_excel(input_file_name, sheet_name='Mapping')
# print(input_book['Row Labels'])
tags = {
    "KEHK": ["Active Directory"],
    "KerryESG": [],
    "KFHK": [],
    "KFMS": [],
    "KLHK": [],
    "KLUK": [],
    "KPHARMA": [],
    "KPHK": [],
    "KWHK": [],
    "KWMS": [],
    "KSIS": []
}

for label in range(0, len(input_book)):
    if input_book["Company"][label] in tags:
        tags[input_book["Company"][label]].append(
            input_book["Row Labels"][label].split("|")[1].lstrip())
print(json.dumps(tags))
with open('tags.json', 'w') as outfile:
    outfile.write(json.dumps(tags))

# Company
