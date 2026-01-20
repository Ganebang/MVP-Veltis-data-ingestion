import json
from pathlib import Path

notebook_path = Path("/workspaces/MVP-web-scrapping-project/notebooks/exploration_veltis_data.ipynb")

with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

# Iterate through cells to find the code cell setting base_path
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        source = cell['source']
        new_source = []
        modified = False
        for line in source:
            if "data/raw/2023" in line:
                new_line = line.replace("2023", "2024")
                new_source.append(new_line)
                modified = True
            else:
                new_source.append(line)
        
        if modified:
            cell['source'] = new_source
            print("Updated base_path to 2024")
            
    # Also update Markdown title
    if cell['cell_type'] == 'markdown':
        source = cell['source']
        new_source = []
        for line in source:
            if "Exploration (2023)" in line:
                new_source.append(line.replace("2023", "2024"))
            else:
                new_source.append(line)
        cell['source'] = new_source

with open(notebook_path, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=4)
print("Notebook updated successfully.")
