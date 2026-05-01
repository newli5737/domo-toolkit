import json

with open(r'd:\DOMO\domo_toolkit\dataflow_215.json', encoding='utf-8') as f:
    df = json.load(f)

# Find input datasets
for act in df.get('actions', []):
    if act.get('type') == 'LoadFromVault':
        ds = act.get('dataSource', {})
        name = ds.get('name', '?')
        guid = ds.get('guid', '?')
        rows = ds.get('numRows', '?')
        print(f"{name}")
        print(f"  ID: {guid}")
        print(f"  Rows: {rows}")
        print()
