import json
sheet_num = 0
with open('wip_data_n+1_new/2023-03-30/odf_wait_wip.json', 'r') as f:
    rows = json.load(f)
    for row in rows:
        if row['model_no'] == "MD0012":
            sheet_num += int(row['size'])
    print(sheet_num)

with open('wip_data_n+1_new/2023-03-30/odf_run_wip.json', 'r') as f:
    rows = json.load(f)
    for row in rows:
        if row['model_no'] == "MD0012":
            sheet_num += int(row['size'])
    print(sheet_num)

