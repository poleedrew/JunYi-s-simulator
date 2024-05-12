from collections import defaultdict
import os, json
task_path = "./train_dates/3_all.json"
with open(task_path, "r") as file:
    tasks = json.load(file)
data_root_dir = "../AUO_data/wip_data"

for item in tasks:
    start, task_name = item['start'], item['name']
    wait_wip_path = os.path.join(data_root_dir, task_name, 'odf_wait_wip.json')
    run_wip_path = os.path.join(data_root_dir, task_name, 'odf_run_wip.json')
    with open(wait_wip_path, 'r') as fp:
        wait_wip_info_list = json.load(fp)
    with open(run_wip_path, 'r') as fp:
        run_wip_info_list = json.load(fp)
    date_model_comp = defaultdict(dict)
    for wip_info in wait_wip_info_list:
        if wip_info['original_stage_id'] in ('PA', 'ODF'):
            if wip_info['product_code'] not in date_model_comp[wip_info['original_stage_id']].keys():
                date_model_comp[wip_info['original_stage_id']][wip_info['product_code']] = wip_info['size']
            else:
                date_model_comp[wip_info['original_stage_id']][wip_info['product_code']] += wip_info['size']
    
    for wip_info in run_wip_info_list:
        if wip_info['original_stage_id'] in ('PA', 'ODF'):
            if wip_info['product_code'] not in date_model_comp[wip_info['original_stage_id']].keys():
                date_model_comp[wip_info['original_stage_id']][wip_info['product_code']] = wip_info['size']    
            else:
                date_model_comp[wip_info['original_stage_id']][wip_info['product_code']] += wip_info['size']
    print(task_name)
    print(date_model_comp)        
