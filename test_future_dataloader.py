import json
import os
from datetime import datetime
from env.data_loader import AUO_Dataloader
from env.future_loader import AUO_Future_loader

def test_load_future_wip(system_start_time, task_name, future_day_num, export=True):
    system_start_time_dt = datetime.strptime(system_start_time, '%Y-%m-%d %H:%M:%S')
    dataloader = AUO_Future_loader(data_root_dir, system_start_time_dt, future_day_num)
    if export:
        export_path = os.path.join(data_root_dir, task_name, f"FUTURE_{future_day_num}_DAYS.csv")
        dataloader.export(export_path)
        future_wip_path = os.path.join(data_root_dir, task_name, f"FUTURE_{future_day_num}_EXPECTED_PRODUCT.csv")
        dataloader.export_future_table(future_wip_path)

def test_load_data(system_start_time, wip_data_dir, task_name, export=False):
    system_start_time_dt = datetime.strptime(system_start_time, '%Y-%m-%d %H:%M:%S')
    date = datetime.strftime(system_start_time_dt, '%Y-%m-%d')
    dataloader = AUO_Dataloader(system_start_time_dt)
    dataloader.load_AUO_data(
        wip_table_path=wip_table_path,
        eqp_group_table_path=eqp_group_table_path,
        process_flow_table_path=process_flow_table_path,
        op2eqp_table_path=op2eqp_table_path,
        qtime_table_path=qtime_table_path,
        pc2model_table_path=pc2model_table_path,
        op_history_path=op_history_path,
        tact_time_table_path=tact_time_table_path,
        future_table_path=future_table_path,
        subroutine_table_path=subroutine_table_path
        )

    print(f"{date}\t"
          f"{len(dataloader.current_wait_wip_info_list)}\t"
          f"{len(dataloader.current_run_wip_info_list)}\t"
          f"{len(dataloader.sheet_list)}\t")
    if export:
        export_dir = os.path.join(wip_data_dir, task_name)
        if not os.path.exists(export_dir):
            os.mkdir(export_dir)
        with open(os.path.join(export_dir, 'odf_sheet.json'), 'w') as f:
            json.dump(dataloader.sheet_info_list, f, indent=4)
        with open(os.path.join(export_dir, 'odf_wip.json'), 'w') as f:
            json.dump(dataloader.wip_info_list, f, indent=4)
        with open(os.path.join(export_dir, 'odf_wait_wip.json'), 'w') as f:
            json.dump(dataloader.current_wait_wip_info_list, f, indent=4)
        with open(os.path.join(export_dir, 'odf_run_wip.json'), 'w') as f:
            json.dump(dataloader.current_run_wip_info_list, f, indent=4)
        with open(os.path.join(export_dir, 'tact_time_imputed_table_dict.json'), 'w') as f:
            json.dump(dataloader.tact_time_imputed_table_dict, f, indent=4)
        with open(os.path.join(export_dir, 'tact_time_table_dict.json'), 'w') as f:
            json.dump(dataloader.tact_time_table_dict, f, indent=4)
        total_sheet_nums = sum(dataloader.tact_time_imputed_table_dict.values()) + sum(dataloader.tact_time_table_dict.values())
        tact_time_imputed_table_ratio = sum(dataloader.tact_time_imputed_table_dict.values()) / total_sheet_nums
        tact_time_table_ratio = 1 - tact_time_imputed_table_ratio
        with open(os.path.join(export_dir, 'tact_time_ratio.json'), 'w') as f:
            json.dump({"tact_time_imputed_table_ratio": tact_time_imputed_table_ratio,
                       "tact_time_table_ratio": tact_time_table_ratio}, f, indent=4)


if __name__ == '__main__':
    task_path = "./train_dates/all.json"
    with open(task_path, "r") as file:
        tasks = json.load(file)

    data_root_dir = "../AUO_data"
    future_day_num = 1
    wip_data_dir = os.path.join(data_root_dir,  "wip_data")
    if future_day_num != 0:
        wip_data_dir =  os.path.join(data_root_dir,  f"wip_data_n+{future_day_num}_subroutine")
    
    if not os.path.exists(wip_data_dir):
        os.makedirs(wip_data_dir)
    
    for item in tasks:
        start, task_name = item['start'], item['name']
        wip_table_path =            os.path.join(data_root_dir, task_name, "1_WIP.csv")
        eqp_group_table_path =      os.path.join(data_root_dir, task_name, "2_EQP_GROUP.csv")
        process_flow_table_path =   os.path.join(data_root_dir, task_name, "4_PROCESS_FLOW.csv")
        op2eqp_table_path =         os.path.join(data_root_dir, task_name, "5_OPTOEQP.csv")
        qtime_table_path =          os.path.join(data_root_dir, task_name, "6_QTIME.csv")
        pc2model_table_path =       os.path.join(data_root_dir, task_name, "8_PRODUCT_TO_MODEL.csv")
        input_target_table_path =   os.path.join(data_root_dir, task_name, "11_INPUT_TARGET.csv")
        op_history_path =           os.path.join(data_root_dir, task_name, "13_FEOL_CHIP_ODS.csv")
        subroutine_table_path =     os.path.join(data_root_dir, "sub_routine/18_op_seq_back_mapping.csv")
        tact_time_table_path =      os.path.join(data_root_dir, "tact_time/imputed/14_STD_TACTTIME_imputed_update.csv")

        test_load_future_wip(start, task_name, future_day_num, export=True)
        future_table_path = os.path.join(data_root_dir, task_name, f"FUTURE_{future_day_num}_DAYS.csv")
        if os.path.exists(future_table_path) == False:
            future_table_path = None
        
        test_load_data(start, wip_data_dir, task_name, export=True)
        