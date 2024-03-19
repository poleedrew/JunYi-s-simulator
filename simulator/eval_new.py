import json
import os
import torch
from params import get_args
from datetime import datetime, timedelta
from heuristic import *
from env.simulator import AUO_Simulator
from model.REINFORCE import REINFORCE
from kpi import KPI
from dateutil.parser import parse

seed = 1002
torch.manual_seed(seed)
torch.cuda.manual_seed(seed)
torch.cuda.manual_seed_all(seed)
np.random.seed(seed)
random.seed(seed)

def eval_heuristic(eval_task):
    eval_date = eval_task['name']
    eval_start = datetime.strptime(eval_task['start'], '%Y-%m-%d %H:%M:%S').replace(second=0, microsecond=0)
    eval_end = datetime.strptime((eval_start + timedelta(hours=24)).date().strftime('%Y-%m-%d') + " 13:30:00", '%Y-%m-%d %H:%M:%S').replace(second=0, microsecond=0)
    simulator = AUO_Simulator(eval_start, eval_end, args=args)
    simulator.load_booking(os.path.join(args.booking_dir, eval_date, 'booking.json'))
    simulator.load_wips(
        wait_wip_path = os.path.join(args.job_dir, eval_date, 'odf_wait_wip.json'),
        run_wip_path = os.path.join(args.job_dir, eval_date, 'odf_run_wip.json')
    )
    simulator.load_plan(aout_path=os.path.join(plan_dir, "actual_output.json"),
                        eout_path=os.path.join(plan_dir, eval_date, "expected_output.json"))

    # simulator.load_gap(gap_path=os.path.join(args.gap_path, eval_date, 'gap.json'))
    
    avai_jobs = simulator.get_avai_jobs()
    while True:
        action = MTT(avai_jobs, simulator.instance)
        avai_jobs, _, done = simulator.step(action['job_id'], action['m_id']) 
        if done:
            if not os.path.exists("./result/MTT/" + eval_task['name']):
                os.makedirs("./result/MTT/" + eval_task['name'])
            simulator.write_result(os.path.join("./result/MTT/", eval_task['name'], eval_task['name']+'.json'))
            break

def eval_policy(eval_task):
    eval_date = eval_task['name']
    eval_start = datetime.strptime(eval_task['start'], '%Y-%m-%d %H:%M:%S').replace(second=0, microsecond=0)
    eval_end = datetime.strptime((eval_start + timedelta(hours=24)).date().strftime('%Y-%m-%d') + " 13:30:00", '%Y-%m-%d %H:%M:%S').replace(second=0, microsecond=0)
    simulator = AUO_Simulator(eval_start, eval_end, args=args)
    simulator.load_booking(os.path.join(args.booking_dir, eval_date, 'booking.json'))
    simulator.load_wips(
        wait_wip_path = os.path.join(args.job_dir, eval_date, 'odf_wait_wip.json'),
        run_wip_path = os.path.join(args.job_dir, eval_date, 'odf_run_wip.json')
    )
    simulator.load_plan(aout_path=os.path.join(plan_dir, "actual_output.json"),
                        eout_path=os.path.join(plan_dir, eval_date, "expected_output.json"))

    # simulator.load_gap(gap_path=os.path.join(args.gap_path, eval_date, 'gap.json'))
    
    avai_jobs = simulator.get_avai_jobs()
    step = 1
    while True:
        data = simulator.get_graph_data()
        action, _, _ = policy(avai_jobs, data, step, simulator.instance, True)
        avai_jobs, _, done = simulator.step(action['job_id'], action['m_id']) 
        step += 1
        if done:
            simulator.find_idle()
            if not os.path.exists("./result/" + exp_name):
                os.makedirs("./result/" + exp_name)
            result_path = os.path.join("./result/" + exp_name + "/", eval_task['name'] +'.json')
            simulator.write_result(result_path)
            win, auo_dps, dps_0 = is_win(result_path, eval_date)
            if win:
                global win_days
                win_days += 1
            print(eval_date, "auo:", auo_dps, "RL:", dps_0)
            break

def is_win(result_path, eval_date):
    kpi_calc = KPI(result_path, args.future_day_num)
    kpi_calc.load_plan(plan_dir, eval_date)
    settle_time_dt = parse(eval_date) + timedelta(days=1, hours=7, minutes=30) + timedelta(hours=6)
    kpi_calc.add_completion(settle_time_dt)
    kpi_calc.calculate_actual_dps_rate()
    kpi_calc.calculate_gap()

    # 需求面 KPI
    actual_dps_rate = kpi_calc.total_actual_dps_rate
    dps_0 = kpi_calc.dps_0

    input_kpi = kpi_calc.date_actual_output
    if input_kpi < kpi_calc.total_contribute_num[0]:
        return True, actual_dps_rate, dps_0
    return False, actual_dps_rate, dps_0
        
if __name__ == "__main__":
    args = get_args()
    plan_dir = "plan/auo_data"
    exp_name = "dps/round_10"
    weight_dir = './weight/' + exp_name
    print(args)
    policy = REINFORCE(args).to(args.device)
    policy.load_state_dict(torch.load(weight_dir))
    win_days = 0
    with open(args.eval_dates, 'r') as file:
        eval_tasks = json.load(file)
        for eval_task in eval_tasks:
            eval_policy(eval_task)
            #eval_heuristic(eval_task)
    print(win_days)