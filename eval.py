import json
import os
import torch
from datetime import datetime, timedelta
from agent.REINFORCE.params import get_args
from env.simulator import AUO_Simulator
from agent.REINFORCE.model.REINFORCE import REINFORCE
from agent.REINFORCE.heuristic import *
from kpi import KPI
from dateutil.parser import parse
from collections import defaultdict

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

    simulator.load_gap(gap_path=os.path.join(plan_dir, eval_date, 'gap.json'))
    total_reward = 0
    avai_jobs = simulator.get_avai_jobs()
    while True:
        action = MTT(avai_jobs, simulator.instance)
        avai_jobs, reward, done = simulator.step(action['job_id'], action['m_id']) 
        total_reward += reward
        print(avai_jobs)
        if done:
            print(total_reward)
            if not os.path.exists("agent/REINFORCE/result/MTT/" + eval_task['name']):
                os.makedirs("agent/REINFORCE/result/MTT/" + eval_task['name'])
            simulator.write_result(os.path.join("agent/REINFORCE/result/MTT/", eval_task['name'], eval_task['name']+'.json'))
            break

def eval_policy(eval_task):
    tic = datetime.now()
    eval_date = eval_task['name']
    eval_start = datetime.strptime(eval_task['start'], '%Y-%m-%d %H:%M:%S').replace(second=0, microsecond=0)
    eval_end = datetime.strptime((eval_start + timedelta(hours=48)).date().strftime('%Y-%m-%d') + " 13:30:00", '%Y-%m-%d %H:%M:%S').replace(second=0, microsecond=0)
    simulator = AUO_Simulator(eval_start, eval_end, args=args)
    simulator.load_booking(os.path.join(args.booking_dir, eval_date, 'booking.json'))
    simulator.load_wips(
        wait_wip_path = os.path.join(args.job_dir, eval_date, 'odf_wait_wip.json'),
        run_wip_path = os.path.join(args.job_dir, eval_date, 'odf_run_wip.json')
    )
    simulator.load_plan(aout_path=os.path.join(plan_dir, "actual_output.json"),
                        eout_path=os.path.join(plan_dir, eval_date, "expected_output.json"))
    simulator.load_gap(gap_path=os.path.join(args.gap_path, eval_date, 'gap.json'))
    
    avai_jobs = simulator.get_avai_jobs()
    step = 1
    while True:
        data = simulator.get_graph_data()
        action, _, _ = policy(avai_jobs, data, step, simulator.instance, False)
        avai_jobs, _, done = simulator.step(action['job_id'], action['m_id']) 
        step += 1
        if done:
            toc = datetime.now()
            makespan = round(simulator.instance.get_makespan(), 2)
            total_over_qtime_sheet_count = simulator.instance.total_over_qtime_sheet_count
            total_min_tact_time_sheet_count = simulator.instance.total_min_tact_time_sheet_count
            total_setup_time = simulator.instance.total_setup_time
            simulator.find_idle()
            if not os.path.exists("agent/REINFORCE/result/" + weight_dir):
                os.makedirs("agent/REINFORCE/result/" + weight_dir)
            result_path = os.path.join("agent/REINFORCE/result/" + weight_dir + "/", eval_task['name'] +'.json')
            simulator.write_result(result_path)
            win, auo_dps, dps_0 = is_win(result_path, eval_date)
            if win:
                global win_days
                win_days += 1
            else:
                lose_dates.append(eval_date)
            # print('f{eval_date}, auo: f{auo_dps}, RL:f{dps_0}')
            kpi_calc = KPI(result_path, 1+args.total_days)
            kpi_calc.load_plan(plan_dir, eval_date)
            settle_time_dt = parse(eval_date) + timedelta(days=1, hours=7, minutes=30) + timedelta(hours=6)
            kpi_calc.add_completion(settle_time_dt)
            kpi_calc.calculate_actual_dps_rate()
            kpi_calc.calculate_gap()
            actual_dps_rate = kpi_calc.total_actual_dps_rate
            dps_0 = kpi_calc.dps_0
            dps_1 = kpi_calc.dps_1
            input_kpi = kpi_calc.date_actual_output
            print(
                f"{eval_date} "
                f"{makespan} "
                f"{total_over_qtime_sheet_count} "
                f"{total_min_tact_time_sheet_count} "
                f"{total_setup_time} "
                f"({kpi_calc.total_history_num[0]}+{input_kpi})/{kpi_calc.total_expected_num[0]}={actual_dps_rate:.3} "
                f"({kpi_calc.total_history_num[0]}+{kpi_calc.total_contribute_num[0]})/{kpi_calc.total_expected_num[0]}={dps_0:.3} "
                f"({kpi_calc.total_history_num[1]}+{kpi_calc.total_contribute_num[1]})/{kpi_calc.total_expected_num[1]}={dps_1:.3} "
                f"{round((toc - tic).total_seconds(), 2)} "
            )
            global total_over_qtime_sheet, total_min_tact_time_sheet, total_setup_time_hour, AUO_dps_avg, RL_dps_avg_0, RL_dps_avg_1
            total_over_qtime_sheet += total_over_qtime_sheet_count
            total_min_tact_time_sheet += total_min_tact_time_sheet_count
            total_setup_time_hour += total_setup_time
            AUO_dps_avg += actual_dps_rate
            RL_dps_avg_0 += dps_0
            RL_dps_avg_1 += dps_1
            # if dps_0 > actual_dps_rate:
            #     global win_days
            #     win_days += 1
            # else:
                # lose_dates.append(eval_date)
            policy.clear_memory()
            break

def is_win(result_path, eval_date):
    kpi_calc = KPI(result_path, 1+args.total_days)
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
    plan_dir = "../AUO_data/plan/auo_data"

    # with open(args.eval_dates, 'r') as file:
    #     eval_tasks = json.load(file)
    #     for eval_task in eval_tasks:
    #         eval_heuristic(eval_task)
    weight_dir = args.exp_name
    exp_name = weight_dir.split('/')[-2]
    history_args_dir = os.path.join("agent/REINFORCE/args", exp_name, 'args.json')
    with open(history_args_dir, 'r') as fp:
        history_args = json.load(fp)
    args.w0, args.w1, args.w2, args.w3 = history_args["w0"], history_args["w1"], history_args["w2"], history_args["w3"]
    args.job_dir = history_args["job_dir"]
    print(args)
    policy = REINFORCE(args).to(args.device)
    policy.load_state_dict(torch.load(weight_dir)["policy_net"])
    test_times = 10
    average_data = defaultdict(float)
    for i in range(test_times):
        print(f'test_{i}:')
        win_days = 0
        lose_dates = []
        total_over_qtime_sheet, total_min_tact_time_sheet, total_setup_time_hour = 0, 0, 0
        AUO_dps_avg, RL_dps_avg_0, RL_dps_avg_1 = 0, 0, 0
        with open(args.eval_dates, 'r') as file:
            eval_tasks = json.load(file)
            for eval_task in eval_tasks:
                eval_policy(eval_task)
        print("win_days total_over_qtime total_min_tact_time_sheet total_setup_time_hour")
        print(win_days, total_over_qtime_sheet, total_min_tact_time_sheet, total_setup_time_hour)
        print("AUO_average_dps End-to-end_average_dps_0 End-to-end_average_dps_1")
        print(AUO_dps_avg/len(eval_tasks), RL_dps_avg_0/len(eval_tasks), RL_dps_avg_1/len(eval_tasks))
        print(lose_dates)
        average_data["win_days"] += win_days / test_times
        average_data["total_over_qtime"] += total_over_qtime_sheet / test_times
        average_data["total_min_tact_time_sheet"] += total_min_tact_time_sheet / test_times
        average_data["total_setup_time_hour"] += total_setup_time_hour / test_times
        average_data["End-to-end_average_dps_0"] += RL_dps_avg_0/len(eval_tasks) / test_times
        average_data["End-to-end_average_dps_1"] += RL_dps_avg_1/len(eval_tasks) / test_times
        print()
    for v in list(average_data.keys()):
        print(v, end=' ')
    print()
    for v in list(average_data.values()):
        print(v, end=' ')