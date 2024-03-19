import json, os, argparse
from pprint import pprint
from datetime import datetime, timedelta
from dateutil.parser import parse
from itertools import permutations
from collections import defaultdict
from agent.Rule.rule_agent import RuleAgent
from env.simulator import AUO_Simulator
from env.utils.utils import datetime2str, str2datetime
from kpi import KPI

DIGIT = 2

AVAILABLE = 0
PROCESSED = 1
SETUP = 2
BOOKING = 3

def test_rule(start, task_name, rule_name):
    tic = datetime.now()
    system_start_time_dt = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    with open(os.path.join(plan_dir, task_name, "gap.json")) as fp:
        gap = json.load(fp)
    agent = RuleAgent()
    agent.load_rules(gap, tact_time_table_path)

    simulator = AUO_Simulator(system_start_time_dt, args=args)
    simulator.load_wips(
        wait_wip_path=os.path.join(wip_data_dir, task_name, 'odf_wait_wip.json'),
        run_wip_path=os.path.join(wip_data_dir, task_name, 'odf_run_wip.json'),
        sheet_path=os.path.join(wip_data_dir, task_name, 'odf_sheet.json'))
    simulator.reset()
    simulator.load_booking(os.path.join(booking_dir, task_name, 'booking.json'))
        
    simulator.load_plan(aout_path=os.path.join(plan_dir, "actual_output.json"),
                        eout_path=os.path.join(plan_dir, task_name, "expected_output.json"))
    simulator.put_run_wip2timeline()
    current_time_dt = simulator.instance.start_time_dt
    step = 1
    while True:
        wip_info, eqp_id = agent.apply_rule(
            rule_name,
            simulator.instance.current_wait_wip_info_list,
            simulator.instance.odf_machines,
            simulator.instance.current_time_dt)
        step += 1
        next_state, reward, done, info = simulator.step(wip_info, eqp_id, other_info={'rule': str(rule_name)})
        status = [machine.get_status(simulator.instance.current_time_dt) for machine in simulator.instance.odf_machines.values()]
        while AVAILABLE not in status and args.schedule_point:
            simulator.instance.current_time_dt += timedelta(seconds=1)
            status = [machine.get_status(simulator.instance.current_time_dt) for machine in simulator.instance.odf_machines.values()]
        if done:
            break      
    simulator.find_idle()
    toc = datetime.now()
    
    makespan = round(simulator.instance.get_makespan(), 2)
    model_abbr_count = len(simulator.instance.model_abbr_count.keys())
    job_count = sum(simulator.instance.model_abbr_count.values())
    sheet_num = simulator.instance.num_sheets
    total_over_qtime_sheet_count = simulator.instance.total_over_qtime_sheet_count
    total_min_tact_time_sheet_count = simulator.instance.total_min_tact_time_sheet_count
    total_setup_time = simulator.instance.total_setup_time
    rules_count = { str(rule_name): count for rule_name, count in agent.rules_count.items() }

    if isinstance(option_name, str):
        fn = heur_name + '_' + option_name
    elif isinstance(option_name, tuple):
        option_fn = str(option_name).replace(", ", "-")
        option_fn = option_fn.replace("'", '')
        fn = heur_name + '_' + option_fn[1:-1]
    for wip_info in simulator.instance.history:
        datetime2str(wip_info)
    if not os.path.exists(os.path.join(result_dir, task_name)):
        os.makedirs(os.path.join(result_dir, task_name))
    with open(os.path.join(result_dir, task_name, f"{fn}.json"), 'w') as f:
        json.dump(simulator.instance.history, f, indent=4)

    result_path = os.path.join(result_dir, task_name, f"{fn}.json")
    
    kpi_calc = KPI(result_path, args.future_day_num)
    kpi_calc.load_plan(plan_dir, task_name)

    settle_time_dt = parse(task_name) + timedelta(days=1, hours=7, minutes=30) + timedelta(hours=6)
    kpi_calc.add_completion(settle_time_dt)
    kpi_calc.calculate_actual_dps_rate()
    kpi_calc.calculate_gap()

    actual_dps_rate = kpi_calc.total_actual_dps_rate
    dps_0 = kpi_calc.dps_0
    # dps_1 = kpi_calc.dps_1
    # dps_2 = kpi_calc.dps_2

    if dps_0 > actual_dps_rate:
        # dates.append(task_name)
        win_count_list[str(rule_name)] += 1

    total_gap_0 = kpi_calc.total_gap_0
    # total_gap_1 = kpi_calc.total_gap_1
    # total_gap_2 = kpi_calc.total_gap_2
    input_kpi = kpi_calc.date_actual_output

    print(
        f"{task_name}\t"
        f"{makespan}\t"
        f"{model_abbr_count}\t"
        f"{job_count}\t"
        f"{sheet_num}\t"
        f"{total_over_qtime_sheet_count}\t"
        f"{total_min_tact_time_sheet_count}\t"
        f"{total_setup_time}\t"
        f"({kpi_calc.total_history_num[0]}+{input_kpi})/{kpi_calc.total_expected_num[0]}={actual_dps_rate:.3} "
        f"({kpi_calc.total_history_num[0]}+{kpi_calc.total_contribute_num[0]})/{kpi_calc.total_expected_num[0]}={dps_0:.3} "
        # f"({kpi_calc.total_history_num[1]}+{kpi_calc.total_contribute_num[1]})/{kpi_calc.total_expected_num[1]}={dps_1:.3} "
        # f"({kpi_calc.total_history_num[2]}+{kpi_calc.total_contribute_num[2]})/{kpi_calc.total_expected_num[2]}={dps_2:.3} "    
        f"{round((toc - tic).total_seconds(), 2)}\t"
        f"{agent.rules_count}\t"
        )
    ratio_list.append(dps_0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-d', '--device', default='cuda')
    parser.add_argument('--warmup', default=10000, type=int)
    parser.add_argument('--episode', default=100000, type=int)
    parser.add_argument('--capacity', default=10000, type=int)
    parser.add_argument('--batch_size', default=32, type=int)
    parser.add_argument('--lr', default=.01, type=float)
    parser.add_argument('--eps', default=0.1, type=float)
    parser.add_argument('--eps_decay', default=.995, type=float)
    parser.add_argument('--eps_min', default=.01, type=float)
    parser.add_argument('--gamma', default=1.0, type=float)
    parser.add_argument('--freq', default=4, type=int)
    parser.add_argument('--target_freq', default=1000, type=int)
    parser.add_argument('--w0', default=0, type=float)    # no use
    parser.add_argument('--w1', default=0, type=float)      # no use
    parser.add_argument('--w2', default=0, type=float)    # no use
    parser.add_argument('--w3', default=1.0, type=float)    # no use
    parser.add_argument('--c1', default=0.5, type=float)  # tardiness
    parser.add_argument('--c2', default=0.1, type=float)  # fix penalty
    parser.add_argument('--c3', default=0.5, type=float)  # max d day tardiness penalty
    parser.add_argument('--c4', default=0.1, type=float)  # cost for over production
    parser.add_argument('--c5', default=0.5, type=float)  # max d day over penalty  
    parser.add_argument('--k', default=5, type=float)     # max expire day  
    parser.add_argument('--m', default=4, type=float)     # max cost day
    parser.add_argument('--e1', default=0.5, type=float)  # exponential preproduction
    parser.add_argument('--e2', default=1, type=float)    # exponential tardiness 
    parser.add_argument('--seed', default=128, type=int)  # random seed
    parser.add_argument('--reverse', action='store_true')
    parser.add_argument('--exp', action='store_true')
    parser.add_argument('--grid_search', action='store_true')
    parser.add_argument('--double', action='store_true')
    parser.add_argument('--input_dir', default='.')
    parser.add_argument('--future_day_num', default=3)
    parser.add_argument('--delete_node', action='store_true', default=True)
    parser.add_argument('--method', default="DQN")
    parser.add_argument('--schedule_point', action='store_true', default=False)
    parser.add_argument('--behave_cloning', action='store_true', default=True)
    args = parser.parse_args()
    tasks = [
        {"name": "2023-03-01", "start": "2023-03-01 08:05:33"},
        {"name": "2023-03-02", "start": "2023-03-02 08:05:34"},
        {"name": "2023-03-03", "start": "2023-03-03 08:05:34"},
        {"name": "2023-03-04", "start": "2023-03-04 08:05:34"},
        {"name": "2023-03-05", "start": "2023-03-05 08:05:34"},
        {"name": "2023-03-06", "start": "2023-03-06 08:05:35"},
        {"name": "2023-03-07", "start": "2023-03-07 08:05:50"},
        {"name": "2023-03-08", "start": "2023-03-08 08:05:36"},
        {"name": "2023-03-09", "start": "2023-03-09 08:05:35"},
        {"name": "2023-03-10", "start": "2023-03-10 08:05:55"},
        {"name": "2023-03-11", "start": "2023-03-11 08:05:56"},
        {"name": "2023-03-12", "start": "2023-03-12 08:05:55"},
        {"name": "2023-03-13", "start": "2023-03-13 08:05:56"},
        {"name": "2023-03-14", "start": "2023-03-14 08:05:56"},
        {"name": "2023-03-15", "start": "2023-03-15 08:05:56"},
        {"name": "2023-03-16", "start": "2023-03-16 08:05:56"},
        {"name": "2023-03-17", "start": "2023-03-17 08:05:57"},
        {"name": "2023-03-18", "start": "2023-03-18 08:05:17"},
        {"name": "2023-03-19", "start": "2023-03-19 08:05:18"},
        {"name": "2023-03-20", "start": "2023-03-20 08:05:18"},
        {"name": "2023-03-21", "start": "2023-03-21 16:14:10"},
        {"name": "2023-03-22", "start": "2023-03-22 08:05:10"},
        {"name": "2023-03-23", "start": "2023-03-23 08:05:10"},
        {"name": "2023-03-24", "start": "2023-03-24 08:05:10"},
        {"name": "2023-03-25", "start": "2023-03-25 08:05:11"},
        {"name": "2023-03-26", "start": "2023-03-26 08:05:11"},
        {"name": "2023-03-27", "start": "2023-03-27 08:05:11"},
        {"name": "2023-03-28", "start": "2023-03-28 08:05:11"},
        {"name": "2023-03-29", "start": "2023-03-29 08:05:12"},
        {"name": "2023-03-30", "start": "2023-03-30 08:05:12"},
        {"name": "2023-03-31", "start": "2023-03-31 08:05:12"}
    ]
    # wip_data_dir = "wip_data_n+1_new"
    wip_data_dir = "wip_data"
    booking_dir = "booking"
    result_dir = "agent/Rule/result/"+wip_data_dir
    plan_dir = "plan/auo_data"
    wip_heurs = ['EDD' ,'EDD_v2','EMaxQ', 'EMinQ', 'FIFO', 'LPT', 'SPT', 'SPT_model_abbr_FIFO_WIP', 'FIFO_model_abbr_FIFO_WIP']
    # wip_heurs = ['FIFO']
    machine_option = ('setup_time', 'earliest','tact_time')
    tact_time_table_path = "tact_time/imputed/14_STD_TACTTIME_imputed.csv"
    # tie breaker rule
    win_count_list = defaultdict(int)
    
    for task in tasks:
        option_perm = permutations(machine_option)
        for i, option_name in enumerate(option_perm):
            assert i < 6
            total_count = 0
            dates = []
            for heur_name in wip_heurs:
                total_count += 1
                start, task_name = task['start'], task['name']
                ratio_list = []
                print(start)
                test_rule(start, task_name, (heur_name, option_name))
                
            print('rule_compete_test:')
            pprint(win_count_list)
            print('total count', total_count)
            print('win_dates',dates)
            print()
        # for heur_name in wip_heurs:
        #     start, task_name = task['start'], task['name']
        #     ratio_list = []
        #     option_name = machine_option
        #     test_rule(start, task_name, (heur_name, option_name))

