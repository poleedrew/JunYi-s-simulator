import json, os, copy, random, torch
from datetime import datetime, timedelta
from agent.Baseline_Rollout.params import get_args
from agent.Baseline_Rollout.model.network import Network
from torch.utils.tensorboard import SummaryWriter
from env.simulator import AUO_Simulator
from env.utils.utils import datetime2str, str2datetime
from djsp_plotter import DJSP_Plotter
from djsp_logger import DJSP_Logger
from kpi import KPI
from dateutil.parser import parse

AVAILABLE = 0
PROCESSED = 1
SETUP = 2
BOOKING = 3

def setup_seed(seed):
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    random.seed(seed)
    torch.backends.cudnn.deterministic = True

def eval(eval_task):
    eval_start_dt = datetime.strptime(eval_task['start'], '%Y-%m-%d %H:%M:%S')
    eval_date = eval_task['name']
    
    simulator = AUO_Simulator(eval_start_dt, args=args)
    simulator.load_wips(
        wait_wip_path=os.path.join(wip_data_dir, eval_date, 'odf_wait_wip.json'),
        run_wip_path=os.path.join(wip_data_dir, eval_date, 'odf_run_wip.json'))
    state = simulator.reset()
    simulator.load_booking(os.path.join(booking_dir, eval_date, 'booking.json'))
    simulator.put_run_wip2timeline()
    
    simulator.load_plan(aout_path=os.path.join(plan_dir, "actual_output.json"),
                        eout_path=os.path.join(plan_dir, eval_date, "expected_output.json"))
    tic = datetime.now()
    current_time_dt = simulator.instance.current_time_dt 
    avai_ops = simulator.get_avai_ops(current_time_dt)
    total_reward, step = 0, 0
    action_probs, action_idxs, job_datas, machine_datas = [], [], [], []
    # debug_dir = f"debug/{eval_date}"
    while True:
        avai_ops = simulator.get_avai_ops(simulator.instance.current_time_dt)
        graph = simulator.get_graph_data()
        with torch.no_grad():
            # print('Order:',simulator.instance.order)
            # for eqp_id, m_info  in simulator.instance.odf_machines.items():
            #     print(eqp_id, m_info.current_time, m_info.last_wip()['product_code'] if m_info.last_wip() != None else None)
            action_idx, action_prob, log_prob, entropy = net(avai_ops, graph, greedy=True)
            action_idxs.append(action_idx)
            action_probs.append(action_prob)
            writer.add_scalar(f'{eval_date}/Action_probs', action_prob, step)
        job_id, m_id = avai_ops[action_idx]
        wip_info = simulator.instance.current_wait_wip_info_list[job_id]
        eqp_id = simulator.instance.eqp_id_list[m_id]
        avai_ops, done, reward = simulator.step(avai_ops, action_idx)
        total_reward += reward
        step+=1
        status = [machine.get_status(simulator.instance.current_time_dt) for machine in simulator.instance.odf_machines.values()]
        if AVAILABLE not in status and args.schedule_point:
            simulator.instance.current_time_dt += timedelta(seconds=1)
        if done:
            break
    toc = datetime.now()
    simulator.find_idle()
    makespan = round(simulator.instance.get_makespan(), 2)
    total_over_qtime_sheet_count = simulator.instance.total_over_qtime_sheet_count
    total_min_tact_time_sheet_count = round(simulator.instance.total_min_tact_time_sheet_count, 2)
    total_setup_time = simulator.instance.total_setup_time
    # total_contribute = simulator.instance.get_contribute()
    
    # dps related
    result_name = weight_path.split('/')[-2:]
    # print(result_name)
    result_name = result_name[0] + '_' + result_name[1] + '_' + eval_task['name'] + '.json'

    history = copy.deepcopy(simulator.instance.history)
    for wip_info in history:
        datetime2str(wip_info)
        
    result_path = os.path.join(result_dir, eval_task['name'], result_name)

    if not os.path.exists(result_dir + "/" + eval_task['name']):
        os.makedirs(result_dir + "/" + eval_task['name'])
    with open(result_path, 'w') as f:
        json.dump(history, f, indent=4)

    logger = DJSP_Logger()
    logger.load(result_path)
    plotter = DJSP_Plotter(logger)

    if not os.path.exists(os.path.join(timeline_dir)):
        os.makedirs(os.path.join(timeline_dir))
    timeline_path = os.path.join(timeline_dir, eval_task['name'] + '.html')
    # print(timeline_path)
    plotter.plot_googlechart_timeline(timeline_path)
        
    kpi_calc = KPI(result_path, args.future_day_num)
    kpi_calc.load_plan(plan_dir, eval_date)
    settle_time_dt = parse(eval_date) + timedelta(days=1, hours=7, minutes=30) + timedelta(hours=6)
    kpi_calc.add_completion(settle_time_dt)
    kpi_calc.calculate_actual_dps_rate()
    kpi_calc.calculate_gap()

    # 產能面 KPI
    availability = kpi_calc.availability()
    utilization = kpi_calc.utilization()
    speedup = kpi_calc.speedup()

    # 需求面 KPI
    actual_dps_rate = kpi_calc.total_actual_dps_rate
    dps_0 = kpi_calc.dps_0
    dps_1 = kpi_calc.dps_1
    dps_2 = kpi_calc.dps_2

    total_gap_0 = kpi_calc.total_gap_0
    total_gap_1 = kpi_calc.total_gap_1
    total_gap_2 = kpi_calc.total_gap_2

    input_kpi = kpi_calc.date_actual_output
    if input_kpi < kpi_calc.total_contribute_num[0]:
        global win_count
        win_count += 1
    # if dps_0 > actual_dps_rate:
    #     global win_count
    #     win_count += 1
    global total_count
    total_count += 1
    
    print(
        f"{eval_date}\t"
        f"{makespan}\t"
        f"{simulator.instance.num_sheets}\t"
        f"{total_over_qtime_sheet_count}\t"
        f"{round(total_min_tact_time_sheet_count)}\t"
        f"{total_setup_time}\t"
        # f"{total_contribute}\t"
        f"({kpi_calc.total_history_num[0]}+{input_kpi})/{kpi_calc.total_expected_num[0]}={actual_dps_rate:.3} "
        f"({kpi_calc.total_history_num[0]}+{kpi_calc.total_contribute_num[0]})/{kpi_calc.total_expected_num[0]}={dps_0:.3} "
        f"({kpi_calc.total_history_num[1]}+{kpi_calc.total_contribute_num[1]})/{kpi_calc.total_expected_num[1]}={dps_1:.3} "
        f"({kpi_calc.total_history_num[2]}+{kpi_calc.total_contribute_num[2]})/{kpi_calc.total_expected_num[2]}={dps_2:.3} "    
        f"{total_gap_0}\t"
        f"{total_gap_1}\t"
        f"{total_gap_2}\t"
        f"{availability:.3}\t"
        f"{utilization:.3}\t"
        f"{speedup:.3}\t"
        f"total reward: {total_reward}\t"
        f"{round((toc - tic).total_seconds(), 2)}\t"
        )

    

if __name__ == "__main__":
    args = get_args()
    print(args)
    root_dir = args.root_dir
    wip_data_dir = args.wip_dir
    booking_dir = "booking"
    plan_dir = "plan/" + args.demo_dir
    result_dir = os.path.join(root_dir, "result", "eval")    
    weight_path = os.path.join(root_dir, "weight", args.weight_dir)
    timeline_dir = os.path.join(root_dir, "timeline", args.weight_dir)
    log_dir = os.path.join(root_dir, "action_probs", args.weight_dir)
    print(root_dir)
    print(weight_path)
    print(timeline_dir)
    eval_tasks = args.eval_dates
    writer = SummaryWriter(log_dir)
    with open(args.eval_dates, 'r') as file:
        eval_tasks = json.load(file)
    # print(eval_tasks)
    
    setup_seed(args.seed)
    net = Network(args).to(args.device)
    net.load(weight_path)
    # net.eval()
    net.train()
    
    win_count = 0
    total_count = 0
    for eval_task in eval_tasks:
        eval(eval_task)
    print('win count:',win_count)
    print('total count:',total_count)
