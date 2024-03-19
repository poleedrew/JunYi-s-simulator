import json, os, copy, random, torch, time
from pprint import pprint
from torch.utils.tensorboard import SummaryWriter
from agent.Baseline_Rollout.params import get_args
from agent.Baseline_Rollout.model.network import Network
from agent.Baseline_Rollout.heuristic import *
from env.simulator import AUO_Simulator
from env.utils.utils import datetime2str
from djsp_plotter import DJSP_Plotter
from djsp_logger import DJSP_Logger
from kpi import KPI
from datetime import timedelta, datetime
from dateutil.parser import parse
import numpy as np

AVAILABLE = 0
PROCESSED = 1
SETUP = 2
BOOKING = 3
def setup_seed(seed):
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    np.random.seed(seed)
    random.seed(seed)
    torch.backends.cudnn.deterministic = True
    print(torch.cuda.initial_seed())

def eval(iter, eval_task, win_count):
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
    avai_ops = simulator.get_avai_ops()
    total_reward, step = 0, 0
    action_probs = []
    avai_ops = simulator.get_avai_ops()
    
    while True:
        avai_ops = simulator.get_avai_ops()
        graph = simulator.get_state()
        with torch.no_grad():
            action_idx, action_prob, log_prob, entropy = net(avai_ops, graph, greedy=True)
            action_probs.append(action_prob)
        job_id, m_id = avai_ops[action_idx]
        avai_ops, done, reward = simulator.step(job_id, m_id)
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
    total_contribute = simulator.instance.get_contribute()

    history = copy.deepcopy(simulator.instance.history)
    for wip_info in history:
        datetime2str(wip_info)
    result_path = os.path.join(result_dir, f"{eval_date}_iter{iter}.json")
    with open(result_path, 'w') as f:
        json.dump(history, f, indent=4)
    
    logger = DJSP_Logger()
    logger.load(result_path)
    plotter = DJSP_Plotter(logger, True)

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
    print(
        f"Iter {iter}\t"
        f"{eval_date}\t"
        f"{makespan}\t"
        f"{total_over_qtime_sheet_count}\t"
        f"{round(total_min_tact_time_sheet_count)}\t"
        f"{total_setup_time}\t"
    )
    print(
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
    if input_kpi < kpi_calc.total_contribute_num[0]:
        win_count += 1
    writer.add_scalar(f'{eval_date}/Makespan', makespan, iter)
    writer.add_scalar(f'{eval_date}/Total Over Q-Time Sheet Count', total_over_qtime_sheet_count, iter)
    writer.add_scalar(f'{eval_date}/Total Min Tact Time Sheet Count', total_min_tact_time_sheet_count, iter)
    writer.add_scalar(f'{eval_date}/Total Setup Time', total_setup_time, iter)
    writer.add_scalar(f'{eval_date}/Total Contribute', total_contribute, iter)
    writer.add_scalar(f'{eval_date}/Outcome', simulator.instance.outcome(), iter)
    writer.add_scalar(f'{eval_date}/Max action_prob', max(action_probs), iter)
    writer.add_scalar(f'{eval_date}/Actual-DPS', actual_dps_rate, iter)
    writer.add_scalar(f'{eval_date}/DPS-0', dps_0, iter)
    writer.add_scalar(f'{eval_date}/DPS-1', dps_1, iter)
    writer.add_scalar(f'{eval_date}/DPS-2', dps_2, iter)
    # writer.add_scalar(f'{eval_date}/Gap-0', total_gap_0, iter)
    # writer.add_scalar(f'{eval_date}/Gap-1', total_gap_1, iter)
    # writer.add_scalar(f'{eval_date}/Gap-2', total_gap_2, iter)
    # writer.add_scalar(f'{eval_date}/Availability', availability, iter)
    # writer.add_scalar(f'{eval_date}/Utilization', utilization, iter)
    # writer.add_scalar(f'{eval_date}/Speedup', speedup, iter)
    # writer.add_scalar('KPI/Availability', availability, iter)
    # writer.add_scalar('KPI/Utilization', utilization, iter)
    # writer.add_scalar('KPI/Speedup', speedup, iter)
    return win_count
# @profile
def play():
    for episode in range(1, args.episode):
        t1 = time.time()
        baselines, rewards, log_probs, entropies, action_probs = [], [], [], [], []
        print(f"Episode {episode}")
        train_task = random.choice(train_tasks)
        train_start_dt = datetime.strptime(train_task['start'], '%Y-%m-%d %H:%M:%S')
        train_date = train_task['name']
        
        simulator = AUO_Simulator(train_start_dt, args=args)
        simulator.load_wips(
            wait_wip_path=os.path.join(wip_data_dir, train_date, 'odf_wait_wip.json'),
            run_wip_path=os.path.join(wip_data_dir, train_date, 'odf_run_wip.json'))
        state = simulator.reset()
        simulator.load_booking(os.path.join(booking_dir, train_date, 'booking.json'))
        simulator.put_run_wip2timeline()

        simulator.load_plan(aout_path=os.path.join(plan_dir, "actual_output.json"),
                            eout_path=os.path.join(plan_dir, train_date, "expected_output.json"))
        with open(os.path.join(plan_dir, train_date, "gap.json")) as fp:
            simulator.gap = json.load(fp)
        avai_ops = simulator.get_avai_ops()
       
        loss = 0.0
        policy_loss = 0.0
        entropy_loss = 0.0
        rollout_env = copy.deepcopy(simulator)
        rollout_env, heuristic = heuristic_outcome(rollout_env, baseline_rule)
        step = 1
        status = [machine.get_status(simulator.instance.current_time_dt) for machine in simulator.instance.odf_machines.values()]
            # _, heuristic = heuristic_outcome(simulator, baseline_rule)
        while True:
            avai_ops = simulator.get_avai_ops()  
            baseline = heuristic - simulator.instance.outcome()
            graph = simulator.get_state()
            action_idx, action_prob, log_prob, entropy = net(avai_ops, graph)
            job_id, m_id = avai_ops[action_idx]
            avai_ops, done, reward = simulator.step(job_id, m_id)
            baselines.append(baseline)
            log_probs.append(log_prob)
            entropies.append(entropy)
            action_probs.append(action_prob)
            rewards.append(reward)
            step+=1
            status = [machine.get_status(simulator.instance.current_time_dt) for machine in simulator.instance.odf_machines.values()]
            if AVAILABLE not in status and args.schedule_point:
                simulator.instance.current_time_dt += timedelta(seconds=1)
            if done:
                break
        loss, policy_loss, entropy_loss = net.update(episode, log_probs, entropies, baselines, rewards)
        rule_o, rule_m = rollout_env.instance.total_over_qtime_sheet_count, rollout_env.instance.total_min_tact_time_sheet_count
        rule_s, rule_c = rollout_env.instance.total_setup_time, rollout_env.instance.get_contribute()

        policy_o, policy_m = simulator.instance.total_over_qtime_sheet_count, simulator.instance.total_min_tact_time_sheet_count
        policy_s, policy_c = simulator.instance.total_setup_time, simulator.instance.get_contribute()
        print(
            f'Date: {train_date} \t'
            f'Sheet_num: {simulator.instance.num_sheets} \t'
            f'Wip_num: {len(simulator.instance.current_wait_wip_info_list)} \t'
            f'Improve Over_qtime: {rule_o - policy_o} \t'
            f'Improve Min_tact: {policy_m - rule_m} \t'
            f'Improve Setup_time: {rule_s - policy_s} \t'
            f'Imporve Contribute: {policy_c -rule_c} \t'
            f'Rule Over_qtime: {rule_o} \t'
            f'Rule Min_tact: {rule_m} \t'
            f'Rule Setup_time: {rule_s} \t'
            f'Rule Contribute: {rule_c} \t'
            f'Policy Over_qtime: {policy_o} \t'
            f'Policy Min_tact: {policy_m} \t'
            f'Policy Setup_time: {policy_s} \t' 
            f'Policy Contribute: {policy_c} \t'
        )
        t2 = time.time()
        print('time:', t2-t1)

        if episode % 5 == 0:
            win_count = 0
            net.save(os.path.join(weight_dir, f"iter{episode:05}"))
            # training
            writer.add_scalar("max action_prob", max(action_probs),episode)
            writer.add_scalar("outcome", simulator.instance.outcome(), episode)
            writer.add_scalar("baseline mean", sum(baselines)/len(baselines), episode)
            writer.add_scalar("Loss/loss", loss, episode)
            writer.add_scalar("Loss/policy loss", policy_loss, episode)
            writer.add_scalar("Loss/entropy loss", entropy_loss, episode)
            for eval_task in eval_tasks:
                win_count = eval(episode, eval_task, win_count)
            writer.add_scalar(f'Win_count', win_count, episode)
        episode += 1
        

if __name__ == "__main__":
    args = get_args()
    print(args)
    root_dir = args.root_dir
    wip_data_dir = args.wip_dir
    booking_dir = "booking"
    plan_dir = "plan/" + args.demo_dir
    baseline_rule = args.baseline_rule
    result_dir = os.path.join(root_dir, "result", args.demo_dir, args.name)
    weight_dir = os.path.join(root_dir, "weight", args.demo_dir, args.name)
    log_dir = os.path.join(root_dir, "log", args.demo_dir, args.name)
    args_dir = os.path.join(root_dir, "args", args.demo_dir, args.name)
    timeline_dir = os.path.join(root_dir, "timeline", "eval", args.weight_dir)

    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    if not os.path.exists(weight_dir):
        os.makedirs(weight_dir)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    if not os.path.exists(args_dir):
        os.makedirs(args_dir)
        
    print(weight_dir)
    print(log_dir)
    print(result_dir)
    print(args_dir)

    with open(os.path.join(args_dir, "args.json"), "w") as outfile:
        json.dump(vars(args), outfile, indent=4)

    writer = SummaryWriter(log_dir)
    train_tasks = None
    # training set
    train_dates = args.train_dates
    print("train dates:", train_dates)
    with open(train_dates) as file:
        train_tasks = json.load(file)

    eval_task = None
    # validate set
    valid_date = args.vaild_date
    print("valid date:", valid_date)
    with open(valid_date) as file:
        eval_tasks = json.load(file)
    
    setup_seed(args.seed)
    net = Network(args).to(args.device)
    # print(args.weight_dir)
    # net.load(args.weight_dir)
    play()
