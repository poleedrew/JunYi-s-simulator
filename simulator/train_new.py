import json
import os
import copy
import torch
import torch.optim as optim
from datetime import datetime, timedelta
from params import get_args
from env.simulator import AUO_Simulator
from model.REINFORCE import REINFORCE
from heuristic import *
from torch.utils.tensorboard import SummaryWriter
from kpi import KPI
from dateutil.parser import parse

seed = 1002
torch.manual_seed(seed)
torch.cuda.manual_seed(seed)
torch.cuda.manual_seed_all(seed)
np.random.seed(seed)
random.seed(seed)

def train(train_task, round, episode):
    train_date = train_task['name']
    train_start = datetime.strptime(train_task['start'], '%Y-%m-%d %H:%M:%S').replace(second=0, microsecond=0)
    train_end = datetime.strptime((train_start + timedelta(hours=24)).date().strftime('%Y-%m-%d') + " 13:30:00", '%Y-%m-%d %H:%M:%S').replace(second=0, microsecond=0)
    simulator = AUO_Simulator(train_start, train_end, args=args)
    
    simulator.load_booking(os.path.join(args.booking_dir, train_date, 'booking.json'))
    
    simulator.load_wips(
        wait_wip_path = os.path.join(args.job_dir, train_date, 'odf_wait_wip.json'),
        run_wip_path = os.path.join(args.job_dir, train_date, 'odf_run_wip.json')
    )
    
    simulator.load_plan(aout_path=os.path.join(plan_dir, "actual_output.json"),
                        eout_path=os.path.join(plan_dir, train_date, "expected_output.json"))
    # simulator.load_gap(gap_path=os.path.join(args.gap_path, train_date, 'gap.json'))
 
    avai_jobs = simulator.get_avai_jobs()
    MTT_reward = heuristic_makespan(copy.deepcopy(simulator), copy.deepcopy(avai_jobs), 'MTT')
    MCT_reward = heuristic_makespan(copy.deepcopy(simulator), copy.deepcopy(avai_jobs), 'MCT')
    total_reward = 0
    step = 1
    #print("##########################")
    while True:
        # baseline
        MTT_baseline = heuristic_makespan(copy.deepcopy(simulator), copy.deepcopy(avai_jobs), 'MTT')
        MCT_baseline = heuristic_makespan(copy.deepcopy(simulator), copy.deepcopy(avai_jobs), 'MCT')
        baseline = min(MTT_baseline, MCT_baseline)

        data = simulator.get_graph_data()
        action, log_prob, entropy = policy(avai_jobs, data, step, simulator.instance, False)
        avai_jobs, reward, done = simulator.step(action['job_id'], action['m_id'])
        total_reward += reward
        # policy record
        policy.log_probs.append(log_prob)
        policy.entropies.append(entropy)
        policy.rewards.append(reward)
        policy.baselines.append(baseline)
        step += 1
        if done:
            optimizer.zero_grad()
            loss = policy.calculate_loss(args.device)
            loss.backward()
            optimizer.step()
            scheduler.step()
            policy.clear_memory()
            improve = total_reward - min(MTT_reward, MCT_reward)
            improve_list.append(improve)
            return_list.append(reward)
            loss_list.append(loss)
            print("Round : {} \t\tEpisode : {} \t\tpolicy : {} \t\tImporve : {} \t\tMTT : {} \t\tMCT : {}".format(
                round, episode, total_reward, improve, MTT_reward, MCT_reward))
            if not os.path.exists("./tmp/result/" + exp_name):
                os.makedirs("./tmp/result/" + exp_name)
            result_path = os.path.join("./tmp/result/" + exp_name + "/", train_date +'.json')
            simulator.write_result(result_path)
            win, auo_dps, dps_0 = is_win(result_path, train_date)
            if win:
                win_list.append(train_date)
            print(train_date, "auo:", auo_dps, "RL:", dps_0)
            break
    
    # insert idle time
    #simulator.insert_idle_time()
    # show result
    #simulator.print_result()
    #simulator.write_result("./plotting/result/model.json")
    #raise NotImplementedError

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
    print(args)
    plan_dir = "plan/auo_data"
    exp_name = "0_setup+dps"
    policy = REINFORCE(args).to(args.device)
    optimizer = optim.Adam(policy.parameters(), lr=args.lr, betas=(0.9, 0.999))
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=args.step_size, gamma=0.99)
    writer = SummaryWriter("log/"+exp_name+"/")
    if not os.path.exists("./weight/" + exp_name):
        os.makedirs("./weight/" + exp_name)
    
    with open(args.train_dates, 'r') as file:
        train_tasks = json.load(file)
    
    for round in range(1, args.round + 1):
        improve_list = []
        return_list = []
        loss_list = []
        win_list = []
        episode = 1 
        for train_task in train_tasks:
            train(train_task, round, episode)
            episode += 1
        # save model weight
        torch.save(policy.state_dict(), './weight/'+ exp_name + '/round_' + str(round))
        
        
        writer.add_scalar("average improve", sum(improve_list) / len(improve_list), round)
        writer.add_scalar("average return", sum(return_list) / len(return_list), round)
        writer.add_scalar("average loss", sum(loss_list) / len(loss_list), round)
        writer.add_scalar("win_num", len(win_list), round)
            