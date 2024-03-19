import argparse

def get_args():
    parser = argparse.ArgumentParser(description='Arguments for RL_GNN_JSP')
    # args for normal setting
    parser.add_argument('--device', type=str, default="cuda:0")
    # args for data
    parser.add_argument('--config_path', type=str, default="./env/config.yml")
    parser.add_argument('--gap_path', type=str, default="./plan/auo_data")
    parser.add_argument('--job_dir', type=str, default="wip_data")
    parser.add_argument('--booking_dir', type=str, default="booking")
    parser.add_argument('--train_dates', type=str, default="./train_dates/train.json")
    parser.add_argument('--eval_dates', type=str, default="./train_dates/3_all.json")
    # args for RL
    parser.add_argument('--round', type=int, default=100000000)
    parser.add_argument('--lr', type=float, default=1e-4)
    parser.add_argument('--step_size', type=float, default=100)
    parser.add_argument('--entropy_coef', type=float, default=5e-3)
    # args for policy network
    parser.add_argument('--policy_num_layers', type=int, default=3)
    parser.add_argument('--hidden_dim', type=int, default=128)
    # args for GNN
    parser.add_argument('--GNN_num_layers', type=int, default=2)
    # args for environment
    # weighted reward
    parser.add_argument('--w0', default=0.0, type=float)    # over_qtime
    parser.add_argument('--w1', default=0.0, type=float)    # select_min_tact
    parser.add_argument('--w2', default=0.5, type=float)    # setup_time
    parser.add_argument('--w3', default=0.5, type=float)    # dps
    # dps reward
    parser.add_argument('--c1', default=0.5, type=float)
    parser.add_argument('--c2', default=0.1, type=float)
    parser.add_argument('--c3', default=0.5, type=float)
    parser.add_argument('--c4', default=0.1, type=float)
    parser.add_argument('--c5', default=0.5, type=float)
    parser.add_argument('--k',  default=5, type=int)
    parser.add_argument('--m',  default=4, type=int)
    parser.add_argument('--e1', default=0.5, type=float)  # exponential preproduction
    parser.add_argument('--e2', default=1, type=float)    # exponential tardiness  
    # compare dps setting
    parser.add_argument('--exp', action='store_true', default=False)
    parser.add_argument('--delete_node', action='store_true', default=True)
    parser.add_argument('--future_day_num', default=3, type=int)  # future_day_num
    args = parser.parse_args()
    return args
