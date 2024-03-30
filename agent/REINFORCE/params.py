import argparse

def get_args():
    parser = argparse.ArgumentParser(description="Arguments for Jun-Yi's Simulator REINFORCE algorithm")
    # args for normal setting
    parser.add_argument('--device', type=str, default="cuda:0")
    parser.add_argument('--exp_name', type=str, default="dummy")
    # parser.add_argument('--remove_similar_feat', action='store_true', default=False)
    # args for data
    parser.add_argument('--config_path', type=str, default="env/config.yml")
    parser.add_argument('--gap_path', type=str, default="../AUO_data/plan/auo_data")
    parser.add_argument('--job_dir', type=str, default="../AUO_data/wip_data_n+1_subroutine")
    # parser.add_argument('--job_dir', type=str, default="../AUO_data/wip_data")
    parser.add_argument('--booking_dir', type=str, default="../AUO_data/booking")
    parser.add_argument('--train_dates', type=str, default="train_dates/train.json")
    parser.add_argument('--eval_dates', type=str, default="train_dates/3_all.json")
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
    parser.add_argument('--w2', default=0.0, type=float)    # setup_time
    parser.add_argument('--w3', default=0.0, type=float)    # dps
    
    # compare dps setting
    parser.add_argument('--total_days', default=1, type=int)  # future_day_num
    args = parser.parse_args()
    return args
