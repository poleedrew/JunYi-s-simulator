import argparse

def get_args():
    parser = argparse.ArgumentParser(description='Arguments for RL_GNN_JSP')
    # args for normal setting
    parser.add_argument('--name', type=str, default='dummy')
    parser.add_argument('--device', type=str, default='cuda:0')
    parser.add_argument('--seed', type=int, default=128)
    # args data_dates
    parser.add_argument('--root_dir', type=str, default="agent/Baseline_Rollout")
    parser.add_argument('--wip_dir', type=str, default="wip_data")
    parser.add_argument('--train_dates', type=str, default="./train_dates/12_26.json")
    parser.add_argument('--vaild_date', type=str, default="./train_dates/12_26.json")
    parser.add_argument('--eval_dates', type=str, default="./train_dates/12_26.json")
    parser.add_argument('--weight_dir', type=str, default="")
    ## dummy
    parser.add_argument('--demo_dir', type=str, default="auo_data")
    # args for env
    parser.add_argument('--baseline_rule', type=str, default="Random")
    # args for RL
    parser.add_argument('--entropy_coef', type=float, default=0)
    parser.add_argument('--episode', type=int, default=100001)
    parser.add_argument('--lr', type=float, default=1e-4)
    # args for network
    parser.add_argument('--hidden_dim', type=int, default=256)
    parser.add_argument('--num_policy_layers', type=int, default=2)
    # args for GNN
    parser.add_argument('--GNN_num_layers', type=int, default=3)
    
    # weighted reward
    parser.add_argument('--w0', default=0.0, type=float)    # over_qtime
    parser.add_argument('--w1', default=0.0, type=float)    # select_min_tact
    parser.add_argument('--w2', default=0.0, type=float)    # setup_time
    parser.add_argument('--w3', default=0.0, type=float)    # dps
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
    parser.add_argument('--reverse', action='store_true', default=False)
    parser.add_argument('--exp', action='store_true', default=False)
    parser.add_argument('--delete_node', action='store_true', default=True)
    parser.add_argument('--future_day_num', default=3, type=int)  # future_day_num
    parser.add_argument('--schedule_point', action='store_true', default=True)
    parser.add_argument('--norm_capacity', action='store_true', default=True)
    parser.add_argument('--setup_feat', action='store_true', default=True)
    parser.add_argument('--model_feat', action='store_true', default=True)
    parser.add_argument('--size_feat', action='store_true', default=True)
    args = parser.parse_args()
    return args
