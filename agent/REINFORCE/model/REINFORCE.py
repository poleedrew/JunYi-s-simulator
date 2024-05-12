import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import Linear
from torch.distributions import Categorical
from itertools import accumulate
from agent.REINFORCE.model.gnn import GNN
from datetime import timedelta
from collections import defaultdict
import numpy as np
torch.set_printoptions(precision=4)

class REINFORCE(nn.Module):
    def __init__(self, args):
        super(REINFORCE, self).__init__()
        self.args = args
        self.num_layers = args.policy_num_layers
        self.hidden_dim = args.hidden_dim
        self.gnn = GNN(args)
        self.layers = torch.nn.ModuleList()
        self.layers.append(nn.Linear(self.hidden_dim * 2, self.hidden_dim))
        for _ in range(self.num_layers - 2):
            self.layers.append(nn.Linear(self.hidden_dim, self.hidden_dim))
        self.layers.append(nn.Linear(self.hidden_dim, 1))
        self.reset_parameters()
        
        self.log_probs = []
        self.entropies = []
        self.rewards = []
        self.baselines = []
        self.probs = []
        self.idx = []
        
    def reset_parameters(self):
        for m in self.modules():
            if isinstance(m, Linear):
                nn.init.kaiming_normal_(m.weight, mode='fan_out', nonlinearity='relu')
        
    def forward(self, avai_jobs, data, step, instance, greedy=False):
        x_dict = self.gnn(data, step)
        # available machine
        avai_m = [i for i in range(len(avai_jobs)) if len(avai_jobs[i]) > 0]
        # print(avai_jobs)
        # print(avai_m)
        # divide jobs apart according to setup time
        zero_setup_jobs = []
        short_setup_jobs = []
        long_setup_jobs = []
        # for machine_id in avai_m:
        #     # print(machine_id)
            # for info in avai_jobs[avai_m[machine_id]]:
        for info in avai_jobs[avai_m[0]]:
            job_info = instance.wait_wip_info_list[info['job_id']]
            setup_time = instance.odf_machines[info['m_id']].setup_time_handler(job_info, instance.current_time)
            if setup_time == timedelta(hours=0):
                zero_setup_jobs.append(info)
            elif setup_time == timedelta(hours=0.5):
                short_setup_jobs.append(info)
            else:
                long_setup_jobs.append(info)
        if len(zero_setup_jobs) > 0:
            candidated_jobs = zero_setup_jobs
        elif len(short_setup_jobs) > 0:
            candidated_jobs = short_setup_jobs
        else:
            candidated_jobs = long_setup_jobs    
        
        # choose job for candidated jobs
        job_scores = []
        score_idx = defaultdict(list)
        machine_prob = defaultdict(list)
        for id, info in enumerate(candidated_jobs):
        #for info in avai_jobs[avai_m[0]]:
            m_id = instance.eqp_id2id[info['m_id']]
            score = torch.cat((x_dict['m'][m_id], x_dict['job'][info['job_id']]), dim=0)
            for i in range(self.num_layers - 1):
                score = torch.relu(self.layers[i](score))
            score = self.layers[self.num_layers - 1](score)
            score_idx[info['m_id']].append(id)
            job_scores.append(score)
        probs = F.softmax(torch.stack(job_scores), dim=0).flatten()
        for eqp_id, id in score_idx.items():
            machine_prob[eqp_id].append(probs[id])
        # print(machine_prob)
        # print("xxxxxx")
        dist = Categorical(probs)
        if greedy == True:
            idx = torch.argmax(probs)
        else:
            idx = dist.sample()
        self.probs.append(probs)
        self.idx.append(idx.item())
        return candidated_jobs[idx.item()], dist.log_prob(idx), dist.entropy()
        #return avai_jobs[avai_m[0]][idx.item()], dist.log_prob(idx), dist.entropy()
    
    def calculate_loss(self, device):
        loss = []
        returns = torch.FloatTensor(list(accumulate(self.rewards[::-1]))[::-1]).to(device)
        for log_prob, entropy, R, baseline, probs, idx in zip(self.log_probs, self.entropies, returns, self.baselines, self.probs, self.idx):
            if baseline == 0:
                # print(f"R:{R}, baseline: {baseline}")
                advantage = R
            else:
                # print(f"R:{R}, baseline: {baseline}")
                advantage = (R - baseline) / baseline
            # sign = torch.sign(R-baseline) 
            # advantage = sign * abs(advantage)
            advantage = R - baseline # no normalized method
            # print(f"advantage: {advantage}")
            # print(f"log_prob: {log_prob.item()}, advantage: {advantage.item()}, entropy_coef: {self.args.entropy_coef}, entropy {entropy.item()}")
            loss.append(-log_prob * advantage - self.args.entropy_coef * entropy)
            # print(f"loss: {loss[-1].item()}")
            #loss.append(-log_prob * advantage)
        return torch.stack(loss).mean()
    
    def clear_memory(self):
        del self.log_probs[:]
        del self.entropies[:]
        del self.rewards[:]
        del self.baselines[:]
        del self.probs[:]
        del self.idx[:]
    