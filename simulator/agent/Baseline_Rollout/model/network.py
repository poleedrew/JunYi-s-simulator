import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from random import sample
from torch.distributions import Categorical
from itertools import accumulate
# from agent.Baseline_Rollout.model.gnn import GNN
from agent.Baseline_Rollout.model.gnn_base import GNN
from torch.optim.lr_scheduler import StepLR


class Net(nn.Module):
    def __init__(self, args):
        super(Net, self).__init__()
        self.hidden_dim = args.hidden_dim
        self.num_policy_layers = args.num_policy_layers
        self.device = args.device
        self.gnn = GNN(args)
        # policy network
        self.policy_layers = torch.nn.ModuleList()
        self.policy_layers.append(nn.Linear(self.hidden_dim*4, self.hidden_dim))
        for _ in range(self.num_policy_layers - 2):
            self.policy_layers.append(nn.Linear(self.hidden_dim, self.hidden_dim))
        self.policy_layers.append(nn.Linear(self.hidden_dim, 1))

    def forward(self, avai_ops, graph, greedy=False, T=1.0):
        data = graph.get_data()
        x_graph, x_dict = self.gnn(data)
        avai_list = graph.job_unfinished
        scores = torch.empty(size=(0, self.hidden_dim * 4)).to(self.device)
        # print(avai_list)
        for order, (job_id, m_id) in enumerate(avai_ops):
            job_m = torch.cat((x_dict['m'][m_id], x_dict['job'][avai_list.index(job_id)]), dim=0).unsqueeze(0)
            x_pair = torch.cat((x_graph, job_m), dim=1)
            scores = torch.cat((scores, x_pair), dim=0)
        for i in range(self.num_policy_layers - 1):
            scores = F.relu(self.policy_layers[i](scores))
        scores = self.policy_layers[self.num_policy_layers - 1](scores).t().squeeze()

        probs = F.softmax(scores, dim=0).flatten()
        dist = Categorical(probs)
        if greedy == True:
            idx = torch.argmax(scores)
        else:
            idx = dist.sample()
        return idx.item(), probs[idx].item(), dist.log_prob(idx), dist.entropy()
    

class Network(nn.Module):
    def __init__(self, args):
        super(Network, self).__init__()
        self.args = args
        self._net = Net(args)
        self._optimizer = torch.optim.Adam(self._net.parameters(), lr=args.lr)
        # self._step_scheduler = StepLR(self._optimizer, step_size=20, gamma=0.99)
    
    def forward(self, avai_ops, data, greedy=False, T=1.0):
        return self._net(avai_ops, data, greedy, T)


    def update(self, iter, log_probs, entropies, baselines, rewards):
        self._optimizer.zero_grad()
        loss, policy_loss, entropy_loss = self.calculate_loss(log_probs, entropies, baselines, rewards)
        loss.backward()
        self._optimizer.step()
        # self._step_scheduler.step()
        # print("Iter : {} \t\tLearning rate : {}".format(iter, self._step_scheduler.get_last_lr()[0]))
        print("Iter : {} \t\tLearning rate : {}".format(iter, self.args.lr))
        print('loss mean:', loss.item())
        print('policy loss:', policy_loss)
        print('entropy loss:', entropy_loss)
        return loss, policy_loss, entropy_loss
        
    def calculate_loss(self, log_probs, entropies, baselines, rewards):
        loss = 0.0
        policy_loss = 0.0
        entropy_loss = 0.0
        returns = torch.FloatTensor(list(accumulate(rewards[::-1]))[::-1]).to(self.args.device)
        num_step = len(rewards)
        for log_prob, entropy, baseline, R in zip(log_probs, entropies, baselines, returns):
            if baseline == 0:
                advantage = R * 1 
            else:
                advantage = (R - baseline) / baseline
            policy_loss += -log_prob * advantage
            entropy_loss += entropy
        policy_loss /= num_step
        entropy_loss /= num_step
        loss += policy_loss - self.args.entropy_coef * entropy_loss
        return loss.mean(), policy_loss , entropy_loss
    
        
    def save(self, model_path, checkpoint=True):
        if checkpoint:
            torch.save(
                {
                    'Policy': self._net.state_dict(),
                    # 'step_scheduler': self._step_scheduler.state_dict(),
                    'optimizer': self._optimizer.state_dict(),
                }, model_path)
        else:
            torch.save({
                'Policy': self._net.state_dict()
            }, model_path)

    def load(self, model_path, checkpoint=True):
        model = torch.load(model_path)
        self._net.load_state_dict(model['Policy'])
        if checkpoint:
            self._optimizer.load_state_dict(model['optimizer'])
            # self._step_scheduler.load_state_dict(model['step_scheduler'])
