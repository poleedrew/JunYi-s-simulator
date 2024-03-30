import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import HeteroConv, Linear, MLP, GINConv, GINEConv
from torch import Tensor
from torch_geometric.nn.conv import MessagePassing
from torch_geometric.typing import OptPairTensor
torch.set_printoptions(threshold=10_000)

class GINEConv(MessagePassing):
    def __init__(self, nn, eps = 0, train_eps = False, edge_dim = None, **kwargs):
        super().__init__(aggr='add')
        self.nn = nn
        self.initial_eps = eps
        if train_eps:
            self.eps = torch.nn.Parameter(torch.Tensor([eps]))
        else:
            self.register_buffer('eps', torch.Tensor([eps]))
        self.reset_parameters()

    def reset_parameters(self):
        self.eps.data.fill_(self.initial_eps)

    def forward(self, x, edge_index, edge_attr = None, size = None):
        if isinstance(x, Tensor):
            x: OptPairTensor = (x, x)
        # propagate_type: (x: OptPairTensor, edge_attr: OptTensor)
        # self-loop of op-op, m-m is in edge_index
        out = self.propagate(edge_index, x=x, edge_attr=edge_attr, size=size)
        if out.shape[0] == 1:
            out = torch.cat((out, out), dim=0)
            return self.nn(out)[:1]

        return self.nn(out)

    def message(self, x_j, edge_attr = None):
        if edge_attr is not None:
            return torch.cat((x_j, edge_attr), dim=1)
        else:
            return x_j

    def __repr__(self):
        return f'{self.__class__.__name__}(nn={self.nn})'

class GNN(nn.Module):
    def __init__(self, args):
        super(GNN, self).__init__()
        self.num_layers = args.GNN_num_layers
        self.hidden_dim = args.hidden_dim
        self.convs = torch.nn.ModuleList()
        # self.m_trans_fc = Linear(3, 7) # remove similar term
        self.m_trans_fc = Linear(4, 7)
        in_dim = 7
        for _ in range(self.num_layers):
            nn1 = MLP([in_dim + 4, self.hidden_dim, self.hidden_dim])
            nn2 = MLP([in_dim + 4, self.hidden_dim, self.hidden_dim])
            nn3 = MLP([in_dim, self.hidden_dim, self.hidden_dim])
            nn4 = MLP([in_dim, self.hidden_dim, self.hidden_dim])
            conv = HeteroConv({
                ('job', 'to', 'm'): GINEConv(nn=nn1, edge_dim=4),
                ('m', 'to', 'job'): GINEConv(nn=nn2, edge_dim=4),
                ('job', 'to', 'job'): GINEConv(nn=nn3),
                ('m', 'to', 'm'): GINEConv(nn=nn4)  
            }, aggr='sum')
            self.convs.append(conv)
            in_dim = self.hidden_dim
        self.job_fc = Linear(self.hidden_dim, self.hidden_dim)
        self.m_fc = Linear(self.hidden_dim, self.hidden_dim)  
        
    def forward(self, data, step):
        x_dict, edge_index_dict, edge_attr_dict = data.x_dict, data.edge_index_dict, data.edge_attr_dict
        #if step == 1:
        #    print(x_dict['job'])
        #   print(x_dict['m'])
            #print(x_dict['job'][0])
            #print(x_dict['job'][2])
        #    print("------------")
            #print(f'edge_index_dict : {edge_index_dict}')
            #print(f'edge_attr_dict : {edge_attr_dict}')

        x_dict['m'] = self.m_trans_fc(x_dict['m'])
        for conv in self.convs:
            x_dict = conv(x_dict, edge_index_dict, edge_attr_dict)
            x_dict = {key: F.relu(x) for key, x in x_dict.items()}
        x_dict['job'] = self.job_fc(x_dict['job'])
        x_dict['m'] = self.m_fc(x_dict['m'])
        #if step == 1:
        #    print(x_dict['job'])
        #    print(x_dict['m'])
        #    print("----------------------")
        return x_dict
