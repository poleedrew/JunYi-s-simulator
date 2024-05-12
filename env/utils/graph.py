import torch
import numpy as np
from torch_geometric.data import HeteroData
from datetime import timedelta
AVAILABLE = 0
PROCESSED = 1
COMPLETE = 2
FUTURE = 3

class Graph:
    def __init__(self, machine_num):
        self.job_num = 0
        self.job_x = []
        self.m_x = []
        self.edge_x = []
        self.job_m_edge_idx = []
        self.m_job_edge_idx = []
        self.job_job_edge_idx = []
        self.m_m_edge_idx = self.fully_connect(0, machine_num - 1)
    
    def get_data(self):
        data = HeteroData()
        data['job'].x = torch.FloatTensor(self.job_x)
        data['m'].x = torch.FloatTensor(self.m_x)
        data['job', 'to', 'm'].edge_index = torch.LongTensor(self.job_m_edge_idx).t().contiguous()
        data['m', 'to', 'job'].edge_index = torch.LongTensor(self.m_job_edge_idx).t().contiguous()
        data['job', 'to', 'job'].edge_index = torch.LongTensor(self.job_job_edge_idx).t().contiguous()
        data['m', 'to', 'm'].edge_index = torch.LongTensor(self.m_m_edge_idx).t().contiguous()
        data['job', 'to', 'm'].edge_attr = torch.FloatTensor(self.edge_x).contiguous()
        data['m', 'to', 'job'].edge_attr = torch.FloatTensor(self.edge_x).contiguous()
        return data
    
    def add_job(self, job, eqp_id2id):
        for eqp_id, _ in job['capacity'].items():
            # for job <--> machine edge
            self.job_m_edge_idx.append([self.job_num, eqp_id2id[eqp_id]])
            self.m_job_edge_idx.append([eqp_id2id[eqp_id], self.job_num])
            #self.edge_x.append(capacity / duration)
        self.job_job_edge_idx.append([self.job_num, self.job_num])
        self.job_num += 1
    
    def update_feature(self, jobs, machines, current_time, end_time, duration, eqp_id2id, gap):
        self.job_x, self.m_x, self.edge_x = [], [], []
        # job feature
        for job in jobs:
            feat = [0] * 4
            # status
            status = -1
            if job['done'] == True:
                if current_time >= job['finish_time']:
                    status = COMPLETE
                else:
                    status = PROCESSED
            else:
                if current_time >= job['min_qtime']:
                    status = AVAILABLE
                else:
                    status = FUTURE                
            feat[status] = 1
            # time to complete
            if status == PROCESSED:
                feat.append(((job['finish_time'] - current_time).total_seconds() // 60) / duration)
            else:
                feat.append(0)
            # available time
            if status == FUTURE:
                feat.append(((job['min_qtime'] - current_time).total_seconds() // 60) / duration)
            else:
                feat.append(0)
            # size
            if gap[job['product_code']] > 0:
                if job['size'] < gap[job['product_code']]:
                    feat.append(job['size'] / 56)
                else:
                    feat.append(gap[job['product_code']] / 56)
            else:
                feat.append(0)
            #feat.append(job['size'])
            self.job_x.append(feat)
            
            # edge feature
            for eqp_id, capacity in job['capacity'].items():
                feat = [0] * 3
                setup_time = machines[eqp_id].setup_time_handler(job, current_time)
                if setup_time == timedelta(hours=0):
                    feat[0] = 1
                elif setup_time == timedelta(hours=0.5):
                    feat[1] = 1
                else:
                    feat[2] = 1
                feat.append(capacity / duration)
                self.edge_x.append(feat)
            
        #print(len(self.edge_x))
        #raise NotImplementedError
        # machine feature
        for m in machines.values():
            feat = [0] * 2
            # status
            status = m.get_status(current_time)
            feat[status] = 1
            # available time
            if status == AVAILABLE:
                feat.append(0)
            else:
                feat.append(((m.current_time - current_time).total_seconds() // 60) / duration)
            # remaining time test_remove
            feat.append(((end_time - current_time).total_seconds() // 60) / duration)
            
            self.m_x.append(feat)
        
    def fully_connect(self, begin, end):
        edge_idx = []
        for i in range(begin, end + 1):
            # edge_idx.append([i, i])
            # test_remove
            for j in range(begin, end + 1):
                edge_idx.append([i, j])
        return edge_idx

        
        