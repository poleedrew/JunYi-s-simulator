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
        #data['job', 'to', 'm'].edge_attr = torch.FloatTensor(self.edge_x).view(-1, 1).contiguous()
        #data['m', 'to', 'job'].edge_attr = torch.FloatTensor(self.edge_x).view(-1, 1).contiguous()
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
    

class Graph_End_to_end:
    def __init__(self, start_time_dt, eqp_id_list, args):
        self.job_x = []
        self.m_x = []
        self.edge_x = []
        self.job_job_edge_src_idx = np.empty(shape=(0,1))
        self.job_job_edge_tar_idx = np.empty(shape=(0,1))
        self.m_m_edge_idx = np.empty(shape=(0,1))
        self.job_edge_idx = np.empty(shape=(0,1))
        self.m_edge_idx = np.empty(shape=(0,1))
        self.start_time_dt = start_time_dt
        self.eqp_id_list = eqp_id_list
        self.num_machine = len(self.eqp_id_list)
        self.args = args
        self.max_capacity = 0
        self.job_unfinished = []
        self.model_abbrs = {}
        self.num_model_abbr = 0

    def get_data(self):
        data = HeteroData()
        data['job'].x = torch.FloatTensor(self.job_x)
        data['m'].x = torch.FloatTensor(self.m_x)

        data['job', 'to', 'job'].edge_index = torch.stack((self.job_job_edge_src_idx, self.job_job_edge_tar_idx), dim=1).t().contiguous()
        data['job', 'to', 'm'].edge_index = torch.stack((self.job_edge_idx, self.m_edge_idx), dim=1).t().contiguous()
        data['m', 'to', 'job'].edge_index = torch.stack((self.m_edge_idx, self.job_edge_idx), dim=1).t().contiguous()
        data['m', 'to', 'm'].edge_index = torch.stack((self.m_m_edge_idx, self.m_m_edge_idx), dim=1).t().contiguous()
        ## edge_attr
        data['job', 'to', 'm'].edge_attr = self.edge_x.view(-1, 1).contiguous()
        data['m', 'to', 'job'].edge_attr = self.edge_x.view(-1, 1).contiguous()
        return data.to(self.args.device)

    def build_graph(self, wip_info_list):
        if self.args.norm_capacity:
            for wip_info in wip_info_list:
                for value in wip_info['capacity'].values():
                    self.max_capacity = max(int(value), self.max_capacity)

        ## job_job_edge
        for i, wip_info_x in enumerate(wip_info_list):
            for j, wip_info_y in enumerate(wip_info_list):
                if wip_info_x['model_abbr'] == wip_info_y['model_abbr']:
                    self.job_job_edge_src_idx = np.append(self.job_job_edge_src_idx, [i])
                    self.job_job_edge_tar_idx = np.append(self.job_job_edge_tar_idx, [j])

        ## job_machine_edge
        for i, wip_info in enumerate(wip_info_list):
            avai_eqp_ids = wip_info['capacity'].keys()
            for j, eqp_id in enumerate(avai_eqp_ids):
                self.job_edge_idx = np.append(self.job_edge_idx, [i])
                self.m_edge_idx = np.append(self.m_edge_idx, [j])
                if wip_info['model_abbr'] not in self.model_abbrs.keys():
                    self.num_model_abbr += 1
                    self.model_abbrs[wip_info['model_abbr']] = self.num_model_abbr
                if self.args.norm_capacity:
                    self.edge_x.append(wip_info['capacity'][eqp_id] / self.max_capacity)
                else:
                    self.edge_x.append(wip_info['capacity'][eqp_id])
        
        ## machine_machine_edge            
        for i in range(self.num_machine):
            self.m_m_edge_idx = np.append(self.m_m_edge_idx, [i])

    def get_latest_max_qtime_dt(self, wip_info_list):
        latest_max_qtime_dt = self.start_time_dt
        for wip_info in wip_info_list:
            max_qtime_dt = wip_info['max_qtime']
            latest_max_qtime_dt = max(latest_max_qtime_dt, max_qtime_dt)
        return latest_max_qtime_dt

    def update_feature(self, wip_info_list, machines, current_time_dt, gap):
        self.job_x, self.m_x = [], []
        latest_max_qtime_dt = self.get_latest_max_qtime_dt(wip_info_list)
        count = 0
        for i, wip_info in enumerate(wip_info_list):            
            features = []
            if self.args.delete_node == True:
                if wip_info['done'] == True:
                    idx = i
                    if idx in self.job_unfinished:
                        self.job_unfinished.remove(idx)
                        self.update_graph(count)
                else:
                    count += 1
                
            else:
                features.append(wip_info['done']) # wip_info status

            min_qtime_dt = wip_info['min_qtime']
            max_qtime_dt = wip_info['max_qtime']
            
            min_qtime_feat = (min_qtime_dt - self.start_time_dt).total_seconds() / \
                (latest_max_qtime_dt - self.start_time_dt).total_seconds()
            max_qtime_feat = (max_qtime_dt - self.start_time_dt).total_seconds() / \
                (latest_max_qtime_dt - self.start_time_dt).total_seconds()
            contri_feat = self.get_contribute_num(wip_info, gap)
            if self.args.size_feat:
                features.append(wip_info['size']/56)  ## job size normalized by 56
            features.append(min_qtime_feat)
            features.append(max_qtime_feat)
            features.append(contri_feat)

            for eqp_id in self.eqp_id_list:
                setup_time = 0
                if self.args.setup_feat:
                    machine = machines[eqp_id]
                    setup_time = machine.setup_time_handler(wip_info).total_seconds()
                if eqp_id not in wip_info['capacity']:
                    features.append(0)
                else:
                    if self.args.norm_capacity:
                        features.append((wip_info['capacity'][eqp_id] + setup_time) / self.max_capacity)
                    else:
                        features.append((wip_info['capacity'][eqp_id] + setup_time) / 15600)
            
            if self.args.model_feat:
                features.append(self.model_abbrs[wip_info['model_abbr']])
            self.job_x.append(features)
        
        self.convert_to_tensor()
        for eqp_id, machine in machines.items():
            features = [0] * 4
            status = machine.get_status(current_time_dt)
            features[status] = 1
            features.append(machine.get_utilization())
            features.append(machine.get_time_left_ratio(current_time_dt))
            if self.args.model_feat:
                m_last_wip = machine.last_wip()
                if m_last_wip == None or m_last_wip['sheet_status'] == 'RUN':
                    model_feat = 0 # modified as biggest value
                else:
                    model_feat = self.model_abbrs[m_last_wip['model_abbr']]
                
                features.append(model_feat)
            self.m_x.append(features)
        self.job_x = torch.Tensor(self.job_x)
        self.m_x = torch.Tensor(self.m_x)

    def convert_to_tensor(self):
        self.job_job_edge_src_idx = torch.LongTensor(self.job_job_edge_src_idx)
        self.job_job_edge_tar_idx = torch.LongTensor(self.job_job_edge_tar_idx)
        self.job_edge_idx = torch.LongTensor(self.job_edge_idx)
        self.m_edge_idx = torch.LongTensor(self.m_edge_idx)
        self.m_m_edge_idx = torch.LongTensor(self.m_m_edge_idx)
        self.edge_x = torch.FloatTensor(self.edge_x)

    def get_contribute_num(self, wip_info, gap):
        ## gap = {'PC0084': -20}
        contri_ratio = 0
        for day in range(len(gap)):
            day_ratio = 0
            for pc, num in gap[day].items():
                if wip_info['product_code'] != pc or num >= 0:
                    continue
                else:
                    if wip_info['size'] >= -num:
                        day_ratio = 1.0
                    else:
                        day_ratio = wip_info['size'] / -num
                    contri_ratio += day_ratio
        return contri_ratio
            
    def update_graph(self, idx):
        src_idxs = np.where(self.job_job_edge_src_idx == idx)

        self.job_job_edge_src_idx = np.delete(self.job_job_edge_src_idx, src_idxs)
        self.job_job_edge_tar_idx = np.delete(self.job_job_edge_tar_idx, src_idxs)
        tar_idxs = np.where(self.job_job_edge_tar_idx == idx)

        self.job_job_edge_src_idx = np.delete(self.job_job_edge_src_idx, tar_idxs)
        self.job_job_edge_tar_idx = np.delete(self.job_job_edge_tar_idx, tar_idxs)

        idxs = np.where(self.job_edge_idx == idx)
        self.job_edge_idx = np.delete(self.job_edge_idx, idxs)
        self.m_edge_idx = np.delete(self.m_edge_idx, idxs)
        self.edge_x = np.delete(self.edge_x, idxs)

        _, self.job_edge_idx = np.unique(self.job_edge_idx, return_inverse=True)
        _, self.job_job_edge_src_idx = np.unique(self.job_job_edge_src_idx, return_inverse=True)
        _, self.job_job_edge_tar_idx = np.unique(self.job_job_edge_tar_idx, return_inverse=True)

        
        