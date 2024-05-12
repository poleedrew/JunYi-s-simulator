import gym, yaml, copy
from datetime import timedelta
from collections import defaultdict
from env.utils.jsp_instance import JSP_Instance

class AUO_Simulator(gym.Env):
    def __init__(self, system_start_time_dt, system_end_time_dt, args, config_path='./env/config.yml'):        
        self.system_start_time_dt = system_start_time_dt
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.args = args
        self.device = self.args.device if self.args != None else 'cpu'
        self.instance = JSP_Instance(self.config, system_start_time_dt, system_end_time_dt, args)

    def reset(self):
        self.instance.reset()
        return self.get_state()
    
    def get_state(self):
        return self.instance.get_graph_data()
    
    def is_done(self):
        return self.instance.done()
    
    def get_reward(self):
        return self.instance.reward_function()

    def load_wips(self, wait_wip_path, run_wip_path):
        self.instance.load_wips(wait_wip_path, run_wip_path)

    def load_booking(self, path):
        self.instance.load_booking(path)
        
    def load_plan(self, aout_path, eout_path):
        self.instance.load_plan(aout_path, eout_path)

    def load_gap(self, gap_path):
        self.instance.load_gap(gap_path)
       
    def get_avai_jobs(self):
        return self.instance.get_avai_jobs()

    def step(self, job_id, m_id):
        reward = self.instance.assign(job_id, m_id)
        avai_jobs = self.get_avai_jobs()
        return avai_jobs, reward, self.is_done()

    def get_graph_data(self):
        return self.instance.get_graph_data()
        
    def find_idle(self):
        self.instance.find_idle()

    def put_run_wip2timeline(self):
        self.instance.put_run_wip2timeline()

    def print_result(self):
        self.instance.print_result()    
    
    def write_result(self, path):
        self.instance.write_result(path)