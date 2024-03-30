
from datetime import datetime, timedelta
import json, csv, os, copy
from collections import defaultdict
from env.utils.machine import ODFMachine
from env.utils.utils import str2datetime, datetime2str
from env.utils.graph import Graph
import bisect

class JSP_Instance:
    def __init__(self, config, system_start_time_dt, system_end_time_dt, args=None):
        self.args = args
        self.config = config
        self.start_time_dt = system_start_time_dt
        self.end_time_dt = system_end_time_dt
        self.current_time = self.start_time_dt
        self.eqp_id2id = {
            "CKODF100": 0,
            "CKODF200": 1,
            "CKODF300": 2,
            "CKODF400": 3,
            "CKODF500": 4,
        }
        self.init_values()

    def init_values(self):
        self.num_machine = len(self.config['eqp_id_list'])
        self.eqp_id_list = self.config['eqp_id_list']
        self.stochastic = self.config['stochastic']
        self.gap_product = [defaultdict(int) for _ in range(self.args.total_days)]
        # self.gap = defaultdict(int)
        self.order = 0
        self.time_stamp = []
        self.history = []
        self.current_time = self.start_time_dt
        self.register_time(self.start_time_dt)
        self.duration = (self.end_time_dt - self.start_time_dt).total_seconds() / 60
        self.completed_wip = []
        odf_machine_config = {
            'add_setup_time': self.config['add_setup_time'],
            'stochastic': self.config['stochastic'],
            'start_time': self.start_time_dt,
        }
        self.odf_machines = {}
        for machine_id, eqp_id in enumerate(self.config['eqp_id_list']):
            machine = ODFMachine(machine_id, eqp_id, odf_machine_config)
            self.odf_machines[eqp_id] = machine
            
        self.wait_wip_info_list = []
        self.run_wip_info_list = []
        self.num_wait_sheets = 0
        self.num_run_sheets = 0
        self.total_days = self.args.total_days 
        # total
        self.total_over_qtime_sheet_count, self.total_min_tact_time_sheet_count, self.total_setup_time, self.total_gap, self.total_return = 0, 0, 0, 0, 0
        # reward
        self.over_qtime_reward = 0
        self.tact_time_reward = 0
        self.setup_reward = 0
        self.dps_reward = 0
        self.contribute = 0

        self.cumulative = defaultdict(int)

        self.BASE_TACT_TIME = 90
        self.load_product_tact_time('../AUO_data/tact_time/imputed/14_STD_TACTTIME_imputed_update.csv')
        self.graph = Graph(len(self.odf_machines))

    def reset(self):
        self.init_values()
        assert len(self.completed_wip) == 0, f'completed_wip should be empty after reset, len(self.completed_wip): {len(self.completed_wip)}'
        assert len(self.history) == 0, f'history should be empty after reset, len(self.history):{len(self.history)}'

    def get_graph_data(self):
        self.graph.update_feature(
            self.wait_wip_info_list, 
            self.odf_machines, 
            self.current_time,
            self.end_time_dt,
            self.duration,
            self.eqp_id2id,
            self.gap_product[0]
        )
        data = self.graph.get_data().to(self.args.device)
        return data

    def done(self):
        if self.current_time >= self.end_time_dt:
            return True
        else:
            for wip_info in self.wait_wip_info_list:
                if not wip_info['done']:
                    return False
            return True

    def reward_function(self):
        value = ((self.args.w0 * self.over_qtime_reward) + (self.args.w1 * self.tact_time_reward) + (self.args.w2 * self.setup_reward) + (self.args.w3 * self.dps_reward))
        return value

    ## for product_replace_ratio
    def load_product_tact_time(self, path):
        self.product_tact_time_pc = {}
        with open(path, newline='', encoding='iso-8859-1') as csvfile:
            rows = csv.reader(csvfile)
            for i, row in enumerate(rows):
                if i == 0:
                    _, _, *eqp_id_list = row
                else:
                    product_code, _, *(eqp_tact_times) = row
                    eqp_tact_times = [int(time) if time != '' else 0 for time in eqp_tact_times]
                    assert product_code[:2] == 'PC', f"The first two char of product_code should be PC, but get {product_code}"
                    assert eqp_tact_times != [0, 0, 0, 0, 0], 'tact time should not be all zero'
                    self.product_tact_time_pc[product_code] = eqp_tact_times
                    for i, eqp_id in enumerate(eqp_id_list):
                        self.product_tact_time_pc[product_code, eqp_id] = eqp_tact_times[i]

    def load_wips(self, wait_wip_path, run_wip_path):
        with open(wait_wip_path, 'r') as fp:
            wait_wip_info_list = json.load(fp)
        with open(run_wip_path, 'r') as fp:
            run_wip_info_list = json.load(fp)
        for wip_info in wait_wip_info_list:
            str2datetime(wip_info)
            self.num_wait_sheets += wip_info['size']
        for wip_info in run_wip_info_list:
            str2datetime(wip_info)
            self.num_run_sheets += wip_info['size']
        for wip_info in run_wip_info_list:
            if wip_info['min_qtime'] > self.end_time_dt:
                continue
            for key in wip_info['capacity'].keys():
                wip_info['capacity'][key] = (wip_info['capacity'][key] / 60)
                wip_info['tact_time'][key] = wip_info['capacity'][key] / wip_info['size']
            self.run_wip_info_list.append(wip_info)
        self.put_run_job2timeline()
        self.model_abbr_count = defaultdict(int)
        cnt = 0
        for wip_info in wait_wip_info_list:
            model_abbr = wip_info['model_no'] + '-' + wip_info['abbr_no']
            self.model_abbr_count[model_abbr] += 1
            for key in wip_info['capacity'].keys():
                wip_info['capacity'][key] = (wip_info['capacity'][key] / 60)
                wip_info['tact_time'][key] = wip_info['capacity'][key] / wip_info['size']
            if wip_info['min_qtime'] >= self.start_time_dt:
                self.register_time(wip_info['min_qtime'])
        
            self.graph.add_job(wip_info, self.eqp_id2id)
            self.wait_wip_info_list.append(wip_info)
            cnt += 1

    def load_booking(self, path):
        start_date = self.start_time_dt.date()
        root_dir = path.split('/')[-3]
        # for i in range(self.total_days):
        for i in range(1):
            load_date = start_date + timedelta(days=i)
            load_date = datetime.strftime(load_date, "%Y-%m-%d")

            path = os.path.join(root_dir, load_date, 'booking.json')

            with open(path, 'r') as f:
                self.bookings = list(json.load(f))                

            for booking in self.bookings:
                str2datetime(booking)
                
                eqp_id = booking['selected_eqp_id']
                start_time = booking['start_time']
                finish_time = booking['finish_time']
                machine = self.odf_machines[eqp_id]
                machine.load_booking(start_time, finish_time)
                self.register_time(finish_time)

            self.history += self.bookings

    def load_gap(self, gap_path):
        with open(gap_path, 'r') as fp:
            self.gap = json.load(fp)
        ## add remainder product in past days
        
        start_date = self.start_time_dt.date()
        remainder_path = os.path.join(self.args.gap_path, gap_path.split('/')[-2], "remainder.json")
        with open(remainder_path, 'r') as fp:
            remainder = json.load(fp)
        self.gap_product = [defaultdict(int) for _ in range(self.total_days)]
        cnt = [0 for _ in range(self.total_days)]
        for mfg, product_list in self.gap.items():
            mfg = datetime.strptime(mfg, "%Y-%m-%d %H:%M:%S.0").date()
            for i in range(self.total_days):
                if mfg > start_date + timedelta(days=i):
                    continue
                for product_code, num_sheet in product_list.items():
                    if num_sheet <= 0:
                        continue
                    if i > 0 and product_code in remainder.keys() and remainder[product_code] > 0:
                        if remainder[product_code] > num_sheet:
                            num_sheet = 0
                        else:
                            num_sheet -= remainder[product_code]
                    self.gap_product[i][product_code] += num_sheet
                    cnt[i] += num_sheet
        
    
    def get_avai_jobs(self):
        if self.done() == True:
            return None
        
        avai_jobs = []
        select_action = False
        for machine in self.odf_machines.values():
            machine.update_booking_avai_time(self.current_time)
            if machine.current_time > self.current_time:
                continue
            m_avai_jobs = []
            for id, wip_info in enumerate(self.wait_wip_info_list): 
                if wip_info['done'] or wip_info['min_qtime'] > self.current_time:
                    continue
                if machine.eqp_id in wip_info['capacity'].keys():
                    if machine.not_process_in_booking(wip_info, self.current_time):
                        m_avai_jobs.append({'job_id': id, 'm_id': machine.eqp_id})
                        select_action = True
            if m_avai_jobs != []:
                avai_jobs.append(m_avai_jobs)

        if select_action == True:
            return avai_jobs

        self.update_time()
        return self.get_avai_jobs()
    
    def get_product_replace_ratio(self, wip_info, eqp_id):
        product_code = wip_info['product_code']
        selected_tact_time = wip_info['tact_time'][eqp_id]
        prodcut_tact_time = self.product_tact_time_pc[product_code, eqp_id]
        min_tact_time = min(wip_info['tact_time'].values())

        if selected_tact_time == min_tact_time:
            self.total_min_tact_time_sheet_count += wip_info['size']
        

        for id in wip_info['tact_time'].keys():
            if self.product_tact_time_pc[product_code, id] > prodcut_tact_time:
                prodcut_tact_time = self.product_tact_time_pc[product_code, id]
        
        self.product_replace_ratio = prodcut_tact_time / self.BASE_TACT_TIME
        
    def get_tact_time_reward(self, wip_info, eqp_id):
        # min tact time reward
        selected_tact_time = wip_info['tact_time'][eqp_id]
        min_tact_time = min(wip_info['tact_time'].values())
        # self.tact_time_reward = -wip_info['size'] * (selected_tact_time - min_tact_time) / self.BASE_TACT_TIME
        if selected_tact_time == min_tact_time:
            self.tact_time_reward = 10
        else:
            self.tact_time_reward = 1
    def get_overq_time_reward(self, wip_info, eqp_id):
        # over q-time reward
        if wip_info['max_qtime'] < wip_info['start_time']:
            self.total_over_qtime_sheet_count += wip_info['size']
            # self.over_qtime_reward = -wip_info['size'] * self.product_replace_ratio
            self.over_qtime_reward = 1
        else:
            # self.over_qtime_reward = 0
            self.over_qtime_reward = 10

    def assign(self, job_id, m_id):
        wip_info = self.wait_wip_info_list[job_id]
        eqp_id = m_id
        assert not wip_info['done'], f"WIP should not be done before processing"
        shot_wip, self.setup_time, wip_info = self.odf_machines[eqp_id].process_one_wip(wip_info, self.current_time, self.history)
        self.register_time(wip_info['finish_time'])
        self.total_setup_time += self.setup_time.total_seconds() / 3600
        
        if self.setup_time.total_seconds() == 0:
            self.setup_reward = 10
        elif self.setup_time.total_seconds() == 1800:
            self.setup_reward = 2
        else:
            self.setup_reward = 1
        # self.setup_reward = -(self.setup_time.total_seconds() / self.BASE_TACT_TIME)

        self.completed_wip.append(wip_info)
        assert wip_info['done'], 'WIP should be done after processing'
        assert wip_info['start_time'] < wip_info['finish_time'], f'wip start time should smaller than finish time {wip_info["sheet_status"], wip_info["size"]}'
        
        self.get_product_replace_ratio(wip_info, eqp_id)
        self.get_tact_time_reward(wip_info, eqp_id)
        self.get_overq_time_reward(wip_info, eqp_id)

        # scheduling order
        wip_info['order'] = self.order
        wip_info['return'] = self.total_return
        self.order += 1
        self.dps_reward = 0
        if self.gap_product[0][wip_info['product_code']] > 0:
            if wip_info['size'] < self.gap_product[0][wip_info['product_code']]:
                self.dps_reward = wip_info['size']
                self.gap_product[0][wip_info['product_code']] -= wip_info['size']
            else:
                self.dps_reward = self.gap_product[0][wip_info['product_code']]
                self.gap_product[0][wip_info['product_code']] = 0

        return self.reward_function()

    def get_makespan(self):
        # unit: hour
        return (max(machine.current_time for machine in self.odf_machines.values(
        )) - self.start_time_dt).total_seconds() / 3600

    def find_idle(self):
        history_eqp_id = defaultdict(list)
        for wip_info in self.history:
            history_eqp_id[wip_info['selected_eqp_id']].append(wip_info)
        for eqp_id, wip_info_list in history_eqp_id.items():
            sorted_wip_info_list = sorted(
                wip_info_list, key=lambda wip_info: wip_info['start_time'])
            prev_wip_info = None
            for _, wip_info in enumerate(sorted_wip_info_list):
                if prev_wip_info == None:
                    prev_wip_info = wip_info
                    continue
                if wip_info['start_time'] < prev_wip_info['finish_time']:
                    if wip_info['finish_time'] > prev_wip_info['finish_time']:
                        prev_wip_info['finish_time'] = wip_info['start_time']
                    elif wip_info['finish_time'] < prev_wip_info['finish_time']:
                        new_info = copy.deepcopy(prev_wip_info)
                        new_info['start_time'] = wip_info['finish_time']
                        new_info['finish_time'] = wip_info['finish_time']
                        new_info['size'] = 0
                        
                        self.history.append(new_info)
                        prev_wip_info['finish_time'] = wip_info['start_time']
                        prev_wip_info = new_info
                        continue
                elif wip_info['start_time'] > prev_wip_info['finish_time'] and prev_wip_info['start_time'] > self.start_time_dt:
                    self.history.append({
                        "sheet_status": "IDLE",
                        "selected_eqp_id": eqp_id,
                        "model_abbr": "IDLE",
                        "booking": "IDLE",
                        "start_time": prev_wip_info['finish_time'],
                        "finish_time": wip_info['start_time']
                    })
                prev_wip_info = wip_info

    def register_time(self, time):
        # maintain a list in sorted order
        bisect.insort(self.time_stamp, time)

    def update_time(self):
        self.current_time = self.time_stamp.pop(0)

    def print_result(self):
        for machine in self.odf_machines.values():
            print("#######################################")
            print(machine.eqp_id)
            for wip_info in machine.machine_history:
                print(wip_info['cassette_id'], wip_info['start_time'], wip_info['finish_time'])
    
    def write_result(self, path):
        for wip_info in self.history:
            datetime2str(wip_info)
        with open(path, 'w') as f:
            json.dump(self.history, f, indent=4)

    def put_run_job2timeline(self):
        for job_info in self.run_wip_info_list:
            """
            machine = self.odf_machines[self.eqp_id2id[job_info['selected_eqp_id']]]
            # run job but booking??
            if machine.not_process_in_booking(job_info, machine.current_time) == False:
                continue
            #
            machine.process_one_job(job_info, machine.current_time, self.completed_job)
            """
            capacity = timedelta(minutes=job_info['capacity'][job_info['selected_eqp_id']])
            start_logon_gap = timedelta(minutes=(self.start_time_dt - job_info['logon_time']).total_seconds() / 60)
            if capacity > start_logon_gap:
                job_info['order'] = '-1'
                job_info['capacity'][job_info['selected_eqp_id']] = (capacity - start_logon_gap).total_seconds() / 60
                machine = self.odf_machines[job_info['selected_eqp_id']]
                # run job but booking??
                if machine.not_process_in_booking(job_info, machine.current_time) == False:
                    continue
                #
                machine.process_one_wip(job_info, machine.current_time, self.history)
            else:
                job_info['start_time'] = job_info['logon_time']
                job_info['finish_time'] = job_info['logon_time'] + timedelta(minutes=job_info['capacity'][job_info['selected_eqp_id']])
                job_info['done'] = True
                self.history.append(job_info)
            
        for machine in self.odf_machines.values():
            self.register_time(machine.current_time)
        self.current_time = self.time_stamp[0]