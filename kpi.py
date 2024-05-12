import os, json, math, copy
from datetime import datetime, timedelta
from dateutil.parser import parse
from collections import defaultdict
from env.utils.utils import str2datetime
DECIMAL = 2

class KPI:
    def __init__(self, result_path, future_day_num=3):
        self.result_path = result_path
        self.future_day_num = future_day_num

        self.history_output = 0 # before the date actual output
        self.gap_product = [defaultdict(int) for _ in range(future_day_num)]
        self.total_history_num = [0 for _ in range(future_day_num)]    # AUO history num 
        self.total_expected_num = [0 for _ in range(future_day_num)]   # AUO expected num
        self.total_contribute_num = [0 for _ in range(future_day_num)] # AUO contribute num

        self.model_abbr = defaultdict(int) # model_abbr type and num
        self.rule_count = defaultdict(int) # rule use count
        
        self.date_actual_output = 0 # the date actual output
        # print(result_path)
       
        with open(result_path, 'r') as f:
            self.result = json.load(f)
    def is_booking(self, wip_info):
        return 'booking' in wip_info

    def load_plan(self, plan_dir, date):
            
        aout_path = os.path.join(plan_dir, "actual_output.json")
        eout_path = os.path.join(plan_dir, date, "expected_output.json")
        gap_path = os.path.join(plan_dir, date, "gap.json")
        
        with open(aout_path, 'r') as fp:
            self.acutal_output = json.load(fp)
        with open(eout_path, 'r') as fp:
            self.expected_output = json.load(fp)
        with open(gap_path, 'r') as fp:
            self.gap = json.load(fp)

        date = datetime.strptime(date, "%Y-%m-%d")
        self.today_product = defaultdict(int)
        self.history_product = defaultdict(int)
        for mfg_day, date_output_product_dict in self.acutal_output.items():
            mfg_day = datetime.strptime(mfg_day, "%Y-%m-%d %H:%M:%S.0")
            if mfg_day.month != date.month:
                continue
            if mfg_day <= date:
                if mfg_day == date:
                    for product, output in date_output_product_dict.items():
                        self.today_product[product] += output
                    continue
                for product, output in date_output_product_dict.items():
                    self.history_product[product] += output
                    self.history_output += output
            else:
                continue

        self.expected_output_list = [defaultdict(int) for _ in range(self.future_day_num)]

        for mfg_day, date_output_product_dict in self.expected_output.items():
            mfg_day = datetime.strptime(mfg_day, "%Y-%m-%d %H:%M:%S.0")
            settle_date = date
            for day in range(self.future_day_num):
                settle_date += timedelta(days=day)
                for product, output in date_output_product_dict.items():
                    if mfg_day.month != settle_date.month:
                        continue
                    if mfg_day <= settle_date:
                        self.expected_output_list[day][product] += output
                        self.total_expected_num[day] += output
        self.calculate_gap()

    def calculate_model_abbr_num(self):
        result = copy.deepcopy(self.result)
        counter = 0
        for wip_info in result:
            if self.is_booking(wip_info):
                continue
            counter += wip_info['size']
            model_abbr = wip_info['model_abbr']
            if model_abbr != "IDLE" and model_abbr != "DMQC" and model_abbr != "TEST":
                self.model_abbr[model_abbr] += 1
        
    def add_completion(self, settle_time_dt):
        self.out_gap = copy.deepcopy(self.gap_product)
        
        result = copy.deepcopy(self.result)
        self.output_product = [defaultdict(int) for _ in range(self.future_day_num)]
        self.contribute_product = [defaultdict(int) for _ in range(self.future_day_num)]
        self.production = defaultdict(int)
        for wip_info in result:
            if 'product_code' not in wip_info:
                continue
            product_code = wip_info['product_code']
            if self.is_booking(wip_info):
                continue
            str2datetime(wip_info)
            self.production[product_code] += wip_info['size']
            for day in range(self.future_day_num):
                if product_code not in self.out_gap[day].keys() or \
                   self.out_gap[day][product_code] >= 0 or settle_time_dt + timedelta(days=day) < wip_info['start_time']:
                    continue
                if wip_info['start_time'] < settle_time_dt + timedelta(days=day) < wip_info['finish_time']:
                    complete_rate = (settle_time_dt + timedelta(days=day) - wip_info['start_time']).total_seconds() / (wip_info['finish_time'] - wip_info['start_time']).total_seconds()
                    completion = math.floor(wip_info['size'] * complete_rate)
                else:
                    completion = wip_info['size']
                
                count_sheets = completion if completion <= -self.out_gap[day][product_code] else -self.out_gap[day][product_code]
                
                self.total_contribute_num[day] += count_sheets
                self.contribute_product[day][product_code] += count_sheets
                self.output_product[day][product_code] += completion
                self.out_gap[day][product_code] += count_sheets

        self.dps_0 = (self.total_contribute_num[0] + self.total_history_num[0]) / self.total_expected_num[0]
        self.dps_1 = (self.total_contribute_num[1] + self.total_history_num[1]) / self.total_expected_num[1]
        # self.dps_2 = (self.total_contribute_num[2] + self.total_history_num[2]) / self.total_expected_num[2]
        # self.total_gap_0 = sum(val for val  in self.out_gap[0].values() if val < 0)
        # self.total_gap_1 = sum(val for val  in self.out_gap[1].values() if val < 0)
        # self.total_gap_2 = sum(val for val  in self.out_gap[2].values() if val < 0)
            

    
    def calculate_actual_dps_rate(self):
        tmp_gap = copy.deepcopy(self.gap_product[0])
        self.actual_count_output_product = defaultdict(int)
        for product, count in self.today_product.items():
            count_sheet = 0
            if product in tmp_gap.keys():
                if tmp_gap[product] >= 0:
                    continue
                count_sheet = count if count <= -tmp_gap[product] else -tmp_gap[product]
                self.actual_count_output_product[product] += count_sheet

                self.date_actual_output += count_sheet
                tmp_gap[product] += count_sheet
        self.total_actual_dps_rate = (self.total_history_num[0] + self.date_actual_output) / self.total_expected_num[0]

    def calculate_gap(self):
        self.total_history_num = [self.history_output for _ in range(self.future_day_num)]
        for day in range(self.future_day_num):
            check_pc = []
            for product, eout in self.expected_output_list[day].items():
                check_pc.append(product)
                aout = self.history_product.get(product, 0)
                if (aout - eout) < 0:
                    self.gap_product[day][product] += (aout - eout)
                else:
                    self.total_history_num[day] -= (aout - eout)
            for product in self.history_product.keys():
                if product not in check_pc:
                    self.total_history_num[day] -= self.history_product[product]
    def get_rule_count(self):
        for wip_info in self.result:
            if 'rule' not in wip_info.keys():
                continue
            rule_name = wip_info['rule']
            if rule_name == '':
                continue
            self.rule_count[rule_name] += 1
        return dict(self.rule_count)

    def get_total_run_time(self):
        run_time = timedelta(seconds=0)
        for wip_info in self.result:
            if not self.is_booking(wip_info):
                start_time_dt = datetime.strptime(
                    wip_info['start_time'], '%Y-%m-%d %H:%M:%S')
                finish_time_dt = datetime.strptime(
                    wip_info['finish_time'], '%Y-%m-%d %H:%M:%S')
                run_time += (finish_time_dt - start_time_dt)
        return run_time

    def get_idle_time(self):
        idle_time = timedelta(seconds=0)
        for wip_info in self.result:
            if self.is_booking(wip_info) and wip_info['booking'] == 'IDLE':
                start_time_dt = datetime.strptime(
                    wip_info['start_time'], '%Y-%m-%d %H:%M:%S')
                finish_time_dt = datetime.strptime(
                    wip_info['finish_time'], '%Y-%m-%d %H:%M:%S')
                idle_time += (finish_time_dt - start_time_dt)
        return idle_time

    def get_down_time(self):
        down_time = timedelta(seconds=0)
        for wip_info in self.result:
            if self.is_booking(wip_info) and wip_info['booking'] == 'DOWN':
                start_time_dt = datetime.strptime(
                    wip_info['start_time'], '%Y-%m-%d %H:%M:%S')
                finish_time_dt = datetime.strptime(
                    wip_info['finish_time'], '%Y-%m-%d %H:%M:%S')
                down_time += (finish_time_dt - start_time_dt)
        return down_time

    def get_pm_time(self):
        pm_time = timedelta(seconds=0)
        for wip_info in self.result:
            if self.is_booking(wip_info) and wip_info['booking'] == 'PM':
                start_time_dt = datetime.strptime(
                    wip_info['start_time'], '%Y-%m-%d %H:%M:%S')
                finish_time_dt = datetime.strptime(
                    wip_info['finish_time'], '%Y-%m-%d %H:%M:%S')
                pm_time += (finish_time_dt - start_time_dt)
        return pm_time

    def get_test_time(self):
        test_time = timedelta(seconds=0)
        for wip_info in self.result:
            if self.is_booking(wip_info) and wip_info['booking'] == 'TEST':
                start_time_dt = datetime.strptime(
                    wip_info['start_time'], '%Y-%m-%d %H:%M:%S')
                finish_time_dt = datetime.strptime(
                    wip_info['finish_time'], '%Y-%m-%d %H:%M:%S')
                test_time += (finish_time_dt - start_time_dt)
        return test_time

    def get_dmqc_time(self):
        dmqc_time = timedelta(seconds=0)
        for wip_info in self.result:
            if self.is_booking(wip_info) and wip_info['booking'] == 'DMQC':
                start_time_dt = datetime.strptime(
                    wip_info['start_time'], '%Y-%m-%d %H:%M:%S')
                finish_time_dt = datetime.strptime(
                    wip_info['finish_time'], '%Y-%m-%d %H:%M:%S')
                dmqc_time += (finish_time_dt - start_time_dt)
        return dmqc_time
    
    def get_one_day_dmqc_time(self, date):
        dmqc_time = timedelta(seconds=0)
        settle_time_dt = parse(date) + timedelta(days=1, hours=7, minutes=30) + timedelta(hours=6)
        print(settle_time_dt)
        for wip_info in self.result:
            start_time_dt = datetime.strptime(wip_info['start_time'], '%Y-%m-%d %H:%M:%S')
            finish_time_dt = datetime.strptime(wip_info['finish_time'], '%Y-%m-%d %H:%M:%S')
            if settle_time_dt < start_time_dt:
                continue
            if self.is_booking(wip_info) and wip_info['booking'] == 'DMQC':
                print(start_time_dt, finish_time_dt)             
                dmqc_time += (finish_time_dt - start_time_dt)
        return dmqc_time

    def availability(self):
        run = self.get_total_run_time()
        idle = self.get_idle_time()
        down = self.get_down_time()
        pm = self.get_pm_time()
        test = self.get_test_time()
        dmqc = self.get_dmqc_time()
        return (run + idle).total_seconds() / \
            (run + idle + down + pm + test + dmqc).total_seconds()

    def utilization(self):
        run = self.get_total_run_time()
        idle = self.get_idle_time()
        return run.total_seconds() / (run + idle).total_seconds()

    def over_qtime_sheet_count(self):
        count = 0
        for wip_info in self.result:
            if self.is_booking(wip_info):
                continue
            if wip_info['sheet_status'] == 'RUN':
                continue
            # str2datetime(wip_info)
            max_qtime = datetime.strptime(
                wip_info['max_qtime'], '%Y-%m-%d %H:%M:%S')
            start_time = datetime.strptime(
                wip_info['start_time'], '%Y-%m-%d %H:%M:%S')
            if max_qtime <= start_time:
                count += int(wip_info['size'])
        return count

    def min_tact_time_sheet_count(self):
        count = 0
        for wip_info in self.result:
            if self.is_booking(wip_info):
                continue
            if wip_info['sheet_status'] == 'RUN':
                continue
            selected_eqp_id = wip_info['selected_eqp_id']
            min_eqp_id = min(
                wip_info['tact_time'],
                key=wip_info['tact_time'].get)
            count += (selected_eqp_id == min_eqp_id or wip_info['tact_time'][selected_eqp_id] == wip_info['tact_time'][min_eqp_id])*wip_info['size']
        return count
            
    def speedup(self):
        total_run = self.get_total_run_time()
        perf = timedelta(seconds=0)
        for wip_info in self.result:
            if not self.is_booking(wip_info):
                selected_eqp_id = wip_info['selected_eqp_id']
                capacity = wip_info['capacity']
                min_capacity = min(capacity.values())
                selected_capacity = capacity[selected_eqp_id]
                start_time_dt = datetime.strptime(
                    wip_info['start_time'], '%Y-%m-%d %H:%M:%S')
                finish_time_dt = datetime.strptime(
                    wip_info['finish_time'], '%Y-%m-%d %H:%M:%S')
                perf += (finish_time_dt - start_time_dt) * (min_capacity / selected_capacity)
        return perf / total_run

if __name__ == '__main__':
    
    tasks = [
        # {"name": "2022-12-01", "start": "2022-12-01 08:05:15"},
        # {"name": "2022-12-04", "start": "2022-12-04 08:05:15"},
        {"name": "2022-12-28", "start": "2022-12-28 08:05:29"},
    ]
    # with open("eval_dates/12.json", 'r') as file:
    #     tasks = json.load(file)
    # result_dir = "agent/Baseline_Rollout/result/eval"
    result_dir = "agent/REINFORCE/result/my_dps_reward/round_249"

    plan_dir = "../AUO_data/plan/auo_data"
    for item in tasks:
        start, task_name = item['start'], item['name']
        result_name_dir = os.path.join(result_dir, task_name + '.json')
        plan_name_dir = plan_dir
        date = task_name
        # for i, fn in enumerate(sorted(os.listdir(result_name_dir))):
        result_file_name = os.path.join(result_name_dir)
        kpi_calc = KPI(result_file_name)
        kpi_calc.load_plan(plan_name_dir, date)
        settle_time_dt = parse(task_name) + timedelta(days=1, hours=7, minutes=30) + timedelta(hours=6)
        
        kpi_calc.add_completion(settle_time_dt)
        kpi_calc.calculate_actual_dps_rate()
        
        # 產能面 KPI
        availability = kpi_calc.availability()
        utilization = kpi_calc.utilization()
        speedup = kpi_calc.speedup()
        
        # 需求面 KPI
        gap_product = kpi_calc.gap_product
        
        # 品質面(Qtime) KPI
        over_qtime_sheet_count = kpi_calc.over_qtime_sheet_count()
        min_tact_time_sheet_count = kpi_calc.min_tact_time_sheet_count()
        total_setup_time = kpi_calc.get_one_day_dmqc_time(date).total_seconds() / 3600
        wip_count = 0
        sheet_count = 0
        for wip_info in kpi_calc.result:
            if "order" in wip_info.keys():
                wip_count += 1
                sheet_count += wip_info['size']
        kpi_calc.calculate_model_abbr_num()
        # kpi_calc.get_rule_count()
        print(
            f"{result_file_name:70}\t"
            f"{total_setup_time}\t"
            f"({kpi_calc.total_history_num[0]}+{kpi_calc.date_actual_output})/{kpi_calc.total_expected_num[0]}={kpi_calc.total_actual_dps_rate:.3}\t"
            f"({kpi_calc.total_history_num[0]}+{kpi_calc.total_contribute_num[0]})/{kpi_calc.total_expected_num[0]}={kpi_calc.dps_0:.3}"
            # f"({kpi_calc.total_history_num[1]}+{kpi_calc.total_contribute_num[1]})/{kpi_calc.total_expected_num[1]}={kpi_calc.dps_1:.3}"
            # f"({kpi_calc.total_history_num[2]}+{kpi_calc.total_contribute_num[2]})/{kpi_calc.total_expected_num[2]}={kpi_calc.dps_2:.3}"
            # f"{round(availability, DECIMAL):10}\t"
            # f"{round(utilization, DECIMAL):10}\t"
            # f"{round(speedup, DECIMAL):10}\t"
            # f"{round(availability * utilization * speedup, DECIMAL)}\t"
            # f"{sum(kpi_calc.model_abbr.values())}\n"
            # f"{kpi_calc.model_abbr}\n"
            # f"{sum(kpi_calc.rule_count.values())}\n"
            # f"{kpi_calc.rule_count}\n"
        )
