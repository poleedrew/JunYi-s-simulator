import os, json, math, copy
from datetime import datetime, timedelta
from dateutil.parser import parse
from collections import defaultdict
from env.utils.utils import str2datetime
DECIMAL = 2

class KPI:
    def __init__(self, result_path, total_days=3):
        self.result_path = result_path
        self.total_days = total_days

        self.history_output = 0 # before the date actual output
        self.gap_product = [defaultdict(int) for _ in range(total_days)]
                
        with open(result_path, 'r') as f:
            self.result = json.load(f)

    def load_plan(self, plan_dir, date):
        gap_path = os.path.join(plan_dir, date, "gap.json")
        remainder_path = os.path.join(plan_dir, date, "remainder.json")
        
        with open(gap_path, 'r') as fp:
            self.gap = json.load(fp)
        
        with open(remainder_path, 'r') as fp:
            self.remainder = json.load(fp)

        date = datetime.strptime(date, "%Y-%m-%d")
        cnt = [0 for _ in range(self.total_days)]
        for mfg, product_list in self.gap.items():
            mfg = datetime.strptime(mfg, "%Y-%m-%d %H:%M:%S.0")
            for i in range(self.total_days):
                if mfg > date + timedelta(days=i):
                    continue
                for product_code, num_sheet in product_list.items():
                    if num_sheet <= 0:
                        continue
                    if i > 0 and product_code in self.remainder.keys() and self.remainder[product_code] > 0:
                        if self.remainder[product_code] > num_sheet:
                            num_sheet = 0
                        else:
                            num_sheet -= self.remainder[product_code]
                    self.gap_product[i][product_code] += num_sheet
                    cnt[i] += num_sheet
        print(self.gap_product)
        print(cnt)
                
                
            


if __name__ == '__main__':
    
    tasks = [
        {"name": "2022-12-28", "start": "2022-12-28 08:05:29"},
    ]
    result_dir = "../AUO-Simulator/simulator/agent/Baseline_Rollout/result/eval"

    plan_dir = "../AUO_data/plan/auo_data"
                
    tasks = [
        {"name": "2023-03-25", "start": "2023-03-25 08:05:11"},
    ]
    result_path = "agent/REINFORCE/result/1_setup/round_9/" + tasks[0]['name'] + '.json'
    kpi_calc = KPI(result_path)
    kpi_calc.load_plan(plan_dir, tasks[0]['name'])
    