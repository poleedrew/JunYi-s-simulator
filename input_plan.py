from collections import defaultdict
import os, csv, json, copy
from dateutil.parser import parse
from datetime import datetime

class InputPlan:
    def __init__(self, start):
        self.start_dt = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        self.expected_output = defaultdict(int)
        self.actual_output = defaultdict(int)

    def load_data(self, input_target_path, input_kpi_path, input_wip_path, task_name):
        self.task_name = parse(task_name)
        self._load_input_target_table(input_target_path)
        self._load_input_kpi_table(input_kpi_path)

    def _load_input_target_table(self, path):
        with open(path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            for i, row in enumerate(rows):
                if i == 0:
                    continue
                mfg_day, product_code, part_no, target_type, target, _ = row
                assert product_code[:
                                    2] == 'PC', f"The first two char of product_code should be PC, but get {product_code}"
                mfg_day_dt = parse(mfg_day)
                target = int(target)
                if self.start_dt.year == mfg_day_dt.year:
                    if self.start_dt.month > mfg_day_dt.month:
                        continue
                self.expected_output[product_code, mfg_day] += target

    def _load_input_kpi_table(self, path):
        with open(path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            for i, row in enumerate(rows):
                if i == 0:
                    continue
                mfg_day, product_code, part_no, model_no, abbr_no, qty, _ = row
                assert product_code[:
                                    2] == 'PC', f"The first two char of product_code should be PC, but get {product_code}"
                assert model_no[:
                                2] == 'MD', f"The first two char of model_no should be MD, but get {model_no}  at row{i}"
                mfg_day_dt = parse(mfg_day)
                qty = int(qty)
                if self.start_dt.year == mfg_day_dt.year:
                    if self.start_dt.month > mfg_day_dt.month:
                        continue
                self.actual_output[product_code, mfg_day] += qty

    def calculate_gap(self):
        expected_output = defaultdict(lambda: defaultdict(int))
        for key, tgt in self.expected_output.items():
            pc, mfg_day = key
            expected_output[mfg_day][pc] = tgt
        expected_output = dict(
            sorted(
                expected_output.items(), 
                key=lambda item: parse(item[0])))
        self.gap = copy.deepcopy(expected_output)
        actual_output = defaultdict(int)
        for key, num in self.actual_output.items():
            pc, mfg_day = key
            actual_output[pc] += num

        for mfg_day, diff_dict in expected_output.items():
            for pc, num in diff_dict.items():
                if self.start_dt >= parse(mfg_day):
                    if num >= actual_output[pc]:
                        self.gap[mfg_day][pc] -= actual_output[pc]
                        actual_output[pc] = 0
                    else:
                        actual_output[pc] -= num
                        self.gap[mfg_day][pc] = 0
        self.remainder = actual_output

    def export_data(self, name):
        export_dir = os.path.join("./plan/auo_data", name)
        if not os.path.exists(export_dir):
            os.mkdir(export_dir)
        actual_output = defaultdict(lambda: defaultdict(int))
        expected_output = defaultdict(lambda: defaultdict(int))

        for key, tgt in self.actual_output.items():
            pc, mfg_day = key
            actual_output[mfg_day][pc] = tgt
        actual_output = dict(
            sorted(
                actual_output.items(),
                key=lambda item: parse(
                    item[0])))
        with open(os.path.join(export_dir, "actual_output.json"), 'w') as fp:
            json.dump(actual_output, fp, indent=4)

        for key, qty in self.expected_output.items():
            pc, mfg_day = key
            expected_output[mfg_day][pc] = qty
        expected_output = dict(
            sorted(
                expected_output.items(),
                key=lambda item: parse(
                    item[0])))
        with open(os.path.join(export_dir, "expected_output.json"), 'w') as fp:
            json.dump(expected_output, fp, indent=4)

        with open(os.path.join(export_dir, "gap.json"), 'w') as fp:
            json.dump(self.gap, fp, indent=4)
            
        with open(os.path.join(export_dir, "remainder.json"), 'w') as fp:
            json.dump(self.remainder, fp, indent=4)

    def sat_rate(self, product_code):
        return self.actual_output[product_code] / \
            self.expected_output[product_code]


if __name__ == "__main__":
    tasks = []
    with open("train_dates/all.json") as file:
        tasks = json.load(file)
    
    data_root_dir = "../AUO_data"
    for item in tasks:
        start, task_name = item['start'], item['name']
        input_target_path = os.path.join(data_root_dir, task_name, "11_INPUT_TARGET.csv")
        input_kpi_path = os.path.join(data_root_dir, task_name, "16_INPUT_KPI.csv")
        input_wip_path = os.path.join(data_root_dir, task_name, "1_WIP.csv")
        plan = InputPlan(start)
        plan.load_data(input_target_path, input_kpi_path, input_wip_path, task_name)
        plan.calculate_gap()
        plan.export_data(task_name)
