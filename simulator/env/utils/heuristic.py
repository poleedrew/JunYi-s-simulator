import csv
from collections import defaultdict
from datetime import timedelta
from datetime import datetime
from pprint import pprint
from dateutil.parser import parse


class Rule:
    def __init__(self):
        self.name = 'Rule'

    def get_candidate_job_list(self, wip_info_list, machines, avai_time_dt):
        candidate_wip_info_list = []
        for wip_info in wip_info_list:
            if wip_info['done']:
                continue
            if wip_info['min_qtime'] <= avai_time_dt:
                candidate_wip_info_list.append(wip_info)
        if len(candidate_wip_info_list) == 0:
            earliest_min_qtime = self.next_available_time(wip_info_list)
            candidate_wip_info_list = self.get_candidate_job_list(
                wip_info_list, machines, earliest_min_qtime + timedelta(hours=4))
        return candidate_wip_info_list

    def next_available_time(self, wip_info_list):
        earliest_min_qtime = datetime.strptime(
            '9999/12/31 00:00', '%Y/%m/%d %H:%M')
        for wip_info in wip_info_list:
            if wip_info['done']:
                continue
            if earliest_min_qtime > wip_info['min_qtime']:
                earliest_min_qtime = wip_info['min_qtime']
        return earliest_min_qtime

class FIFO_model_abbr_FIFO_WIP(Rule):
    def __init__(self):
        super().__init__()
        self.name = 'FIFO_model_abbr_FIFO_WIP'
        self.wip_info_list = []
        self.fifo = FIFO_WIP()
        self.inf_dt = datetime.strptime('9999/12/31 00:00', '%Y/%m/%d %H:%M')

    def group_by_model_abbr(self, wip_info_list):
        wip_info_list_model_abbr = defaultdict(list)
        for wip_info in wip_info_list:
            # if wip_info['done']:
            #     continue
            model_no, abbr_no = wip_info['model_no'], wip_info['abbr_no']
            wip_info_list_model_abbr[(model_no, abbr_no)].append(wip_info)
        return wip_info_list_model_abbr

    def _get_earliest_arrival(self, wip_info_list):
        earliest_arrival = self.inf_dt
        for wip_info in wip_info_list:
            earliest_arrival = min(earliest_arrival, wip_info['arrival_time'])
        return earliest_arrival

    def all_done(self, wip_info_list):
        for wip_info in wip_info_list:
            if not wip_info['done']:
                return False
        return True

    def select_model_abbr(self, wip_info_list_model_abbr):
        selected_model_abbr = None
        selected_value = self.inf_dt
        for (model_no, abbr_no), wip_info_list in wip_info_list_model_abbr.items():
            if self.all_done(wip_info_list):
                continue
            earliest_arrival = self._get_earliest_arrival(wip_info_list)
            if selected_value > earliest_arrival:
                selected_model_abbr = (model_no, abbr_no)
                selected_value = earliest_arrival
        return selected_model_abbr

    def __call__(self, wip_info_list, machines, current_time_dt, verbose=False):
        candidate_wip_info_list = self.get_candidate_job_list(wip_info_list, machines, current_time_dt)
        wip_info_list_model_abbr = self.group_by_model_abbr(candidate_wip_info_list)
        model_no, abbr_no = self.select_model_abbr(wip_info_list_model_abbr)
        return self.fifo(wip_info_list_model_abbr[(model_no, abbr_no)], machines, current_time_dt)

class SPT_model_abbr_FIFO_WIP(Rule):
    def __init__(self):
        super().__init__()
        self.name = 'SPT_model_abbr_FIFO_WIP'
        self.wip_info_list = []
        self.fifo = FIFO_WIP()

    def group_by_model_abbr(self, wip_info_list):
        wip_info_list_model_abbr = defaultdict(list)
        for wip_info in wip_info_list:
            if wip_info['done']:
                continue
            model_no, abbr_no = wip_info['model_no'], wip_info['abbr_no']
            wip_info_list_model_abbr[(model_no, abbr_no)].append(wip_info)
        return wip_info_list_model_abbr

    def _get_capacity_sum(self, wip_info_list):
        capacity_sum = 0
        for wip_info in wip_info_list:
            capacity_sum += sum(wip_info['capacity'].values()) / len(wip_info['capacity'])
        return capacity_sum

    def select_model_abbr(self, wip_info_list_model_abbr):
        spt_model_abbr = None
        spt = 1 << 30
        for (model_no, abbr_no), wip_info_list in wip_info_list_model_abbr.items():
            capacity_sum = self._get_capacity_sum(wip_info_list)
            if spt > capacity_sum:
                spt_model_abbr = (model_no, abbr_no)
                spt = capacity_sum
        return spt_model_abbr

    def __call__(self, wip_info_list, machines, current_time_dt, verbose=False):
        candidate_wip_info_list = self.get_candidate_job_list(wip_info_list, machines, current_time_dt)
        wip_info_list_model_abbr = self.group_by_model_abbr(candidate_wip_info_list)
        model_no, abbr_no = self.select_model_abbr(wip_info_list_model_abbr)
        return self.fifo(wip_info_list_model_abbr[(model_no, abbr_no)], machines, current_time_dt)
        

class SPT_WIP(Rule):
    def __init__(self):
        super().__init__()
        self.name = 'SPT_WIP'

    def __call__(self, wip_info_list, machines, current_time_dt, verbose=False):
        candidate_wip_info_list = self.get_candidate_job_list(wip_info_list, machines, current_time_dt)
        if verbose:
            for wip_info in candidate_wip_info_list:
                print(f"\t\tcassette_id: {wip_info['cassette_id']}"
                    f"\tarrival_time: {wip_info['arrival_time']}"
                    f"\tmin_qtime: {wip_info['min_qtime']}"
                    f"\tmax_qtime: {wip_info['max_qtime']}"
                    f"\tcapacity: {wip_info['capacity']}")
        return min(candidate_wip_info_list, 
                   key=lambda wip_info: sum(wip_info['capacity'].values()) / len(wip_info['capacity']))


class LPT_WIP(Rule):
    def __init__(self):
        super().__init__()
        self.name = 'LPT_WIP'

    def __call__(self, wip_info_list, machines, current_time_dt, verbose=False):
        candidate_wip_info_list = self.get_candidate_job_list(wip_info_list, machines, current_time_dt)
        if verbose:
            for wip_info in candidate_wip_info_list:
                print(f"\t\tcassette_id: {wip_info['cassette_id']}"
                    f"\tarrival_time: {wip_info['arrival_time']}"
                    f"\tmin_qtime: {wip_info['min_qtime']}"
                    f"\tmax_qtime: {wip_info['max_qtime']}"
                    f"\tcapacity: {wip_info['capacity']}")
        return max(candidate_wip_info_list,
                    key=lambda wip_info: sum(wip_info['capacity'].values()) / len(wip_info['capacity']))


class EMinQ_WIP(Rule):
    def __init__(self):
        super().__init__()
        self.name = 'EMinQ_WIP'

    def __call__(self, wip_info_list, machines, current_time_dt, verbose=False):
        candidate_wip_info_list = self.get_candidate_job_list(wip_info_list, machines, current_time_dt)
        if verbose:
            for wip_info in candidate_wip_info_list:
                print(f"\t\tcassette_id: {wip_info['cassette_id']}"
                    f"\tarrival_time: {wip_info['arrival_time']}"
                    f"\tmin_qtime: {wip_info['min_qtime']}"
                    f"\tmax_qtime: {wip_info['max_qtime']}"
                    f"\tcapacity: {wip_info['capacity']}")
        return min(candidate_wip_info_list,
                    key=lambda wip_info: wip_info['min_qtime'])

class EMaxQ_WIP(Rule):
    def __init__(self):
        super().__init__()
        self.name = 'EMaxQ_WIP'

    def __call__(self, wip_info_list, machines, current_time_dt, verbose=False):
        candidate_wip_info_list = self.get_candidate_job_list(wip_info_list, machines, current_time_dt)
        if verbose:
            for wip_info in candidate_wip_info_list:
                print(f"\t\tcassette_id: {wip_info['cassette_id']}"
                    f"\tarrival_time: {wip_info['arrival_time']}"
                    f"\tmin_qtime: {wip_info['min_qtime']}"
                    f"\tmax_qtime: {wip_info['max_qtime']}"
                    f"\tcapacity: {wip_info['capacity']}")
        return min(candidate_wip_info_list,
                    key=lambda wip_info: wip_info['max_qtime'])


class FIFO_WIP(Rule):
    def __init__(self):
        super().__init__()
        self.name = 'FIFO_WIP'

    def __call__(self, wip_info_list, machines, current_time_dt, verbose=False):
        candidate_wip_info_list = self.get_candidate_job_list(wip_info_list, machines, current_time_dt)
        if verbose:
            for wip_info in candidate_wip_info_list:
                print(f"\t\tcassette_id: {wip_info['cassette_id']}"
                    f"\tarrival_time: {wip_info['arrival_time']}"
                    f"\tmin_qtime: {wip_info['min_qtime']}"
                    f"\tmax_qtime: {wip_info['max_qtime']}"
                    f"\tcapacity: {wip_info['capacity']}")
        return min(candidate_wip_info_list,
                    key=lambda wip_info: wip_info['arrival_time'])

class EDD_WIP(Rule):
    def __init__(self, gap, tact_time_path):
        super().__init__()
        self.name = 'EDD_WIP'
        self.gap = gap
        # print(f"\t{self.gap}")
        self.load_tact_time_imputed_table(tact_time_path)
        self.fifo = FIFO_WIP()
    
    def group_by_pc(self, wip_info_list):
        wip_info_list_pc = defaultdict(list)
        for wip_info in wip_info_list:
            if wip_info['done']:
                continue
            product_code = wip_info['product_code']
            wip_info_list_pc[product_code].append(wip_info)
        return wip_info_list_pc

    def update_gap(self):
        exceed_output = {}
        for mfg_day_dt, diff_dict in self.gap.items():
            for pc, exceed in exceed_output.items():
                if pc not in self.gap[mfg_day_dt]:
                    continue
                self.gap[mfg_day_dt][pc] += exceed
                exceed_output[pc] = 0
            for pc, diff in diff_dict.items():
                if diff <= 0:
                    exceed_output[pc] = diff
                    diff_dict[pc] = 0

    def __call__(self, wip_info_list, machines, current_time_dt, verbose=False):
        candidate_wip_info_list = self.get_candidate_job_list(wip_info_list, machines, current_time_dt)
        candidate_wip_info_list_pc = self.group_by_pc(candidate_wip_info_list)
        # print(f"\t\tEDD, current_time_dt: {current_time_dt}\tcandidate: {[(wip_info['cassette_id'], wip_info['product_code']) for wip_info in candidate_wip_info_list]}")
        inf_dt = datetime.strptime('9999/12/31 00:00', '%Y/%m/%d %H:%M')
        for mfg_day, diff_dict in self.gap.items():
            mfg_day_dt = parse(mfg_day)
            selected_pc = max(diff_dict, key=lambda pc: diff_dict[pc] * self.tact_time_imputed_table[pc])
            # selected_pc = max(diff_dict, key=lambda pc: diff_dict[pc])
            # print(f"selected_pc: {selected_pc}")
            selected_wip_info = None
            
            if len(candidate_wip_info_list_pc[selected_pc]) == 0:
                # print("\tno avai WIP")
                continue
            else:
                selected_wip_info = self.fifo(candidate_wip_info_list_pc[selected_pc], machines, current_time_dt)
                self.gap[mfg_day][selected_pc] -= selected_wip_info['size']
                break
        if selected_wip_info == None:
            selected_wip_info = self.fifo(candidate_wip_info_list, machines, current_time_dt)
            self.gap[mfg_day][selected_pc] -= selected_wip_info['size']
        self.update_gap()
        if verbose:
            print("\tcandidate_wip_info_list")
            for wip_info in candidate_wip_info_list:
                print(f"\t\tcassette_id: {wip_info['cassette_id']}\t"
                      f"{wip_info['product_code']}\t")
            print("\tcandidate_wip_info_list_pc")
            for wip_info in candidate_wip_info_list_pc[selected_pc]:
                print(f"\t\tcassette_id: {wip_info['cassette_id']}"
                      f"\tselected_pc: {selected_pc}"
                    f"\tarrival_time: {wip_info['arrival_time']}"
                    f"\tmin_qtime: {wip_info['min_qtime']}"
                    f"\tmax_qtime: {wip_info['max_qtime']}"
                    f"\tcapacity: {wip_info['capacity']}")
            pprint(self.gap)
        return selected_wip_info

    def load_tact_time_imputed_table(self, path):
        eqp_id_list = []
        # (product_code, eqp_id) -> tact_time
        self.tact_time_imputed_table = {}
        with open(path, newline='', encoding='iso-8859-1') as csvfile:
            rows = csv.reader(csvfile)
            for i, row in enumerate(rows):
                if i == 0:
                    # eqp_id_list: [CKODF100	CKODF200	CKODF300	CKODF400]
                    _, _, _, *eqp_id_list = row
                else:
                    # _, product_code, _, *(eqp_tact_times) = row
                    product_code, _, *(eqp_tact_times) = row
                    eqp_tact_times = [
                        int(time) if time != '' else 0 for time in eqp_tact_times]
                    assert product_code[:
                                        2] == 'PC', f"The first two char of product_code should be PC, but get {product_code}"
                    assert eqp_tact_times != [
                        0, 0, 0, 0], "tact time should not be all zero"
                    self.tact_time_imputed_table[product_code] = sum(
                        eqp_tact_times) / len(eqp_tact_times)

class EDD_WIP_v2(Rule):
    def __init__(self, gap):
        super().__init__()
        self.name = 'EDD_WIP_v2'
        self.gap = gap
        satisfied_mfg_day = []
        for mfg_day, diff_dict in self.gap.items():
            satisfied_pc = []
            for pc, exceed in diff_dict.items():
                if exceed == 0:
                    satisfied_pc.append(pc)
            for pc in satisfied_pc:
                del diff_dict[pc]

            if diff_dict == {}:
                satisfied_mfg_day.append(mfg_day)
        for date in satisfied_mfg_day:
            del self.gap[date]
        
        # self.fifo = FIFO_WIP()
        self.fifo = EMaxQ_WIP()
    
    def group_by_pc(self, wip_info_list):
        wip_info_list_pc = defaultdict(list)
        for wip_info in wip_info_list:
            if wip_info['done']:
                continue
            product_code = wip_info['product_code']
            wip_info_list_pc[product_code].append(wip_info)
        return wip_info_list_pc

    def update_gap(self):
        exceed_output = {}
        for mfg_day_dt, diff_dict in self.gap.items():
            for pc, exceed in exceed_output.items():
                if pc not in self.gap[mfg_day_dt]:
                    continue
                self.gap[mfg_day_dt][pc] += exceed
                exceed_output[pc] = 0
            for pc, diff in diff_dict.items():
                if diff <= 0:
                    exceed_output[pc] = diff
                    diff_dict[pc] = 0

    def __call__(self, wip_info_list, machines, current_time_dt, verbose=False):
        candidate_wip_info_list = self.get_candidate_job_list(wip_info_list, machines, current_time_dt)
        candidate_wip_info_list_pc = self.group_by_pc(candidate_wip_info_list)
        inf_dt = datetime.strptime('9999/12/31 00:00', '%Y/%m/%d %H:%M')
        for mfg_day, diff_dict in self.gap.items():
            needs = False
            for pc, exceed in diff_dict.items():
                if exceed != 0:
                    needs = True
                    break
            if not needs:
                continue
            mfg_day_dt = parse(mfg_day)
            # selected_pc_list = sorted(diff_dict, key=lambda pc: diff_dict[pc] * self.tact_time_imputed_table[pc], reverse=True)
            selected_pc_list = sorted(diff_dict, key=lambda pc: diff_dict[pc], reverse=True)
            selected_pc = selected_pc_list[0]
            for pc in selected_pc_list:
                if len(candidate_wip_info_list_pc[pc]) == 0:
                    continue
                selected_pc = pc
            selected_wip_info = None
            
            if len(candidate_wip_info_list_pc[selected_pc]) == 0:
                continue
            else:
                selected_wip_info = self.fifo(candidate_wip_info_list_pc[selected_pc], machines, current_time_dt)
                selected_pc = selected_wip_info['product_code']
                if selected_wip_info['product_code'] in self.gap[mfg_day].keys():
                    self.gap[mfg_day][selected_pc] -= selected_wip_info['size']
                break
        if selected_wip_info == None:
            selected_wip_info = self.fifo(candidate_wip_info_list, machines, current_time_dt)
            selected_pc = selected_wip_info['product_code']
            if selected_wip_info['product_code'] in self.gap[mfg_day].keys():
                self.gap[mfg_day][selected_pc] -= selected_wip_info['size']
        self.update_gap()
        if verbose:
            print("\tcandidate_wip_info_list")
            for wip_info in candidate_wip_info_list:
                print(f"\t\tcassette_id: {wip_info['cassette_id']}\t"
                      f"{wip_info['product_code']}\t")
            print("\tcandidate_wip_info_list_pc")
            for wip_info in candidate_wip_info_list_pc[selected_pc]:
                print(f"\t\tcassette_id: {wip_info['cassette_id']}"
                      f"\tselected_pc: {selected_pc}"
                    f"\tarrival_time: {wip_info['arrival_time']}"
                    f"\tmin_qtime: {wip_info['min_qtime']}"
                    f"\tmax_qtime: {wip_info['max_qtime']}"
                    f"\tcapacity: {wip_info['capacity']}")
            pprint(self.gap)
        return selected_wip_info