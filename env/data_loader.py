import yaml, os, math
from datetime import datetime, timedelta
from dateutil.parser import parse
import numpy as np
import statistics, csv, bisect
from collections import defaultdict, Counter
from tqdm import tqdm
from env.utils.sheet import ODFSheet
from env.utils.wip import ODFWIP


class AUO_Dataloader:
    def __init__(self, system_start_time_dt=None,
                 config_path='./env/config.yml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        if system_start_time_dt is None:
            self.system_start_time_dt = datetime.strptime(
                self.config['system_time']['start'], '%Y-%m-%d %H:%M:%S')
        else:
            self.system_start_time_dt = system_start_time_dt
        # sheet list and wip list
        self.sheet_list = []
        self.wip_list = []
        self.current_wait_wip_list = []
        self.current_run_wip_info_list = []
        # sheet info list and wip info list
        self.sheet_info_list = []
        self.wip_info_list = []
        self.current_wait_wip_info_list = []
        self.current_run_wip_list = []
        self.wip_groupby_model_abbr = defaultdict(list)
        self.model_no2product_code = {}
        self.tact_time_table_dict = defaultdict(int)
        self.tact_time_imputed_table_dict = defaultdict(int)

        self.dummy_cassette_ids = {} ## for the loadsheet
    def load_AUO_data(
            self,
            wip_table_path,
            eqp_group_table_path,
            process_flow_table_path,
            op2eqp_table_path,
            qtime_table_path,
            pc2model_table_path,
            op_history_path,
            tact_time_table_path,
            subroutine_table_path,
            future_table_path = None):
        self._load_pc2model_table(pc2model_table_path)
        self._load_qtime_table(qtime_table_path)
        self._load_eqp_group_table(eqp_group_table_path)
        self._load_process_flow_table(process_flow_table_path)
        self._load_op2eqp_table(op2eqp_table_path)
        self._load_subroutine_table(subroutine_table_path)
        self.load_sheets(wip_table_path,future_table_path, op_history_path)
        
        self._load_tact_time_table(tact_time_table_path)
        self.group_into_wip()
        self._group_into_model_abbr()
        self.sheet_info_list = [sheet.to_json() for sheet in self.sheet_list]
        self.wip_info_list = [wip.to_json() for wip in self.wip_list]
        
        self.current_wait_wip_info_list = [wip.to_json() for wip in self.current_wait_wip_list]
        self.current_run_wip_info_list = [wip.to_json() for wip in self.current_run_wip_list]

    def _load_subroutine_table(self, subroutine_table):
        self.op_id2_back = {}
        with open(subroutine_table, newline='') as csvfile:
            rows = csv.reader(csvfile)
            for i, row in enumerate(tqdm(rows, desc="load pc2model table")):
                if i == 0:
                    continue
                op_id, op_seq_back, op_id_back, _ = row
                self.op_id2_back[op_id] = (op_seq_back, op_id_back)


    def _cassette_id_generator(self, sheet_info):
        index = sheet_info["abbr_no"]+sheet_info["op_id"]+sheet_info["route_id"]
        for key in self.dummy_cassette_ids.keys():
            if key == index:
                return "DUM" +'{0:04}'.format(self.dummy_cassette_ids[index])
        values = list(self.dummy_cassette_ids.values())
        if values != []:
            idx = max(values) + 1
            self.dummy_cassette_ids[index] = idx
            return "DUM" +'{0:04}'.format(idx)
        else:
            self.dummy_cassette_ids[index] = 0
            return "DUM" +'{0:04}'.format(0)

    def _load_pc2model_table(self, pc2model_table):
        with open(pc2model_table, newline='') as csvfile:
            rows = csv.reader(csvfile)
            # for i, row in enumerate(rows):
            for i, row in enumerate(tqdm(rows, desc="load pc2model table")):
                if i == 0:
                    continue
                model_no, abbr_no, product_code, part_no, main_route_id, main_route_ver, _ = row
                assert model_no[:2] == 'MD', f"The first two char of model_no should be MD, but get {model_no}"
                assert product_code[:2] == 'PC', f"The first two char of product_code should be PC, but get {product_code}"
                self.model_no2product_code[model_no] = product_code

    def _load_qtime_table(self, qtime_table_path):
        self.qtime_table = {}
        with open(qtime_table_path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            # for i, row in enumerate(rows):
            for i, row in enumerate(tqdm(rows, desc="load qtime table")):
                if i == 0:
                    continue
                main_route_id, _, route_id, _, op_seq_start, op_seq_end, qtime_type, qtime, _ = row
                op_seq_start = int(op_seq_start)
                op_seq_end = int(op_seq_end)
                qtime = int(qtime)
                assert route_id[:2] == 'RT', \
                    f"the first two char of route_id should be RT, but get {route_id}"
                assert op_seq_start < op_seq_end
                assert qtime_type == 'MIN' or qtime_type == 'MAX', \
                    f"queue time type should be MIN or MAX, but get {qtime_type}"
                self.qtime_table[(qtime_type, route_id, op_seq_start, op_seq_end)] = qtime

    def _load_eqp_group_table(self, path):
        # eqp_group_id -> [eqp_id, eqp_id, ...]
        self.eqp_group_table = defaultdict(list)
        with open(path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            # for i, row in enumerate(rows):
            for i, row in enumerate(tqdm(rows, desc="load eqp group table")):
                if i == 0:
                    continue
                eqp_group_id, eqp_id, _ = row
                assert eqp_group_id[:
                                    4] == 'EQPG', f"The first four char of eqp_group_id should be EQPG, but get {eqp_group_id}"
                self.eqp_group_table[eqp_group_id].append(eqp_id)

    def _load_process_flow_table(self, path):
        self.process_flow_table = defaultdict(list)
        with open(path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            # for i, row in enumerate(rows):
            for i, row in enumerate(tqdm(rows, desc="load process flow table")):
                if i == 0:
                    continue
                # main_route_id, _, route_id, _, op_id, op_ver, op_seq, stage_id = row
                main_route_id, _, route_id, _, op_id, op_ver, op_seq, stage_id, _ = row
                op_seq = int(op_seq)
                assert route_id[:
                                2] == 'RT', f"The first two char of route_id should be RT, but get {route_id}"
                bisect.insort(
                    self.process_flow_table[route_id], (op_seq, stage_id, op_id, op_ver))

    def _load_op2eqp_table(self, path):
        self.op2eqp_table = {}
        # ROUTE_ID	ROUTE_VER	OP_ID	OP_VER	EQP_GROUP_ID	STAGE_ID
        with open(path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            # for i, row in enumerate(rows):
            for i, row in enumerate(tqdm(rows, desc="load op2eqp table")):
                if i == 0:
                    continue
                route_id, _, op_id, op_ver, eqp_group_id, stage_id, _ = row
                assert route_id[:
                                2] == 'RT', f"The first two char of route_id should be RT, but get {route_id}"
                assert eqp_group_id[:
                                    4] == 'EQPG', f"The first four char of eqp_group_id should be EQPG, but get {eqp_group_id}"
                self.op2eqp_table[(route_id, op_id)] = eqp_group_id
                self.op2eqp_table[(op_id, op_ver)] = eqp_group_id

    def load_sheets(self, wip_table_path,future_table_path, op_history_path):
        self.wip_table = []
        self._load_wip_table(wip_table_path)
        if future_table_path != None:
            self._load_wip_table(future_table_path)
        self._load_op_history(op_history_path)
        self._load_op_history2sheets()
        self.current_op = 'ODF'
        for sheet_info in self.wip_table:
            # assert 'product_code' in sheet_info
            sheet_id = sheet_info['sheet_id']
            sheet_status = sheet_info['sheet_status']
            model_no = sheet_info['model_no']
            abbr_no = sheet_info['abbr_no']
            abbr_cat = sheet_info['abbr_cat']
            main_route_id = sheet_info['main_route_id']
            route_id = sheet_info['route_id']
            stage_id = sheet_info['stage_id']
            op_id = sheet_info['op_id']
            process_flow = self.process_flow_table[route_id]
            if op_id == 'GAP':
                continue
            if stage_id == 'ODF':
                sheet_info['on_ODF'] = True
            else:
                sheet_info['on_ODF'] = False
            start = False
            
            if sheet_info['on_ODF']:
                sheet_info['arrival_time'] = sheet_info['pre_logoff_time']
            else:
                sheet_info['arrival_time'] = self.system_start_time_dt
            process_time, cycle_time = self._get_history_time(sheet_info)
            sheet_info['process_time'] = process_time
            sheet_info['cycle_time'] = cycle_time

            for i, (pf_op_seq, pf_stage_id, pf_op_id, pf_op_ver) in enumerate(process_flow):
                if pf_stage_id in ('PIRPA', 'PI_AOI', 'PA_AOI'):
                    process_time, cycle_time = self._get_history_time(sheet_info)
                    sheet_info['process_time'] = process_time
                    sheet_info['cycle_time'] = cycle_time
                    if op_id in ('PI_REPAIR_T', 'PI_REPAIR_CF'):
                        sheet_info['stage_id'] = 'PIRPA'
                        op_seq_back, op_id_back = self.op_id2_back[op_id]
                        next_stage_id = 'ODF'
                    elif op_id in ('PI_AOI_T', 'PI_AOI_C', 'PI_ACC'):
                        sheet_info['stage_id'] = 'PI_AOI'
                        op_seq_back, op_id_back = self.op_id2_back[op_id]
                        if op_id == 'PI_ACC' and abbr_cat == 'TFT':
                            op_seq_back, op_id_back = 3000, 'ODF_TFT'
                        if op_id == 'PI_ACC' and abbr_cat == 'CF':
                            op_seq_back, op_id_back = 3200, 'ODF_CF' 
                        next_stage_id = 'ODF'
                    elif op_id in ('PA_AOI_T', 'PA_AOI_C'):
                        sheet_info['stage_id'] = 'PA_AOI'
                        op_seq_back, op_id_back = self.op_id2_back[op_id]
                        next_stage_id = 'ODF'
                    elif op_id in ('PI_PRE_AOI_T', 'PI_PRE_AOI_C'):
                        sheet_info['stage_id'] = 'PI_AOI'
                        op_seq_back, op_id_back = self.op_id2_back[op_id]
                        next_stage_id = 'PI'
                    else:
                        break
                    assert pf_stage_id == sheet_info['stage_id'], f"pf_stage_id: {pf_stage_id}, sheet_info['stage_id']: {sheet_info['stage_id']}"
                    
                    if sheet_status in ('WAIT', 'HOLD'):
                        sheet_info['arrival_time'] += max(
                            cycle_time - (sheet_info['arrival_time'] - sheet_info['receive_time']), 
                            process_time)
                        sheet_info['sheet_status'] = 'WAIT'
                    elif sheet_status in ('RUN'):
                        sheet_info['arrival_time'] += max(
                            cycle_time - (sheet_info['arrival_time'] - sheet_info['pre_logoff_time']),
                            process_time
                        )
                        sheet_info['sheet_status'] = 'WAIT'
                    
                    # find main stand op_seq
                    flow = self.process_flow_table[main_route_id]
                    for i, (flow_op_seq, flow_stage_id, _, _) in enumerate(flow):
                        if flow_stage_id == stage_id:
                            prev_op_seq = flow_op_seq
                            break
                    sheet_info['stage_id'] = next_stage_id
                    sheet_info['op_id'] = op_id_back
                    sheet_info['route_id'] = main_route_id
            route_id = sheet_info['route_id']
            process_flow = self.process_flow_table[route_id]        

            for i, (pf_op_seq, pf_stage_id, pf_op_id, pf_op_ver) in enumerate(process_flow):
                if op_id == pf_op_id:
                    start = True
                if not start:
                    continue
                # print(f"\t{sheet_id}\t{sheet_info['op_id']}\t{sheet_info['arrival_time']}")
                
                if pf_stage_id in ('PI', 'PA'):
                    process_time, cycle_time = self._get_history_time(sheet_info)
                    sheet_info['process_time'] = process_time
                    sheet_info['cycle_time'] = cycle_time
                    assert pf_stage_id == sheet_info['stage_id'], f"pf_stage_id: {pf_stage_id}, sheet_info['stage_id']: {sheet_info['stage_id']}"
                    _, next_stage_id, next_op_id, _ = process_flow[i + 1]
                    if sheet_status == 'RUN':
                        sheet_info['arrival_time'] += max(
                            process_time - (sheet_info['arrival_time'] - sheet_info['logon_time']), timedelta(seconds=0))
                        sheet_info['sheet_status'] = 'WAIT'
                        # print(sheet_info)
                        sheet_info['cassette_id'] = self._cassette_id_generator(sheet_info)
                        # print(sheet_info['cassette_id'])
                    if sheet_status in ('WAIT', 'HOLD'):
                        if sheet_status == 'WAIT' and pf_stage_id == 'PI':
                            sheet_info['arrival_time'] += max(
                                cycle_time - (sheet_info['arrival_time'] - sheet_info['receive_time']), 
                                process_time)
                        else:
                            sheet_info['arrival_time'] += max(
                                cycle_time - (sheet_info['arrival_time'] - sheet_info['pre_logoff_time']), 
                                process_time)
                        sheet_info['sheet_status'] = 'WAIT'
                    sheet_info['stage_id'] = next_stage_id
                    sheet_info['op_id'] = next_op_id
                if pf_stage_id == 'ODF':
                    assert pf_stage_id == sheet_info[
                        'stage_id'], f"pf_stage_id: {pf_stage_id}, sheet_info['stage_id']: {sheet_info['stage_id']}"
                    process_time, cycle_time = self._get_history_time(sheet_info)
                    sheet_info['process_time'] = process_time
                    sheet_info['cycle_time'] = cycle_time
                    prev_op_seq, _, _, _ = process_flow[i - 1]
                    min_qtime = self.qtime_table.get(('MIN', route_id, prev_op_seq, pf_op_seq), 0)
                    max_qtime = self.qtime_table.get(('MAX', route_id, prev_op_seq, pf_op_seq), 60*24*100)
                    min_qtime_td = timedelta(minutes=min_qtime)
                    max_qtime_td = timedelta(minutes=max_qtime)
                    assert min_qtime_td <= max_qtime_td, f"min qtime should earlier than max qtime, key: {(route_id, prev_op_seq, pf_op_seq)}, min qtime:{min_qtime_td}, max qtime: {max_qtime_td}"
                    if sheet_info['min_qtime'] == '':
                        sheet_info['min_qtime'] = sheet_info['arrival_time'] + min_qtime_td
                    if sheet_info['max_qtime'] == '':
                        sheet_info['max_qtime'] = sheet_info['arrival_time'] + max_qtime_td
                    self.sheet_info_list.append(sheet_info)
                    break
        for sheet_info in self.sheet_info_list:
            assert sheet_info['stage_id'] == 'ODF', sheet_info
            self.sheet_list.append(ODFSheet(sheet_info))

    def _load_wip_table(self, path):
        # Notice!!! although it called "wip table", each row of wip table is a
        # sheet
        # self.wip_table = []
        subroutine_list = ('PI_REPAIR_T','PI_REPAIR_C','PI_AOI_T','PI_AOI_C','PA_AOI_T','PA_AOI_C', 'PI_PRE_AOI_T', 'PI_PRE_AOI_C', 'PI_ACC')
        with open(path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            # for i, row in enumerate(rows):
            for i, row in enumerate(tqdm(rows, desc="load WIP table")):
                if i == 0:
                    continue
                # SHEET_ID	SHEET_STATUS	SHEET_TYPE	
                # MODEL_NO	ABBR_NO	ABBR_CAT	
                # EQP_ID	MAIN_ROUTE_ID	MAIN_ROUTE_VER	
                # ROUTE_ID	ROUTE_VER	ROUTE_TYPE	
                # OP_ID	STAGE_ID	OP_VER	OP_SEQ	
                # EQP_GROUP_ID	CASSETTE_ID	SHEET_PRIORITY	
                # RECEIVE_TIME	PRE_LOGOFF_TIME	LOGON_TIME	MIN_QT_OP_SEQ_START	MIN_QT_OP_SEQ_END	
                # MIN_QT_END_TIME	MAX_QT_OP_SEQ_START	MAX_QT_OP_SEQ_END	MAX_QT_END_TIME	SYS_DATE
                assert len(row) == 30, f"row {i} has {len(row)} values to unpack"
                sheet_id, sheet_status, sheet_type, \
                    model_no, abbr_no, abbr_cat, \
                    eqp_id, main_route_id, main_route_ver, \
                    route_id, route_ver, route_type, \
                    op_id, stage_id, op_ver, op_seq, \
                    eqp_group_id, cassette_id, sheet_priority, \
                    receive_time, pre_logoff_time, logon_time, _, _, \
                    min_qt_end_time, _, _, max_qt_end_time, sys_date, _ = row

                if sheet_status == 'COMP' or op_id == '':
                    continue
                assert sheet_id[:
                                2] == 'ST', f"The first two char of sheet_id should be ST, but get {sheet_id}  at row{i}"
                assert model_no[:
                                2] == 'MD', f"The first two char of model_no should be MD, but get {model_no}  at row{i}"
                assert abbr_cat == 'TFT' or abbr_cat == 'CF', f"abbr_cat should be TFT or CF, but get {abbr_cat}  at row{i}"
                assert route_id[:
                                2] == 'RT', f"The first two char of route_id should be RT, but get {route_id}  at row{i}"
                assert eqp_group_id[:
                                    4] == 'EQPG', f"The first three char of eqp_group_id should be EQPG, but get {eqp_group_id} at row{i}"
                if abbr_cat not in self.config['available_abbr_cat']:
                    continue
                if sheet_status not in self.config['available_sheet_status']:
                    continue
                if stage_id in ('PA_TFT', 'PA_CF'):
                    stage_id = 'PA'
                if (model_no, abbr_no) == ('MD0543', '43T068Q03_RU1H'):
                    continue
                if (model_no, abbr_no) == ('MD0465', '24T32AHR1_R221'):
                    continue
                if pre_logoff_time == '' or logon_time == '':
                    pre_logoff_time_dt = self.system_start_time_dt
                    logon_time_dt = self.system_start_time_dt
                else:
                    pre_logoff_time_dt = parse(pre_logoff_time)
                    logon_time_dt = parse(logon_time)
                if sheet_status == 'RUN':
                    assert pre_logoff_time_dt <= logon_time_dt, f"row{i}, pre_logoff_time_dt:{pre_logoff_time_dt}, logon_time_dt:{logon_time_dt}"
                if sheet_status in ('WAIT', 'HOLD'):
                    assert pre_logoff_time_dt >= logon_time_dt, f"row{i}, pre_logoff_time_dt:{pre_logoff_time_dt}, logon_time_dt:{logon_time_dt}"
                if model_no == '' and abbr_no == '':
                    continue
                min_qt_end_time_dt = ''
                max_qt_end_time_dt = ''
                if stage_id == 'ODF' or op_id in subroutine_list:
                    if min_qt_end_time != '':
                        min_qt_end_time_dt = parse(min_qt_end_time)
                    else:
                        min_qt_end_time_dt = ''
                    if max_qt_end_time != '':
                        max_qt_end_time_dt = parse(max_qt_end_time)
                    else:
                        max_qt_end_time_dt = ''
                receive_time_dt = parse(receive_time)
                sheet_info = {
                    'sheet_id': sheet_id,
                    'sheet_status': sheet_status,
                    'model_no': model_no,
                    'abbr_no': abbr_no,
                    'abbr_cat': abbr_cat,
                    'main_route_id': main_route_id,
                    'route_id': route_id,
                    'original_op_id': op_id,
                    'op_id': op_id,
                    'original_stage_id': stage_id,
                    'stage_id': stage_id,
                    'op_seq': op_seq,
                    'pre_logoff_time': pre_logoff_time_dt,
                    'logoff_time': pre_logoff_time_dt,
                    'logon_time': logon_time_dt,
                    'eqp_group_id': eqp_group_id,
                    'cassette_id': cassette_id,
                    'receive_time': receive_time_dt,
                    'min_qtime': min_qt_end_time_dt,
                    'max_qtime': max_qt_end_time_dt,
                }
                if sheet_status == 'RUN':
                    sheet_info['selected_eqp_id'] = eqp_id
                self.wip_table.append(sheet_info)
        # print('len(wip_table):', len(wip_table))
        return self.wip_table

    def _load_op_history(self, path):
        # every row of op history is a sheet
        self.op_history = []
        with open(path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            # for i, row in enumerate(rows):
            for i, row in enumerate(tqdm(rows, desc="load op history table")):
                if i == 0:
                    continue
                mfg_day, model_no, abbr_no, product_code, \
                    route_id, route_ver, op_id, op_ver, op_seq, eqp_id, \
                    cassette_id, unload_cassette_id, \
                    logon_time, logoff_chip_qty, logoff_time, op_process_time, op_cycle_time, count_ast, _ = row
                assert model_no[:2] == 'MD', f"The first two char of model_no should be MD, but get {model_no}"
                assert product_code[:2] == 'PC', f"The first two char of product_code should be PC, but get {product_code}"
                assert route_id[:2] == 'RT', f"The first two char of route_id should be RT, but get {route_id}"
                logon_time_dt = parse(logon_time)
                logoff_time_dt = parse(logoff_time)
                count_ast = int(count_ast)

                op_process_time = int(op_process_time)
                op_cycle_time = int(op_cycle_time)
                assert op_process_time <= op_cycle_time, 'process time should greater or equal to cycle time, process time:%d sec, cycle time:%d sec' % (
                    op_process_time, op_cycle_time)
                hist = {
                    # 'mfg_day':                mfg_day,
                    'model_no': model_no,
                    'abbr_no': abbr_no,
                    'product_code': product_code,
                    'route_id': route_id,
                    'op_id': op_id,
                    'op_seq': op_seq,
                    'eqp_id': eqp_id,
                    'hist_unload_cassette_id': unload_cassette_id,
                    'logon_time': logon_time_dt,
                    'logoff_time': logoff_time_dt,
                    'op_process_time': op_process_time,
                    'op_cycle_time': op_cycle_time,
                    'count_ast': count_ast,
                }
                self.op_history.append(hist)

    def get_estimated_time(self, history_times):
        estimated_time = defaultdict(float)
        for key, time_dict in history_times.items():
            counter = Counter(time_dict)
            estimated_time[key] = statistics.median(counter.elements())
            # estimated_time[key] = counter.most_common(1)[0][0]
        # pprint(f"estimated_time: {estimated_time}")
        return estimated_time

    def _load_op_history2sheets(self):
        # print("_load_op_history2sheets")
        self.history_process_times = defaultdict(dict)
        self.history_cycle_times = defaultdict(dict)

        for hist in tqdm(self.op_history, desc="estimate process time & cycle time"):
            # print(f"{i} / {len(self.op_history)}")
            product_code = hist['product_code']
            model_no = hist['model_no']
            abbr_no = hist['abbr_no']
            op_id = hist['op_id']
            process_time = hist['op_process_time']
            cycle_time = hist['op_cycle_time']
            if process_time not in self.history_process_times[(model_no, abbr_no, op_id)].keys():
                self.history_process_times[(model_no, abbr_no, op_id)][process_time] = hist['count_ast']
            else:
                self.history_process_times[(model_no, abbr_no, op_id)][process_time] += hist['count_ast']
            if process_time not in self.history_process_times[(product_code, op_id)].keys():
                self.history_process_times[(product_code, op_id)][process_time] = hist['count_ast']
            else:
                self.history_process_times[(product_code, op_id)][process_time] += hist['count_ast']
            if cycle_time not in self.history_cycle_times[(model_no, abbr_no, op_id)].keys():
                self.history_cycle_times[(model_no, abbr_no, op_id)][cycle_time] = hist['count_ast']
            else:
                self.history_cycle_times[(model_no, abbr_no, op_id)][cycle_time] += hist['count_ast']
            if cycle_time not in self.history_cycle_times[(product_code, op_id)].keys():
                self.history_cycle_times[(product_code, op_id)][cycle_time] = hist['count_ast']
            else:
                self.history_cycle_times[(product_code, op_id)][cycle_time] += hist['count_ast']

        self.estimated_process_times = self.get_estimated_time(self.history_process_times)
        self.estimated_cycle_times = self.get_estimated_time(self.history_cycle_times)

        for sheet_info in self.wip_table:
            model_no = sheet_info['model_no']
            abbr_no = sheet_info['abbr_no']
            product_code = self.model_no2product_code[model_no]
            sheet_info['product_code'] = product_code
        self.estimated_process_times[('PC0039', 'ODF_TFT')] = 46896.5
        self.estimated_cycle_times[('PC0039', 'ODF_TFT')] = 59534.5
        self.estimated_process_times[('PC0033', 'PI RWK_T')] = 8843
        self.estimated_cycle_times[('PC0033', 'PI RWK_T')] = 8843
        self.estimated_process_times[('PC0068', 'PI PRINT_TFT')] = 8191
        self.estimated_cycle_times[('PC0068', 'PI PRINT_TFT')] = 8191
        self.estimated_process_times[('PC0068', 'ODF_TFT')] = 18444
        self.estimated_cycle_times[('PC0068', 'ODF_TFT')] = 104955
        # from 2022-07-31 13_FEOL_CHIP_ODS.csv
        self.estimated_process_times[('PC0086', 'PA_TFT')] = 12402
        # from 2022-07-31 13_FEOL_CHIP_ODS.csv
        self.estimated_cycle_times[('PC0086', 'PA_TFT')] = 47323
        # from 2022-08-01 13_FEOL_CHIP_ODS.csv
        self.estimated_process_times[('PC0086', 'ODF_TFT')] = 19010
        # from 2022-08-01 13_FEOL_CHIP_ODS.csv
        self.estimated_cycle_times[('PC0086', 'ODF_TFT')] = 43169
        # from 2022-08-09 13_FEOL_CHIP_ODS.csv
        self.estimated_process_times[('PC0065', 'ODF_TFT')] = 19136
        # from 2022-08-09 13_FEOL_CHIP_ODS.csv
        self.estimated_cycle_times[('PC0065', 'ODF_TFT')] = 61195
        # from 2022-07-30 13_FEOL_CHIP_ODS.csv
        self.estimated_process_times[('PC0065', 'PSA')] = 9876
        # from 2022-07-30 13_FEOL_CHIP_ODS.csv
        self.estimated_cycle_times[('PC0065', 'PSA')] = 45627
        # from 2022-05-19 13_FEOL_CHIP_ODS.csv
        self.estimated_process_times[('PC0077', 'ODF_TFT')] = 14605
        # from 2022-05-19 13_FEOL_CHIP_ODS.csv
        self.estimated_cycle_times[('PC0077', 'ODF_TFT')] = 86136
        # from 2022-08-09 13_FEOL_CHIP_ODS.csv
        self.estimated_process_times[('PC0069', 'ODF_TFT')] = 9026
        # from 2022-08-09 13_FEOL_CHIP_ODS.csv
        self.estimated_cycle_times[('PC0069', 'ODF_TFT')] = 9026
        # from 2022-08-12 13_FEOL_CHIP_ODS.csv
        self.estimated_process_times[('PC0069', 'ODF_TFT')] = 29008
        # from 2022-08-12 13_FEOL_CHIP_ODS.csv
        self.estimated_cycle_times[('PC0069', 'ODF_TFT')] = 163736
        # from 2022-08-13 13_FEOL_CHIP_ODS.csv
        self.estimated_process_times[('PC0069', 'PSA')] = 14293
        # from 2022-08-13 13_FEOL_CHIP_ODS.csv
        self.estimated_cycle_times[('PC0069', 'PSA')] = 48496
        # from 2022-08-19 13_FEOL_CHIP_ODS.csv
        self.estimated_process_times[('PC0075', 'PI PRINT_TFT')] = 7601
        # from 2022-08-19 13_FEOL_CHIP_ODS.csv
        self.estimated_cycle_times[('PC0075', 'PI PRINT_TFT')] = 7694.5
        # from 2022-08-21 13_FEOL_CHIP_ODS.csv
        self.estimated_process_times[('PC0075', 'ODF_TFT')] = 17436
        # from 2022-08-21 13_FEOL_CHIP_ODS.csv
        self.estimated_cycle_times[('PC0075', 'ODF_TFT')] = 100725
        # from 2022-08-13 13_FEOL_CHIP_ODS.csv
        self.estimated_process_times[('PC0075', 'PSA')] = 9973
        # from 2022-08-13 13_FEOL_CHIP_ODS.csv
        self.estimated_cycle_times[('PC0075', 'PSA')] = 52341
        self.estimated_process_times[('PC0075', 'PI_AOI_T')] = 0
        self.estimated_cycle_times[('PC0075', 'PI_AOI_T')] = 0
        # pprint(self.history_process_times)
        # pprint(self.history_cycle_times)

    def _get_history_time(self, sheet_info):
        model_no = sheet_info['model_no']
        abbr_no = sheet_info['abbr_no']
        op_id = sheet_info['op_id']
        product_code = sheet_info['product_code']
        if (model_no, abbr_no, op_id) in self.estimated_process_times:
            process_time = self.estimated_process_times[(
                model_no, abbr_no, op_id)]
        elif (product_code, op_id) in self.estimated_process_times:
            process_time = self.estimated_process_times[(product_code, op_id)]
        else:
            process_time = np.median(list(self.estimated_process_times.values()))
            # assert False, f"history process time not found, model_no: {model_no}, abbr_no: {abbr_no}, op_id: {op_id} product_code: {product_code}"
        if (model_no, abbr_no, op_id) in self.estimated_cycle_times:
            cycle_time = self.estimated_cycle_times[(model_no, abbr_no, op_id)]
        elif (product_code, op_id) in self.estimated_cycle_times:
            cycle_time = self.estimated_cycle_times[(product_code, op_id)]
        else:
            cycle_time = np.median(list(self.estimated_cycle_times.values()))
            # assert False, f"history cycle time not found, model_no: {model_no}, abbr_no: {abbr_no}, op_id: {op_id} product_code: {product_code}"
        assert process_time <= cycle_time, f"process time should less than cycle time, process_time:{process_time}, cycle_time:{cycle_time}"
        return timedelta(seconds=process_time), timedelta(seconds=cycle_time)

    def _load_tact_time_table(self, path):
        # load tact time from op history
        self.tact_time_table = {}
        op_history_sortby_logoff_time = sorted(
            self.op_history, key=lambda hist: hist['logoff_time'])
        op_history_groupby_pc_eqpid = defaultdict(list)
        switch_cassette = False
        num_sheet_in_cassette = 0
        for i, hist in enumerate(op_history_sortby_logoff_time):
            product_code, op_id, eqp_id = hist['product_code'], hist['op_id'], hist['eqp_id']
            # if op_id not in ('ODF_TFT'):
            #     continue
            if (product_code, eqp_id) == ('PC0085', 'CKODF100'):
                continue
            op_history_groupby_pc_eqpid[(product_code, eqp_id)].append(hist)
        # for (product_code, eqp_id), hist_list in op_history_groupby_pc_eqpid.items():
        for (product_code, eqp_id), hist_list in tqdm(op_history_groupby_pc_eqpid.items(), desc="load tact time from op history"):
            num_sheets = 0
            tact_time_list = []
            for (i, hist) in enumerate(hist_list):
                num_sheets += hist['count_ast']
                if i == 0:
                    prev_hist = hist
                    continue
                if hist['hist_unload_cassette_id'] != prev_hist['hist_unload_cassette_id']:
                    logoff_time_dt = hist['logoff_time']
                    prev_logoff_time_dt = prev_hist['logoff_time']
                    assert prev_logoff_time_dt <= logoff_time_dt, f"prev_logoff_time_dt should earlier than logoff_time_dt after sorting"
                    tact_time_td = (
                        logoff_time_dt - prev_logoff_time_dt) / num_sheets
                    tact_time = tact_time_td.total_seconds()
                    tact_time_list.append(tact_time)
                    # print(f"\t({product_code}, {eqp_id}): {tact_time_td.total_seconds()} \t {num_sheets} \t {hist['hist_unload_cassette_id']} \t {prev_hist['hist_unload_cassette_id']} \t {prev_logoff_time_dt} \t {logoff_time_dt}")
                    num_sheets = hist['count_ast']
                prev_hist = hist
            # assert len(tact_time_list) > 0, f"The length of {product_code}, {eqp_id} should greater than tact_time_list"
            if len(tact_time_list) <= 0:
                continue
            # self.tact_time_table[(product_code, eqp_id)] = np.median(tact_time_list) *2
            self.tact_time_table[(product_code, eqp_id)] = 120.0 if np.median(tact_time_list)*2 > 120 else np.median(tact_time_list)*2
            self.tact_time_table[(product_code, eqp_id)] = 76.0 if np.median(tact_time_list)*2 < 76  else np.median(tact_time_list)*2

            if product_code == 'PC0084' and eqp_id == 'CKODF500':
                self.tact_time_table[(product_code, eqp_id)] = 80
        # print(self.tact_time_table)
        # print("\tself.tact_time_table")
        # pprint(self.tact_time_table)
        # load tact time from imputed tact time table
        eqp_id_list = []
        # (product_code, eqp_id) -> tact_time
        self.tact_time_imputed_table = {}
        tact_time_table_pc = {}
        with open(path, newline='', encoding='iso-8859-1') as csvfile:
            rows = csv.reader(csvfile)
            for i, row in enumerate(rows):
                if i == 0:
                    # eqp_id_list: [CKODF100	CKODF200	CKODF300	CKODF400]
                    _, _, *eqp_id_list = row
                else:
                    # _, product_code, _, *(eqp_tact_times) = row
                    product_code, _, *(eqp_tact_times) = row
                    eqp_tact_times = [int(time) if time != '' else 0 for time in eqp_tact_times]
                    assert product_code[:2] == 'PC', f"The first two char of product_code should be PC, but get {product_code}"
                    assert eqp_tact_times != [0, 0, 0, 0], 'tact time should not be all zero'
                    tact_time_table_pc[product_code] = eqp_tact_times
                    for i, eqp_id in enumerate(eqp_id_list):
                        self.tact_time_imputed_table[product_code,eqp_id] = eqp_tact_times[i]
        # print("\tself.tact_time_imputed_table:")
        # pprint(self.tact_time_imputed_table)

    def _get_tact_times(self, route_id, op_id, product_code, num_sheets, on_odf=True):
        tact_times = {}
        if on_odf:
            eqp_group_id = self.op2eqp_table[(route_id, op_id)]
            for eqp_id in self.eqp_group_table[eqp_group_id]:
                if (product_code, eqp_id) in self.tact_time_table and not math.isnan(self.tact_time_table[(product_code, eqp_id)]):
                    tact_times[eqp_id] = self.tact_time_table[(product_code, eqp_id)]
                    self.tact_time_table_dict[product_code] += 1
                else:
                    tact_times[eqp_id] = self.tact_time_imputed_table[(product_code, eqp_id)]
                    self.tact_time_imputed_table_dict[product_code] += 1
            tact_times = dict(sorted(tact_times.items(), key=lambda x: x[0]))
            return tact_times
        else:
            process_flow = self.process_flow_table[route_id]
            start = False
            for i, (pf_op_seq, pf_stage_id, pf_op_id, pf_op_ver) in enumerate(process_flow):
                if op_id == pf_op_id:
                    start = True
                    eqp_group_id = self.op2eqp_table[(pf_op_id, pf_op_ver)]
                    break
            for eqp_id in self.eqp_group_table[eqp_group_id]:
                if (product_code, eqp_id) in self.tact_time_table and not math.isnan(self.tact_time_table[(product_code, eqp_id)]):
                    tact_times[eqp_id] = self.tact_time_table[(product_code, eqp_id)]
                    self.tact_time_table_dict[product_code] += 1
                else:
                    tact_times[eqp_id] = self.tact_time_imputed_table[(product_code, eqp_id)]
                    self.tact_time_imputed_table_dict[product_code] += 1
            tact_times = dict(sorted(tact_times.items(), key=lambda x: x[0]))
            return tact_times

    def _index_by_fields(self, sheet_info_list, field_names):
        # print('sheet_info_list:', sheet_info_list)
        sheet_count = 0
        wip_groups = defaultdict(list)
        for sheet_info in sheet_info_list:
            if 'grouped' in sheet_info and sheet_info['grouped']:
                continue
            sheet_count += 1
            sheet_info['grouped'] = True
            key = tuple([sheet_info[name] for name in field_names])
            wip_groups[key].append(sheet_info)
        num_sheet_info_in_wip = 0
        for _, sheet_info_list_in_wip in wip_groups.items():
            num_sheet_info_in_wip += len(sheet_info_list_in_wip)
        assert num_sheet_info_in_wip == sheet_count, f"num_sheet_info_in_wip:{num_sheet_info_in_wip}, sheet_count:{sheet_count}"
        return wip_groups

    def group_into_wip(self):
        odf_wait_sheet_info_list = []
        for sheet_info in self.sheet_info_list:
            if sheet_info['stage_id'] == 'ODF' and sheet_info['sheet_status'] in (
                    'WAIT', 'HOLD'):
                odf_wait_sheet_info_list.append(sheet_info)
        # print('len(odf_wait_sheet_info_list):', len(odf_wait_sheet_info_list))
        # assert all([sheet_info['cassette_id'] != '' for sheet_info in odf_wait_sheet_info_list])
        wip_groups = self._index_by_fields(
            odf_wait_sheet_info_list, ('abbr_cat', 'cassette_id', 'route_id', 'op_id'))
        for sheet_info_list_in_wip in wip_groups.values():
            
            for sheet_info in sheet_info_list_in_wip:
                if sheet_info['sheet_status'] == 'HOLD' and sheet_info['sheet_status'] != sheet_info_list_in_wip[0]['sheet_status']:
                    sheet_info['sheet_status'] = sheet_info_list_in_wip[0]['sheet_status']
            assert all([sheet_info_list_in_wip[0]['sheet_status'] == sheet_info['sheet_status']
                       for sheet_info in sheet_info_list_in_wip]), "the 'sheet_status' in a WIP must be the same"
            assert all([sheet_info_list_in_wip[0]['model_no'] == sheet_info['model_no'] for sheet_info in sheet_info_list_in_wip]
                       ), f"the 'model_no' in a WIP must be the same, {[(sheet_info['model_no'], sheet_info['cassette_id']) for sheet_info in sheet_info_list_in_wip]}"
            assert all([sheet_info_list_in_wip[0]['abbr_no'] == sheet_info['abbr_no']
                       for sheet_info in sheet_info_list_in_wip]), "the 'abbr_no' in a WIP must be the same"
            assert all([sheet_info_list_in_wip[0]['abbr_cat'] == sheet_info['abbr_cat']
                       for sheet_info in sheet_info_list_in_wip]), "the 'abbr_cat' in a WIP must be the same"
            assert all([sheet_info_list_in_wip[0]['route_id'] == sheet_info['route_id']
                       for sheet_info in sheet_info_list_in_wip]), "the 'route_id' in a WIP must be the same"
            assert all([sheet_info_list_in_wip[0]['main_route_id'] == sheet_info['main_route_id']
                       for sheet_info in sheet_info_list_in_wip]), "the 'main_route_id' in a WIP must be the same"
            assert all([sheet_info_list_in_wip[0]['op_id'] == sheet_info['op_id']
                       for sheet_info in sheet_info_list_in_wip]), "the 'op_id' in a WIP must be the same"
            assert all([sheet_info_list_in_wip[0]['stage_id'] == sheet_info['stage_id']
                       for sheet_info in sheet_info_list_in_wip]), "the 'stage_id' in a WIP must be the same"
            assert all([sheet_info_list_in_wip[0]['on_ODF'] == sheet_info['on_ODF']
                       for sheet_info in sheet_info_list_in_wip]), ""

            sheet_status = sheet_info_list_in_wip[0]['sheet_status']
            model_no = sheet_info_list_in_wip[0]['model_no']
            abbr_no = sheet_info_list_in_wip[0]['abbr_no']
            abbr_cat = sheet_info_list_in_wip[0]['abbr_cat']
            main_route_id = sheet_info_list_in_wip[0]['main_route_id']
            route_id = sheet_info_list_in_wip[0]['route_id']
            original_op_id = sheet_info_list_in_wip[0]['original_op_id']
            original_stage_id = sheet_info_list_in_wip[0]['original_stage_id']
            op_id = sheet_info_list_in_wip[0]['op_id']
            stage_id = sheet_info_list_in_wip[0]['stage_id']
            cassette_id = sheet_info_list_in_wip[0]['cassette_id']
            product_code = sheet_info_list_in_wip[0]['product_code']
            on_ODF = sheet_info_list_in_wip[0]['on_ODF']
            tact_times = self._get_tact_times(
                route_id, op_id, product_code, len(sheet_info_list_in_wip), on_ODF)
            # print('\ttact_times:', tact_times)
            
            wip_config = {
                'sheet_status': sheet_status,
                'model_no': model_no,
                'abbr_no': abbr_no,
                'abbr_cat': abbr_cat,
                'min_qtime': None,
                'max_qtime': None,
                'tact_time': tact_times,
                'cassette_id': cassette_id,
                'main_route_id': main_route_id,
                'route_id': route_id,
                'original_op_id': original_op_id,
                'original_stage_id': original_stage_id,
                'op_id': op_id,
                'stage_id': stage_id,
                'product_code': product_code,
                'on_ODF': on_ODF
            }
            wip = ODFWIP(wip_config)
            wip.sheet_list = [ODFSheet(sheet_info)
                              for sheet_info in sheet_info_list_in_wip]
            self.wip_list.append(wip)
            self.current_wait_wip_list.append(wip)
        num_sheet_in_wip = 0
        for wip in self.wip_list:
            if wip.stage_id == 'ODF' and wip.sheet_status in ('WAIT', 'HOLD'):
                num_sheet_in_wip += len(wip.sheet_list)
        assert num_sheet_in_wip == len(
            odf_wait_sheet_info_list), f"num_sheet_in_wip:{num_sheet_in_wip}, len(odf_wait_sheet_info_list):{len(odf_wait_sheet_info_list)}"

        # grouping ODF RUN sheet into wip
        odf_run_sheet_info_list = []
        for sheet_info in self.sheet_info_list:
            if sheet_info['stage_id'] == 'ODF' and sheet_info['sheet_status'] == 'RUN':
                odf_run_sheet_info_list.append(sheet_info)
        # wip_groups
        # {
        #   ('TFT', 'MD0681', '31T52JD01_RM35F', 'RT2617', 'ODF_TFT'): [sheet, sheet, sheet, ...],
        #   ('TFT', 'MD0656', '32T18CDR1_VM01', 'RT2617', 'ODF_TFT'):  [sheet, sheet, sheet, ...], ...
        # }
        wip_groups = self._index_by_fields(
            odf_run_sheet_info_list,
            ('abbr_cat',
             'model_no',
             'abbr_no',
             'route_id',
             'op_id',
             'selected_eqp_id'))
        for fields, sheet_list in wip_groups.items():
            sheet_list.sort(key=lambda sheet_info: sheet_info['logon_time'])
        cassette_size = self.config['sheet_grouping']['cassette_size']
        time_interval = self.config['sheet_grouping']['time_interval']
        run_wips = []
        # sheet_list: [sheet, sheet, sheet, ...]
        for fields, sheet_list in wip_groups.items():
            assert all([sheet_list[0]['abbr_cat'] == sheet_info['abbr_cat']
                       for sheet_info in sheet_list]), 'the abbr_cat in a WIP must be the same'
            assert all([sheet_list[0]['selected_eqp_id'] == sheet_info['selected_eqp_id'] for sheet_info in sheet_list]
                       ), f"the selected eqp_id in a WIP must be the same {[sheet_info['selected_eqp_id'] for sheet_info in sheet_list]}"
            assert all([sheet_list[0]['model_no'] == sheet_info['model_no']
                       for sheet_info in sheet_list]), 'the model_no in a WIP must be the same'
            assert all([sheet_list[0]['abbr_no'] == sheet_info['abbr_no']
                       for sheet_info in sheet_list]), 'the abbr_no in a WIP must be the same'
            assert all([sheet_list[0]['route_id'] == sheet_info['route_id']
                       for sheet_info in sheet_list]), 'the route_id in a WIP must be the same'
            assert all([sheet_list[0]['op_id'] == sheet_info['op_id']
                       for sheet_info in sheet_list]), 'the op_id in a WIP must be the same'
            sheet_status = sheet_list[0]['sheet_status']
            model_no = sheet_list[0]['model_no']
            abbr_no = sheet_list[0]['abbr_no']
            abbr_cat = sheet_list[0]['abbr_cat']
            selected_eqp_id = sheet_list[0]['selected_eqp_id']
            route_id = sheet_list[0]['route_id']
            original_op_id = sheet_list[0]['original_op_id']
            original_stage_id = sheet_list[0]['original_stage_id']
            op_id = sheet_list[0]['op_id']
            stage_id = sheet_list[0]['stage_id']
            cassette_id = sheet_list[0]['cassette_id']
            product_code = sheet_list[0]['product_code']
            on_ODF = sheet_list[0]['on_ODF']

            tact_times = self._get_tact_times(route_id, op_id, product_code, len(sheet_list), on_ODF)
            wip_config = {
                'sheet_status': sheet_status,
                'model_no': model_no,
                'abbr_no': abbr_no,
                'abbr_cat': abbr_cat,
                'selected_eqp_id': selected_eqp_id,
                'min_qtime': None,
                'max_qtime': None,
                'tact_time': tact_times,
                'cassette_id': 'CST_RUN',
                'main_route_id': main_route_id,
                'route_id': route_id,
                'original_op_id': original_op_id,
                'original_stage_id': original_stage_id,
                'op_id': op_id,
                'stage_id': stage_id,
                'product_code': product_code,
                'on_ODF': on_ODF
            }

            left = 0
            right = 0
            # wip_group: [sheet, sheet, sheet, ...]
            for _ in sheet_list:
                wip_time_span = sheet_list[right]['logon_time'] - \
                    sheet_list[left]['logon_time']
                if wip_time_span <= timedelta(
                        seconds=time_interval) and right - left <= cassette_size:
                    right += 1
                else:
                    right += 1
                    wip = ODFWIP(wip_config)
                    # print('wip.sheet_status:', wip.sheet_status)
                    wip.sheet_list = [ODFSheet(sheet_info)
                                      for sheet_info in sheet_list[left:right]]
                    self.current_run_wip_list.append(wip)
                    self.wip_list.append(wip)
                    left = right
            if left != right:
                wip = ODFWIP(wip_config)
                # print('wip.sheet_status:', wip.sheet_status)
                wip.sheet_list = [ODFSheet(sheet_info)
                                for sheet_info in sheet_list[left:right]]
                self.wip_list.append(wip)
                self.current_run_wip_list.append(wip)
        num_sheet_in_wip = 0
        for wip in self.wip_list:
            if wip.sheet_status != 'RUN':
                continue
            num_sheet_in_wip += len(wip.sheet_list)
        assert num_sheet_in_wip == len(
            odf_run_sheet_info_list), f"num_sheet_in_wip:{num_sheet_in_wip}, len(odf_run_sheet_info_list):{len(odf_run_sheet_info_list)}"

    def _load_input_target_table(self, path):
        with open(path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            for i, row in enumerate(rows):
                if i == 0:
                    continue
                mfg_day, product_code, _, _, target, _ = row
                target = float(target)
                assert product_code[:
                                    2] == 'PC', f"The first two char of product_code should be PC, but get {product_code} at row{i}"

    def _group_into_model_abbr(self):
        for wip in self.wip_list:
            self.wip_groupby_model_abbr[(
                wip.model_no, wip.abbr_no)].append(wip)