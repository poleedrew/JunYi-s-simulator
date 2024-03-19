
import os, csv, random
from datetime import datetime, timedelta
from collections import defaultdict
from operator import itemgetter

class AUO_Future_loader:
    def __init__(self, root_dir, system_start_time_dt, future_day=3+1):
        self.root_dir = root_dir
        self.sys_time = system_start_time_dt
        self.future_day = future_day
        self.date = datetime.strftime(system_start_time_dt, '%Y-%m-%d')
        self.future_date = datetime.strftime(system_start_time_dt + timedelta(days=future_day), '%Y-%m-%d')
        self.future_wip = defaultdict(dict)
        path = os.path.join(self.root_dir, self.future_date, "13_FEOL_CHIP_ODS.csv")
        if os.path.exists(path):
            self._load_future_wip(path)
        self.cassette_cnt = 0
        self.cassette_id = 'CGI_cassette'+ '{0:04}'.format(self.cassette_cnt)
        self.sheet_cnt = 0
        self.sheet_id = 'ST_CGI_sheet'+ '{0:04}'.format(self.sheet_cnt)
    def _load_future_wip(self, path):
        random.seed(128)
        print(path)
        with open(path, "r", newline="") as csvfile:
            reader = csv.reader(csvfile)
            for idx, row in enumerate(reader):
                if idx == 0:
                    continue
                mfg_day, model_no, abbr_no ,product_code, route_id ,route_ver, \
                op_id, op_ver, op_seq, eqp_id, cassette_id, unload_cassette_id, \
                logon_time, logoff_chip_qty, logoff_time, op_process_time, op_cycle_time, count, _ = row
                
                logon_time = datetime.strptime(logon_time, "%Y-%m-%d %H:%M:%S")
                
                # Step 1-1 leave the data with op_id == "PI_PRINT_TFT"
                if op_id != "PI PRINT_TFT":
                    continue
                # Step 1-2 leave the data with logon_time >= sys_time 
                if logon_time < self.sys_time:
                    continue

                # Step 1-3 classify the data into 4 group by the logon_time
                tmp_date = datetime.strptime(self.date + " 07:30:00", "%Y-%m-%d %H:%M:%S")
                for j in range(self.future_day):
                    index = model_no +" "+ abbr_no
                    p1_date = tmp_date + timedelta(hours=6)
                    p2_date = tmp_date + timedelta(hours=12)
                    p3_date = tmp_date + timedelta(hours=18)
                    p4_date = tmp_date + timedelta(hours=24)
                    
                    if tmp_date <= logon_time and logon_time < p1_date:
                        if self.future_wip[str(tmp_date)+'~'+str(p1_date)].get(index) == None: 
                            self.future_wip[str(tmp_date)+'~'+str(p1_date)][index] = int(count)
                        else:
                            self.future_wip[str(tmp_date)+'~'+str(p1_date)][index] += int(count) 
                    if p1_date <= logon_time and logon_time < p2_date:
                        if self.future_wip[str(p1_date)+'~'+str(p2_date)].get(index) == None: 
                            self.future_wip[str(p1_date)+'~'+str(p2_date)][index] = int(count)
                        else:
                            self.future_wip[str(p1_date)+'~'+str(p2_date)][index] += int(count)
                    if p2_date <= logon_time and logon_time < p3_date:
                        if self.future_wip[str(p2_date)+'~'+str(p3_date)].get(index) == None: 
                            self.future_wip[str(p2_date)+'~'+str(p3_date)][index] = int(count)
                        else:
                            self.future_wip[str(p2_date)+'~'+str(p3_date)][index] += int(count)
                    if p3_date <= logon_time and logon_time < p4_date:
                        if self.future_wip[str(p3_date)+'~'+str(p4_date)].get(index) == None: 
                            self.future_wip[str(p3_date)+'~'+str(p4_date)][index] = int(count)
                        else:
                            self.future_wip[str(p3_date)+'~'+str(p4_date)][index] += int(count)
                    tmp_date += timedelta(hours=24)

    def export_future_table(self, path):
        # Step 1-4 export the table based on the date, time and model-abbr
        with open(path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            fields = ['expected_future date', 'received time']
            for i in self.future_wip.keys():
                # print(str(i))
                segment = str(i)
                date = str(i).split('~')[0].split()[0]
                for j in self.future_wip[i].keys():
                    if j not in fields:
                        fields.append(j)
            writer.writerow(fields)
            datas = []
            for i in self.future_wip.keys():
                segment = str(i)
                date = str(i).split('~')[0].split()[0]
                data = []
                data.append(date)
                data.append(segment)
                for j in range(2, len(fields)):
                    # print(fields[j])
                    # print(self.future_wip[i].keys())
                    if fields[j] not in self.future_wip[i].keys():
                        data.append(0)
                    else:
                        data.append(self.future_wip[i][fields[j]])
                datas.append(data)
            ## sort the data
            datas = sorted(datas, key= itemgetter(1, 2))
            for i in datas:
                writer.writerow(i)

    def sheet_id_generator(self):
        self.sheet_cnt += 1
        return 'ST_CGI_sheet'+ '{0:04}'.format(self.sheet_cnt)
    def cassette_id_generator(self):
        self.cassette_cnt += 1
        return 'CGI_cassette'+ '{0:04}'.format(self.cassette_cnt)

    def _read_product_to_model(self, path, model, abbr):
        with open(path, 'r' , newline='') as file:
            rows = csv.reader(file)
            for i, row in enumerate(rows):
                if i == 0:
                    continue
                model_no, abbr_no, product_code, part_no, main_route_id, main_route_ver, _ = row
                if model_no == model and abbr == abbr_no:
                    return main_route_id, main_route_ver

    def _read_process_flow(self, path, main_route, op):
        with open(path, 'r' , newline='') as file:
            rows = csv.reader(file)
            for i, row in enumerate(rows):
                if i == 0:
                    continue
                main_route_id, _, route_id, _, op_id, op_ver, op_seq, stage_id, _ = row
                if main_route == main_route_id and op == op_id:
                    return stage_id, op_ver, op_seq

    def _read_op2eqp_table(self, path, main_route, op):
        with open(path, 'r', newline='') as file:
            rows = csv.reader(file)
            for i, row in enumerate(rows):
                if i == 0:
                    continue
                route_id, _, op_id, op_ver, eqp_group_id, stage_id, _ = row
                if main_route == route_id and op == op_id:
                    return eqp_group_id

    def export(self, path):
        with open(path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            fields = 'sheet_id sheet_status sheet_type model_no abbr_no abbr_cat eqp_id main_route_id main_route_ver route_id route_ver route_type op_id stage_id op_ver op_seq eqp_group_id cassette_id sheet_priority receive_time pre_logoff_time logon_time min_qt_op_seq_start min_qt_op_seq_end min_qt_end_time max_qt_op_seq_start max_qt_op_seq_end max_qt_end_time sys_date'.split(' ')
            writer.writerow(fields)
            for date in self.future_wip.keys():
                data = ['', 'WAIT', 'PROD', '','','TFT', '', '', '', '', '', 'N','PI PRINT_TFT', '', '', '', '', '', 'NOML','','','','','','','','','','','']
                
                for model_abbr in self.future_wip[date].keys():
                    # Step 2-2 get the "model" and "abbr" from FUTURE_EXPECTED_PRODUCT.csv
                    model, abbr = model_abbr.split()
                    # get total sheet number
                    sheet_num = self.future_wip[date][model_abbr]
                    # print(sheet_num)

                    # Step 2-3 use "model" and "abbr" get "main_route_id", "main_route_ver", "id_route", "route_ver" in 8_PRODUCT_TO_MODEL.csv
                    path = os.path.join(self.root_dir, self.date, "8_PRODUCT_TO_MODEL.csv")
                    if os.path.exists(path) == False:
                        assert os.path.exists(path), "8_PRODUCT_TO_MODEL.csv doesn't exist in {}".format(self.date)
                    
                    assert self._read_product_to_model(path, model, abbr) != None, "{}, {} don't exist in {}".format(model, abbr, path)
                    main_route_id, main_route_ver= self._read_product_to_model(path, model, abbr)
                    route_id, route_ver = main_route_id, main_route_ver
                    # print(main_route_id, main_route_ver, route_id, route_ver)

                    # Step 2-4 use "main_route_id" and "op_id" get the "op_seq", "op_ver" in 4_PROCESS_FLOW.csv 
                    path = os.path.join(self.root_dir, self.date, "4_PROCESS_FLOW.csv")
                    if os.path.exists(path) == False:
                        assert os.path.exists(path), "4_PROCESS_FLOW.csv doesn't exist in {}".format(self.date)
                    assert self._read_process_flow(path, main_route_id,  'PI PRINT_TFT') != None, "{} doesn't exist in {}".format(main_route_id, path)
                    stage_id, op_ver, op_seq = self._read_process_flow(path, main_route_id, 'PI PRINT_TFT')
                    # print(stage_id, op_ver, op_seq)

                    # Step 2-5 use "main_route_id" and "op_id" get the "eqp_group_id" in 5_OPTOEQP.csv
                    path = os.path.join(self.root_dir, self.date, "5_OPTOEQP.csv")
                    if os.path.exists(path) == False:
                        assert os.path.exists(path), "5_OPTOEQP.csv doesn't exist in {}".format(self.date)
                    assert self._read_op2eqp_table(path, main_route_id,  'PI PRINT_TFT') != None, "{} doesn't exist in {}".format(main_route_id, path)
                    eqp_group_id = self._read_op2eqp_table(path, main_route_id, 'PI PRINT_TFT')

                    # Step 2-7 get recieve_time from FUTURE_EXPECTED_PRODUCT.csv
                    # random.seed(128)
                    start = datetime.strptime(date.split('~')[0], "%Y-%m-%d %H:%M:%S")
                    end = datetime.strptime(date.split('~')[1], "%Y-%m-%d %H:%M:%S")
                    bias = random.randint(0, (end-start).total_seconds())
                    # print(bias)
                    recieve_time = start + timedelta(seconds=bias) 
                    
                    # if self.sys_time > date - timedelta(hours=6):
                    #     recieve_time = self.sys_time
                    # else:
                    #     recieve_time = date
                    sheet_num_in_cassette = 0
                    cassette_id = self.cassette_id_generator()
                    for i in range(sheet_num):
                        # Step 2-1 generate the sheet_id
                        sheet_id = self.sheet_id_generator()
                        sheet_num_in_cassette += 1
    
                        # print(cassette_id)
                        data[0], data[3],data[4], data[7], data[8], data[9] = sheet_id, model, abbr, main_route_id, main_route_ver, route_id
                        data[10], data[13],data[14],data[15], data[16], data[17], data[19] = route_ver, stage_id, op_ver, op_seq, eqp_group_id, cassette_id, recieve_time
                        data[28] = self.sys_time
                        writer.writerow(data)

                        # Step 2-6 get the cassette_id
                        if sheet_num_in_cassette >= 56:
                            cassette_id = self.cassette_id_generator()
                            sheet_num_in_cassette = 0

                            start = datetime.strptime(date.split('~')[0], "%Y-%m-%d %H:%M:%S")
                            end = datetime.strptime(date.split('~')[1], "%Y-%m-%d %H:%M:%S")
                            bias = random.randint(0, (end-start).total_seconds())
                            # print(bias)
                            recieve_time = start + timedelta(seconds=bias) 


    