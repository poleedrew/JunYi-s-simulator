from datetime import datetime
from datetime import timedelta


class ODFSheet(object):
    def __init__(self, sheet_info):
        self.done =                 False
        self.sheet_id =             sheet_info['sheet_id']
        self.sheet_status =         sheet_info['sheet_status']
        self.model_no =             sheet_info['model_no']
        self.abbr_no =              sheet_info['abbr_no']
        self.abbr_cat =             sheet_info['abbr_cat']
        self.eqp_id =               sheet_info.get('selected_eqp_id', None)
        self.route_id =             sheet_info['route_id']
        self.original_op_id =       sheet_info['original_op_id']
        self.op_id =                sheet_info['op_id']
        self.original_stage_id =    sheet_info['original_stage_id']
        self.stage_id =             sheet_info['stage_id']
        self.op_seq =               sheet_info.get('op_seq', None)
        self.receive_time =         sheet_info.get('receive_time', None)
        self.pre_logoff_time =      sheet_info['pre_logoff_time']
        self.arrival_time =         sheet_info['arrival_time']
        self.logon_time =           sheet_info['logon_time']
        self.eqp_group_id =         sheet_info.get('eqp_group_id', None)
        self.cassette_id =          sheet_info.get('cassette_id', None)
        self.min_qtime =            sheet_info.get('min_qtime', None)
        self.max_qtime =            sheet_info.get('max_qtime', None)
        self.product_code =         sheet_info.get('product_code', None)  # sheet_info['product_code'] 
        self.process_time =         sheet_info.get('process_time', None)
        self.cycle_time =           sheet_info.get('cycle_time', None)
        self.start_time =           None
        self.finish_time =          None  
    
    def process(self, start_time):
        self.done = True
        self.start_time = start_time
    
    def __repr__(self):
        return str(self.__dict__)
    
    def to_json(self):
        return {
            'sheet_id':             self.sheet_id,
            'sheet_status':         self.sheet_status,
            'model_no':             self.model_no,
            'abbr_no':              self.abbr_no,
            'abbr_cat':             self.abbr_cat,
            'route_id':             self.route_id,
            'op_id':                self.op_id,
            'original_op_id':       self.original_op_id,
            'stage_id':             self.stage_id,
            'original_stage_id':    self.original_stage_id,
            'op_seq':               self.op_seq,
            'logon_time':           datetime.strftime(self.logon_time, '%Y-%m-%d %H:%M:%S'),
            'pre_logoff_time':      datetime.strftime(self.pre_logoff_time, '%Y-%m-%d %H:%M:%S'),
            'arrival_time':         datetime.strftime(self.arrival_time, '%Y-%m-%d %H:%M:%S'),
            'eqp_group_id':         self.eqp_group_id,
            'cassette_id':          self.cassette_id,
            'receive_time':         datetime.strftime(self.receive_time, '%Y-%m-%d %H:%M:%S'),
            'min_qtime':            datetime.strftime(self.min_qtime, '%Y-%m-%d %H:%M:%S'),
            'max_qtime':            datetime.strftime(self.max_qtime, '%Y-%m-%d %H:%M:%S'),
            'process_time':         self.process_time.total_seconds(),
            'cycle_time':           self.cycle_time.total_seconds(),
            # 'start_time':       self.start_time,
            # 'finish_time':      self.finish_time,
        }