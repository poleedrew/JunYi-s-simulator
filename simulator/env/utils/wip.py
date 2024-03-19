from datetime import datetime
from datetime import timedelta
from pprint import pprint
import copy

from env.utils.utils import timedelta_to_HMSstr
      

class ODFWIP:
    def __init__(self, wip_config):
        self.done = False
        self.on_ODF =               wip_config['on_ODF']
        self.sheet_status =         wip_config['sheet_status']
        self.model_no =             wip_config['model_no']
        self.abbr_no =              wip_config['abbr_no']
        self.abbr_cat =             wip_config['abbr_cat']
        self.start_time  =          None
        self.finish_time =          None
        self.real_tact_time =       0
        self.cassette_id =          wip_config['cassette_id']
        self.tact_times =           wip_config.get('tact_time', None)
        self.tact_time_sum =        sum(self.tact_times.values()) if self.tact_times != None else 0
        self.product_code =         wip_config['product_code']
        self.original_op_id =       wip_config['original_op_id']
        self.original_stage_id =    wip_config['original_stage_id']
        self.op_id =                wip_config['op_id']
        self.stage_id =             wip_config['stage_id']
        self.route_id =             wip_config['route_id']
        self.sheet_list =           []
        self.capacity =             0
        self.selected_eqp_id =      wip_config.get('selected_eqp_id', None)

    def __repr__(self):
        return '%s\t%s\t%d\t%s\t%s\t%s\t%s' %(
            self.model_no, 
            self.abbr_no,  
            self.size,    
            self.min_qtime,
            self.max_qtime,
            self.start_time,
            self.finish_time,)
    
    @property
    def min_qtime(self):
        return max([sheet.min_qtime for sheet in self.sheet_list])
        
    @property
    def max_qtime(self):
        return min([sheet.max_qtime for sheet in self.sheet_list])
        
    @property
    def size(self):
        return len(self.sheet_list)

    @property
    def pre_logoff_time(self):
        return max([sheet.pre_logoff_time for sheet in self.sheet_list])

    @property
    def logon_time(self):
        return max([sheet.logon_time for sheet in self.sheet_list])

    @property
    def arrival_time(self):
        return max([sheet.arrival_time for sheet in self.sheet_list])

    def process(self, eqp_id):
        self.selected_eqp_id = eqp_id
        self.real_tact_time = self.tact_times[eqp_id]
        self.finish_time = self.start_time + self.capacity
        self.done = True
        for sheet in self.sheet_list:
            sheet.process(self.start_time)
        return self.finish_time
    
    def to_json(self):
        # SHEET_ID	SHEET_STATUS	
        # SHEET_TYPE	MODEL_NO	ABBR_NO	ABBR_CAT	
        # EQP_ID	
        # MAIN_ROUTE_ID	MAIN_ROUTE_VER	ROUTE_ID	ROUTE_VER	ROUTE_TYPE	
        # OP_ID	STAGE_ID	OP_VER	OP_SEQ	OP_SEQ_BACK	ROUTE_DEPTH	
        # EQP_GROUP_ID	CASSETTE_ID	SHEET_PRIORITY	
        # RECEIVE_TIME	PRE_LOGOFF_TIME	LOGON_TIME	
        # MIN_QT_OP_SEQ_START	MIN_QT_OP_SEQ_END	MIN_QT_END_TIME	MAX_QT_OP_SEQ_START	MAX_QT_OP_SEQ_END	MAX_QT_END_TIME	SYS_DATE
        wip_info = {
            'done':                 False,
            'on_ODF':               self.on_ODF,
            'sheet_status':         self.sheet_status,
            'model_no':             self.model_no,
            'abbr_no':              self.abbr_no,
            'model_abbr':           self.model_no + '-' + self.abbr_no,
            'abbr_cat':             self.abbr_cat,
            'selected_eqp_id':      self.selected_eqp_id,
            'route_id':             self.route_id,
            'original_op_id':       self.original_op_id,
            'original_stage_id':    self.original_stage_id,
            'op_id':                self.op_id,
            'stage_id':             self.stage_id,
            'cassette_id':          self.cassette_id,
            'pre_logoff_time':      datetime.strftime(self.pre_logoff_time, '%Y-%m-%d %H:%M:%S'),
            'logon_time':           datetime.strftime(self.logon_time, '%Y-%m-%d %H:%M:%S'),
            'arrival_time':         datetime.strftime(self.arrival_time, '%Y-%m-%d %H:%M:%S'),
            'min_qtime':            datetime.strftime(self.min_qtime, '%Y-%m-%d %H:%M:%S'),
            'max_qtime':            datetime.strftime(self.max_qtime, '%Y-%m-%d %H:%M:%S'), 
            'product_code':         self.product_code,
            'tact_time':            self.tact_times,
            'size':                 self.size,
            'capacity':             {eqp_id: tact_time*self.size for eqp_id, tact_time in self.tact_times.items()},
            'product_code':         self.product_code,
            'tact_time':            self.tact_times,
            'sheet_list':           [sheet.sheet_id for sheet in self.sheet_list],
        }
        if self.start_time != None:
            wip_info['start_time'] = datetime.strftime(self.start_time, '%Y-%m-%d %H:%M:%S') 
        if self.finish_time != None:
            wip_info['finish_time'] = datetime.strftime(self.finish_time, '%Y-%m-%d %H:%M:%S')
        return wip_info

