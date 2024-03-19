from datetime import timedelta
import random

AVAILABLE = 0
PROCESSED = 1
SETUP = 2
BOOKING = 3
SHOT = 4

class ODFMachine:
    def __init__(self, machine_id, eqp_id, config):
        self.machine_id = machine_id
        self.eqp_id = eqp_id
        self.start_time = config['start_time']
        self.current_time = config['start_time']
        self.machine_history = []
        self.loading = timedelta(seconds=0)
        self.add_setup_time = config['add_setup_time']
        self.stochastic = config['stochastic']
        self.bookings = []
        self.setup_time_hist = []
        self.shot_time_hist = []

    def load_booking(self, start_dt, finish_dt):
        self.bookings.append((start_dt, finish_dt))

    def process_one_wip(self, wip_info, start_time, history):
        assert start_time >= self.current_time
        ## setup mechanism
        setup_time = self.setup_time_handler(wip_info, start_time)
        if setup_time != timedelta(hours=0):
            history.append({
                "sheet_status": "DMQC",
                "selected_eqp_id": self.eqp_id,
                "model_abbr": "DMQC",
                "booking": "DMQC",
                "start_time": self.current_time,
                "finish_time": self.current_time + setup_time,
            })
            self.setup_time_hist.append([self.current_time, self.current_time + setup_time])
        self.current_time += setup_time
        self.current_time = max(self.current_time, wip_info['min_qtime'])
        
        capacity = timedelta(minutes=wip_info['capacity'][self.eqp_id])
        
        wip_info['start_time'] = self.current_time
        self.current_time += capacity
        self.loading += capacity
        wip_info['finish_time'] = wip_info['start_time'] + capacity
        for bookings in self.bookings:
            mst, met = bookings[0], bookings[1]
            if wip_info['finish_time'] > mst and wip_info['start_time'] < met:
                wip_info['start_time'] = bookings[1]
                wip_info['finish_time'] = wip_info['start_time'] + capacity
                self.current_time = wip_info['finish_time']

        ## shot mechanism
        shot_time = timedelta(seconds=0) if self.stochastic == False else timedelta(seconds=random.randint(0, 3600))
        shot_wip = {
            'sheet_status': "SHOT",
            'booking': "SHOT",
            'model_no': wip_info['model_no'],
            'abbr_no': wip_info['abbr_no'],
            'model_abbr': wip_info['model_abbr'],
            'selected_eqp_id': wip_info['selected_eqp_id'],
            'start_time': wip_info['finish_time'],
            'finish_time': wip_info['finish_time'] + shot_time,
            'product_code': wip_info['product_code'],
            'duration': int(shot_time.total_seconds())
        }
        if shot_wip['duration'] != 0:
            self.machine_history.append(shot_wip)
            
        wip_info['done'] = True
        self.current_time = wip_info['finish_time'] + shot_time
        self.machine_history.append(wip_info)
        return shot_wip, setup_time, wip_info

    def get_status(self, time):
        # for bookings in self.bookings:
        #     mst, met = bookings[0], bookings[1]
        #     if mst <= time < met:
        #         return BOOKING
        # for setup_interval in self.setup_time_hist:
        #     start, finish = setup_interval[0], setup_interval[1]
        #     if start <= time <= finish:
        #         return SETUP
        # for shot_interval in self.shot_time_hist:
        #     start, finish = shot_interval[0], shot_interval[1]
        #     if start <= time <= finish:
        #         return SHOT
            
        if self.current_time < time:
            return AVAILABLE
        else:
            return PROCESSED
 

    def get_last_wip(self):
        if len(self.machine_history) > 0:
            return self.machine_history[-1]
        else:
            return None

    def setup_time_handler(self, wip_info, current_time):
        zero_time_td = timedelta(hours=0)
        short_setup_time_td = timedelta(hours=0.5)
        long_setup_time_td = timedelta(hours=3)
        last_wip = self.get_last_wip()
        if not last_wip:
            return zero_time_td
        for (booking_start_time, booking_finish_time) in self.bookings:
            if current_time >= booking_finish_time and booking_finish_time > last_wip['finish_time']:
                return zero_time_td
        if last_wip['abbr_no'] != wip_info['abbr_no']:
            if last_wip['model_no'] == wip_info['model_no']:
                return short_setup_time_td
            else:
                return long_setup_time_td
        else:
            return zero_time_td

    def get_utilization(self):
        duration = (self.current_time - self.start_time).total_seconds()
        if duration == 0.0:
            return 0
        else:
            loading = self.loading.total_seconds()
            assert(loading <= duration)
            return loading / duration
        
    def get_time_left_ratio(self, time):
        status = self.get_status(time)
        if status == BOOKING:
            for bookings in self.bookings:
                mst, met = bookings[0], bookings[1]
                if mst <= time < met:
                    return (time - mst) / (met - mst)
        if status == SETUP:
            for setup_interval in self.setup_time_hist:
                start, finish = setup_interval[0], setup_interval[1]
                if start <= time <= finish:
                    return (time - start) / (finish - start) 
        if status == AVAILABLE:
            return 0
        if status == PROCESSED:
            last_wip_info = self.get_last_wip()
            if last_wip_info is None:
                return 0
            start, finish = last_wip_info['start_time'], last_wip_info['finish_time']
            return (self.current_time - start) / (finish - start)
        if status == SHOT:
            for shot_interval in self.shot_time_hist:
                start, finish = shot_interval[0], shot_interval[1]
                if start <= time <= finish:
                    return (time - start) / (finish - start)

    def update_booking_avai_time(self, current_time):
        for (booking_start_time, booking_finish_time) in self.bookings:
            if current_time >= booking_start_time and current_time <= booking_finish_time:
                if self.current_time < booking_finish_time:
                    self.current_time = booking_finish_time


    def not_overlap_in_booking(self, start_time, finish_time):
        for (booking_start_time, booking_finish_time) in self.bookings:
            if finish_time > booking_start_time and start_time < booking_finish_time:
                return False
        return True
    
    def not_process_in_booking(self, wip_info, start_time):
        setup_time = self.setup_time_handler(wip_info, start_time)
        capacity = timedelta(minutes=wip_info['capacity'][self.eqp_id])
        finish_time = start_time + setup_time + capacity
        return self.not_overlap_in_booking(start_time, finish_time)