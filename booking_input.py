import csv, json, os
from datetime import datetime
from collections import defaultdict
if __name__ == '__main__':
    dates = {}
    booking_threshold = 90
    with open('../AUO_data/booking_csv/trans(202304).csv', 'r', newline='') as csvfile:
        rows = csv.reader(csvfile)
        
        bookings = defaultdict(list)
        group_by_machine = defaultdict(list)
        flag = False
        for i, row in enumerate(rows):
            if i == 0:
                continue
            date, selected_eqp_id, pre_status, status , pre_start_time, pre_finish_time, duration = row
            if 'CKPIC' in selected_eqp_id:
                continue
            group_by_machine[selected_eqp_id].append(row)
        for key in group_by_machine.keys():
            for _, row in enumerate(group_by_machine[key]):
                info = {}
                date, selected_eqp_id, pre_status, status , pre_start_time, pre_finish_time, duration = row
                # print(date, selected_eqp_id, pre_status, status , pre_start_time, pre_finish_time, duration)
                start_time = datetime.strptime(pre_start_time, "%Y/%m/%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                finish_time = datetime.strptime(pre_finish_time, "%Y/%m/%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                info['sheet_status'] = pre_status
                info['selected_eqp_id'] = selected_eqp_id
                info['booking'] = pre_status
                info['model_abbr'] = pre_status
                info['start_time'] = start_time
                info['finish_time'] = finish_time
                info['duration'] = float(duration)
                bookings[selected_eqp_id].append(info)
    candidate_booking = defaultdict(list)
    interval = {}
    ## combine the same sheet status interval
    for eqp_id, intervals in bookings.items():
        for i in range(len(intervals)):
            if i == 0:
                interval = intervals[i]
                continue
            if (intervals[i]['sheet_status'] == 'DOWN' or intervals[i]['sheet_status'] == 'IDLE' and intervals[i]['duration'] < 3600) or intervals[i]['sheet_status'] == interval['sheet_status']:
                interval['finish_time'] = intervals[i]['finish_time']
            else:
                candidate_booking[eqp_id].append(interval)
                interval = intervals[i]
            if i == len(intervals) -1:
                candidate_booking[eqp_id].append(interval)
            
            # all booking
            # candidate_booking[eqp_id].append(intervals[i])
    results = defaultdict(list)
    for eqp_id, intervals in candidate_booking.items():
        for i in range(len(intervals)):
            if intervals[i]['duration'] < 3600*3:
                continue
            if intervals[i]['sheet_status'] == 'DMQC' and intervals[i-1]['sheet_status'] != 'RUN' and intervals[i-1]['sheet_status'] != 'IDLE':
                date = intervals[i]['start_time'].split()[0]
                results[date].append(intervals[i])
            elif intervals[i]['sheet_status'] == 'DOWN' or intervals[i]['sheet_status'] == 'SHOT' or intervals[i]['sheet_status'] == 'PM' or intervals[i]['sheet_status'] == 'TEST':
                date = intervals[i]['start_time'].split()[0]
                results[date].append(intervals[i])
            
            # all booking
            # date = intervals[i]['start_time'].split()[0]
            # results[date].append(intervals[i])
                
    print(results.keys())
    for date in results.keys():            
        if not os.path.exists(os.path.join('booking', date)):
            os.makedirs(os.path.join('booking', date))
        with open('booking/' + date + '/booking.json', 'w') as f:
            json.dump(results[date], f, indent=4)
