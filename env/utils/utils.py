from datetime import datetime


def duration(start_dt, time_dt, time_unit='hour'):
    divisor = 1
    if time_unit == 'hour':
        divisor = 3600
    if time_unit == 'min' or time_unit == 'minute':
        divisor = 60
    if time_unit == 'sec' or time_unit == 'second':
        divisor = 1
    return (time_dt - start_dt).total_seconds() / divisor


def timedelta_to_HMSstr(td):
    # Hour, Minute, Second
    seconds = td.total_seconds()
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return '%dh %dm %ds' % (hours, minutes, seconds)


def datetime2str(wip_info):
    for key, value in wip_info.items():
        if key in (
            'start_time',
            'finish_time',
            'min_qtime',
            'max_qtime',
            'logon_time',
            'receive_time',
            'pre_logoff_time',
                'arrival_time'):
            if wip_info[key] is None:
                continue
            wip_info[key] = datetime.strftime(
                wip_info[key], '%Y-%m-%d %H:%M:%S')


def str2datetime(wip_info):
    for key, value in wip_info.items():
        if key in (
            'start_time',
            'finish_time',
            'min_qtime',
            'max_qtime',
            'logon_time',
            'receive_time',
            'pre_logoff_time',
                'arrival_time'):
            wip_info[key] = datetime.strptime(
                wip_info[key], '%Y-%m-%d %H:%M:%S')
