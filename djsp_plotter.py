import os
from datetime import datetime
from colorhash import ColorHash
from djsp_logger import DJSP_Logger


class DJSP_Plotter(object):
    def __init__(self, logger, ForAUO):
        self.ForAUO = ForAUO
        self.logger = logger
        self.time_list = ['pre_logoff_time', 'logon_time', 'arrival_time', 'min_qtime', 'max_qtime', 'start_time', 'finish_time']
        self.model_abbr_list = ['model_no', 'abbr_no', 'abbr_cat', 'model_abbr']

    def _get_tooltip(self, wip_info):
        wip_info.pop('sheet_list', None)    # Too detail
        return str(wip_info)

    def _get_machine(self, wip_info):
        assert 'selected_eqp_id' in wip_info, 'There is no key named selected_eqp_id in wip_info'
        return '%s' % (wip_info['selected_eqp_id'])

    def _get_model_abbr(self, wip_info):
        assert 'model_abbr' in wip_info, 'There is no key named model_abbr in wip_info'
        return str(wip_info['model_abbr'])

    def _get_color(self, wip_info):
        booking_color = {
            'IDLE': '#FFFD00',
            'DOWN': '#FF0000',
            'PM': '#61FEFC',
            'DMQC': '#FF00FF',
            'TEST': '#ccFFc4',
            'SHOT': '#fa9a00',
        }
        booking_type = wip_info.get('booking', None)
        selected_eqp_id = wip_info['selected_eqp_id']
        if booking_type in booking_color:
            return booking_color[booking_type]
        elif wip_info['sheet_status'] == 'RUN':
            return '#808080'
        # elif wip_info['max_qtime'] < wip_info['start_time']:
        #     return '#F88379'
        # elif wip_info['capacity'][selected_eqp_id] == min(wip_info['capacity'].values()):
        #     return '#AFE1AF'
        else:
            if self.ForAUO:
                return '#00FF00'
            else:
                color = ColorHash(wip_info['model_abbr']) #, lightness=[0.6])
                return color.hex

    def _get_info_table(self, wip_info):
        value = '<table style="border:3px #cccccc solid;" cellpadding="10" border="2"><strong>Info table</strong><tr>'
        for key in wip_info.keys():
            if key == 'tact_time' or key == 'capacity' or key in self.model_abbr_list or key in self.time_list:
                continue
            value += "<th>"+str(key)+"</th>"
        value += "</tr><tr>"
        for key, info in wip_info.items():
            if type(info) == dict or key in self.time_list or key in self.model_abbr_list:
                continue
            value += "<th>"+str(info)+"</th>"
        value += "</tr>"
        value += "</table>"
        return value
    def _get_tact_table(self, wip_info):
        value = '<table style="border:3px #cccccc solid;" cellpadding="10" border="2"><strong>Tact_time table</strong><tr>'
        for key in wip_info.keys():
            if key == 'tact_time':
                for k in wip_info['tact_time'].keys():
                    value += "<th>"+str(k)+"</th>"
            continue
        value += "</tr><tr>"
        for key, info in wip_info.items():
            if key == 'tact_time':
                for k in info.values():
                    value += "<th>"+str(k)+"</th>"
            continue
        value += "</tr>"
        value += "</table>"
        return value
    
    def _get_capacity_table(self, wip_info):
        value = '<table style="border:3px #cccccc solid;" cellpadding="10" border="2"><strong>Capacity table</strong><tr>'
        for key in wip_info.keys():
            if key == 'capacity':
                for k in wip_info['capacity'].keys():
                    value += "<th>"+str(k)+"</th>"
            continue
        value += "</tr><tr>"
        for key, info in wip_info.items():
            if key == 'capacity':
                for k in info.values():
                    value += "<th>"+str(k)+"</th>"
            continue
        value += "</tr>"
        value += "</table>"
        return value

    def _get_time_table(self, wip_info):
        value = '<table style="border:3px #cccccc solid;" cellpadding="10" border="2"><strong>Time table</strong><tr>'
        for key in wip_info.keys():
            if key in self.time_list:
                value += "<th>"+str(key)+"</th>"
            continue
        value += "</tr><tr>"
        for key, info in wip_info.items():
            if key in self.time_list:
                value+= "<th>"+str(info)+"</th>"
            continue
        value += "</tr>"
        value += "</table>"
        return value
    
    def _get_model_abbr_table(self, wip_info):
        value = '<table style="border:3px #cccccc solid;" cellpadding="10" border="2"><strong>Model table</strong><tr>'
        for key in wip_info.keys():
            if key not in self.model_abbr_list:
                continue
            value += "<th>"+str(key)+"</th>"
        value += "</tr><tr>"
        for key, info in wip_info.items():
            if key not in self.model_abbr_list:
                continue
            value += "<th>"+str(info)+"</th>"
        value += "</tr>"
        value += "</table>"
        return value

    def _get_gc_row(self, wip_info):
        assert 'start_time' in wip_info, 'There is no key named start_time in wip_info'
        assert 'finish_time' in wip_info, 'There is no key named finish_time in wip_info'
        start_str = wip_info['start_time']
        finish_str = wip_info['finish_time']
        row = [
            self._get_machine(wip_info),
            self._get_model_abbr(wip_info),
            self._get_color(wip_info),
            self._get_tooltip(wip_info),
            self._get_info_table(wip_info),
            self._get_tact_table(wip_info),
            self._get_capacity_table(wip_info),
            self._get_time_table(wip_info),
            self._get_model_abbr_table(wip_info)
        ]
        start_dt = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
        finish_dt = datetime.strptime(finish_str, '%Y-%m-%d %H:%M:%S')
        start_gcstr = 'new Date(%d,%d,%d,%d,%d,%d)' % (start_dt.year,
                                                       start_dt.month - 1,
                                                       start_dt.day,
                                                       start_dt.hour,
                                                       start_dt.minute,
                                                       start_dt.second)
        finish_gcstr = 'new Date(%d,%d,%d,%d,%d,%d)' % (finish_dt.year,
                                                        finish_dt.month - 1,
                                                        finish_dt.day,
                                                        finish_dt.hour,
                                                        finish_dt.minute,
                                                        finish_dt.second)
        date_str = ',%s, %s' % (start_gcstr, finish_gcstr)
        row = str(row)[:-1] + date_str + ']'
        return row

    def plot_googlechart_timeline(self, html_out_file):
        history = self.logger.history
        # history = sorted(history, key=lambda wip_info : wip_info['machine_id'])
        history = sorted(
            history,
            key=lambda wip_info: wip_info['selected_eqp_id'])
        html_text = ''
        html_text += self.logger.google_chart_front_text

        for i, wip_info in enumerate(history):
            if wip_info['sheet_status'] == 'RUN' and wip_info.get('order') == None:
                continue
            gc_row = self._get_gc_row(wip_info)
            gc_row = gc_row + ',\n'
            html_text += gc_row
        html_text += self.logger.google_chart_back_text
        with open(html_out_file, 'w') as fp:
            fp.write(html_text)


if __name__ == '__main__':
    # result_dir = "agent/Rule/result"
    # timeline_dir = "agent/Rule/timeline"
    # result_dir = "agent/DQN/result/eval"
    # timeline_dir = "agent/DQN/timeline"
    # result_dir = "agent/Baseline_Rollout/result/eval"
    # timeline_dir = "agent/Baseline_Rollout/timeline"
    # result_dir = "result/"
    # timeline_dir = "timeline/"
    result_dir = "result/dps"
    timeline_dir = "timeline/"

    for task_name in os.listdir(result_dir):
        for file_name in os.listdir(os.path.join(result_dir, task_name)):
            result_path = os.path.join(result_dir, task_name, file_name)
            # if "" not in result_path:
            #     continue
            print(result_path)
            logger = DJSP_Logger()
            logger.load(result_path)
            # print(logger.history)
            ForAUO = True
            plotter = DJSP_Plotter(logger, ForAUO)
            fn, _ = os.path.splitext(file_name)
            if not os.path.exists(os.path.join(timeline_dir, task_name)):
                os.makedirs(os.path.join(timeline_dir, task_name))
            timeline_path = os.path.join(timeline_dir, task_name, fn + '.html')
            print(timeline_path)
            plotter.plot_googlechart_timeline(timeline_path)
