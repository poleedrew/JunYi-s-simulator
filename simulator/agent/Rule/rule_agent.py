from collections import defaultdict
from pprint import pprint
from env.utils.heuristic import EDD_WIP, EMaxQ_WIP, EMinQ_WIP, FIFO_WIP, LPT_WIP, SPT_WIP, EDD_WIP_v2
from env.utils.heuristic import SPT_model_abbr_FIFO_WIP, FIFO_model_abbr_FIFO_WIP

class RuleAgent:
    def __init__(self):
        self.total_steps = 0

    def load_rules(self, gap, tact_time_table_path):
        # tact_time_table_path = "V20220519/14_STD_TACTTIME_imputed.csv"
        self.index2heur = {
            0: 'EDD',
            1: 'EMaxQ',
            2: 'EminQ',
            3: 'FIFO',
            4: 'LPT',
            5: 'SPT',
            6: 'SPT_model_abbr_FIFO_WIP',
            7: 'FIFO_model_abbr_FIFO_WIP',
            8: 'EDD_v2',}
        self.heuristics = {
            'EDD': EDD_WIP(gap, tact_time_table_path),
            'EMaxQ': EMaxQ_WIP(),
            'EMinQ': EMinQ_WIP(),
            'FIFO': FIFO_WIP(),
            'LPT': LPT_WIP(),
            'SPT': SPT_WIP(),
            'SPT_model_abbr_FIFO_WIP': SPT_model_abbr_FIFO_WIP(),
            'FIFO_model_abbr_FIFO_WIP': FIFO_model_abbr_FIFO_WIP(), 
            'EDD_v2': EDD_WIP_v2(gap),}
        self.machine_option = ['earliest', 'tact_time', 'setup_time']
        self.rules_count = defaultdict(int)

    def apply_rule(self, rule_name, wip_info_list, machines, current_time_dt):
        heur_name, option_name = rule_name
        self.rules_count[rule_name] += 1
        heur = self.heuristics[heur_name]
        wip_info = heur(wip_info_list, machines, current_time_dt, verbose=False)
        eqp_id = self.select_machine(wip_info, machines, option_name)
        self.total_steps += 1
        return wip_info, eqp_id

    def select_machine(self, wip_info, machines, option):
        # {'CKODF100': 1, 'CKODF200': 2, 'CKODF300': 4, 'CKODF400': 3}
        earliest_rank = self._find_earliest_rank(wip_info, machines)
        tact_time_rank = self._find_tact_time_rank(wip_info)
        setup_time_rank = self._find_setup_time_rank(wip_info, machines)
        ranks = {
            'earliest': earliest_rank,
            'tact_time': tact_time_rank,
            'setup_time': setup_time_rank}
        if isinstance(option, int):
            option = self.machine_option[option]
        if isinstance(option, str):
            return min(ranks[option], key=ranks[option].get)
        if isinstance(option, tuple):
            avai_machine = wip_info['tact_time'].keys()
            eqp_ranking = {}
            for eqp_id in avai_machine:
                eqp_ranking[eqp_id] = tuple(
                    ranks[opt][eqp_id] for opt in option)
            # print("eqp_ranking:")
            # pprint(eqp_ranking)
            return min(eqp_ranking, key=eqp_ranking.get)

    def _find_earliest_rank(self, wip_info, machines):
        tact_times = wip_info['tact_time']
        avai_machine = tact_times.keys()
        current_times = {
            eqp_id: machine.current_time for eqp_id,
            machine in machines.items()}
        # print(f"{current_times}")
        ranking = {
            ct: rank for rank,
            ct in enumerate(
                sorted(
                    current_times.values()),
                1)}  # current time of each machine to rank
        return {eqp_id: ranking[current_times[eqp_id]]
                for eqp_id in avai_machine}

    def _find_tact_time_rank(self, wip_info):
        tact_times = wip_info['tact_time']
        avai_machine = tact_times.keys()
        # print(f"{tact_times}")
        ranking = {
            tt: rank for rank,
            tt in enumerate(
                sorted(
                    tact_times.values()),
                1)}  # tact time of each machine to rank
        return {eqp_id: ranking[tact_times[eqp_id]] for eqp_id in avai_machine}

    def _find_setup_time_rank(self, wip_info, machines):
        setup_times = {}
        for eqp_id, machine in machines.items():
            last_wip_info = machine.last_wip()
            setup_time = 0
            if last_wip_info is None:
                setup_time = 1
            elif last_wip_info is not None and last_wip_info['model_no'] != wip_info['model_no']:
                if last_wip_info['abbr_no'] != wip_info['abbr_no']:
                    setup_time = 3 * 60 * 60
                else:
                    setup_time = 0.5 * 60 * 60
                setup_time -= (machine.current_time -
                               last_wip_info['finish_time']).total_seconds()
            setup_times[eqp_id] = setup_time
        # print(f"{setup_times}")
        ranking = {
            st: rank for rank,
            st in enumerate(
                sorted(
                    setup_times.values()),
                1)}  # setup time of each machine to rank
        return {eqp_id: ranking[setup_times[eqp_id]] for eqp_id in setup_times}
