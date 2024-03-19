import copy, random
from env.utils.heuristic import EDD_WIP, EMaxQ_WIP, EMinQ_WIP, FIFO_WIP, LPT_WIP, SPT_WIP, EDD_WIP_v2, SPT_model_abbr_FIFO_WIP, FIFO_model_abbr_FIFO_WIP

def heuristic_outcome(env, rule):
    rollout_env = copy.deepcopy(env)
    avai_ops = rollout_env.get_avai_ops()
    while True:
        wip_info_list = rollout_env.instance.current_wait_wip_info_list
        machines = rollout_env.instance.odf_machines
        current_time_dt = rollout_env.instance.current_time_dt
        if rule == "Random":
            action_idx = random.randint(0, len(avai_ops)-1)
            job_id, m_id = avai_ops[action_idx]
            avai_ops, done, _ = rollout_env.step(job_id, m_id)
        elif rule == "EDD":
            wip_info = EDD_WIP(rollout_env.gap, "tact_time/imputed/14_STD_TACTTIME_imputed.csv")(wip_info_list, machines, current_time_dt)
            eqp_id = select_machine(wip_info, machines,  ('setup_time', 'earliest','tact_time'))
            next_state, reward, done, info = rollout_env.step(
                wip_info, eqp_id, other_info={'rule': 'EDD_WIP'})
        elif rule == "EDD_v2":
            wip_info = EDD_WIP_v2(rollout_env.gap)(wip_info_list, machines, current_time_dt)
            eqp_id = select_machine(wip_info, machines,  ('setup_time', 'earliest','tact_time'))
            next_state, reward, done, info = rollout_env.step(
                wip_info, eqp_id, other_info={'rule': 'EDD_WIP_v2'})
        if done:
            return rollout_env, rollout_env.instance.outcome()
    
## select_machine
def select_machine(wip_info, machines, option):
    # {'CKODF100': 1, 'CKODF200': 2, 'CKODF300': 4, 'CKODF400': 3}
    earliest_rank = find_earliest_rank(wip_info, machines)
    tact_time_rank = find_tact_time_rank(wip_info)
    setup_time_rank = find_setup_time_rank(wip_info, machines)
    ranks = {
        'earliest': earliest_rank,
        'tact_time': tact_time_rank,
        'setup_time': setup_time_rank}
    if isinstance(option, tuple):
        avai_machine = wip_info['tact_time'].keys()
        eqp_ranking = {}
        for eqp_id in avai_machine:
            eqp_ranking[eqp_id] = tuple(
                ranks[opt][eqp_id] for opt in option)

        return min(eqp_ranking, key=eqp_ranking.get)

def find_earliest_rank( wip_info, machines):
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

def find_tact_time_rank( wip_info):
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

def find_setup_time_rank( wip_info, machines):
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

