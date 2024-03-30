import numpy as np
import random
from datetime import timedelta

MAX = 1e6

def heuristic_makespan(simulator, avai_jobs, rule):
    total_reward = 0
    if rule == 'MTT':
        while True:
            action = MTT(avai_jobs, simulator.instance)
            avai_jobs, reward, done = simulator.step(action['job_id'], action['m_id']) 
            
            total_reward += reward
            if done:
                #simulator.print_result()
                #simulator.write_result("./plotting/result/MTT.json")
                return total_reward
    if rule == 'MCT':
        while True:
            action = MCT(avai_jobs, simulator.instance)
            avai_jobs, reward, done = simulator.step(action['job_id'], action['m_id']) 
            total_reward += reward
            if done:
                #simulator.print_result()
                #simulator.write_result("./plotting/result/MCT.json")
                return total_reward  

def MCT(avai_jobs, instance):
    avai_m = [i for i in range(len(avai_jobs)) if len(avai_jobs[i]) > 0]
    # divide jobs apart according to setup time
    zero_setup_jobs = []
    short_setup_jobs = []
    long_setup_jobs = []
    for info in avai_jobs[avai_m[0]]:
        job_info = instance.wait_wip_info_list[info['job_id']]
        setup_time = instance.odf_machines[info['m_id']].setup_time_handler(job_info, instance.current_time)
        if setup_time == timedelta(hours=0):
            zero_setup_jobs.append(info)
        elif setup_time == timedelta(hours=0.5):
            short_setup_jobs.append(info)
        else:
            long_setup_jobs.append(info)
    if len(zero_setup_jobs) > 0:
        candidated_jobs = zero_setup_jobs
    elif len(short_setup_jobs) > 0:
        candidated_jobs = short_setup_jobs
    else:
        candidated_jobs = long_setup_jobs
    min_capacity = MAX
    action = -1
    for i in range(len(candidated_jobs)):
        job_id, eqp_id = candidated_jobs[i]['job_id'], candidated_jobs[i]['m_id']
        job_info = instance.wait_wip_info_list[job_id]
        if job_info['capacity'][eqp_id] < min_capacity:
            min_capacity = job_info['capacity'][eqp_id]
            action = i
    return candidated_jobs[action]

def MTT(avai_jobs, instance):
    avai_m = [i for i in range(len(avai_jobs)) if len(avai_jobs[i]) > 0]
    # divide jobs apart according to setup time
    zero_setup_jobs = []
    short_setup_jobs = []
    long_setup_jobs = []
    for info in avai_jobs[avai_m[0]]:
        job_info = instance.wait_wip_info_list[info['job_id']]
        setup_time = instance.odf_machines[info['m_id']].setup_time_handler(job_info, instance.current_time)
        if setup_time == timedelta(hours=0):
            zero_setup_jobs.append(info)
        elif setup_time == timedelta(hours=0.5):
            short_setup_jobs.append(info)
        else:
            long_setup_jobs.append(info)
    if len(zero_setup_jobs) > 0:
        candidated_jobs = zero_setup_jobs
    elif len(short_setup_jobs) > 0:
        candidated_jobs = short_setup_jobs
    else:
        candidated_jobs = long_setup_jobs
    min_tact_time = MAX
    action = -1
    for i in range(len(candidated_jobs)):
        job_id, eqp_id = candidated_jobs[i]['job_id'], candidated_jobs[i]['m_id']
        job_info = instance.wait_wip_info_list[job_id]
        if job_info['tact_time'][eqp_id] < min_tact_time:
            min_tact_time = job_info['tact_time'][eqp_id]
            action = i
    return candidated_jobs[action]
