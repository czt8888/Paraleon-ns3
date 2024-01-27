import math
import random
import numpy as np
import pandas as pd
import time
import hashlib
from scipy.stats import entropy
import threading
import os


def calculate_file_hash(file_path):
    sha256_hash = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)

    return sha256_hash.hexdigest()

def potential_weight(potential_duration):
    mapped_duration = potential_duration / t_reset
    return 1 / (1 + np.exp(-mapped_duration))

def is_flow_keep_sending_sliding_window(lst, window_size):
    result = []
    for i in range(len(lst) - window_size + 1):
        window_sum = sum(lst[i:i+window_size])
        result.append(window_sum)

    for i in range(1, len(result)-1):
        if result[i] == 0:
            return False
        elif result[i-1] - result[i] > 10:
            if result[i] - result[i + 1] > 20:
                return False
    return True

def is_flow_keep_active_within_window(lst, window_size):
    if len(lst) <= window_size:
        return False
    
    result = []
    for i in range(len(lst) - window_size + 1):
        window_sum = sum(lst[i:i+window_size])
        result.append(window_sum)

    for i in range(1, len(result)-1):
        if result[i] == 0:
            return False
        elif result[i-1] - result[i] > 10:
            if len(result) < 3:
                continue
            if result[i] - result[i + 1] > 20:
                return False
    return True


def update_all_flows(current_flowid_size_dict, current_time):
    global switch_flowid_status_dict
    
    for switch_id, flowid_size_dict in current_flowid_size_dict.items():
        if switch_id not in switch_flowid_status_dict:
            switch_flowid_status_dict[switch_id] = {}
        
        for flow_id, flow_size in flowid_size_dict.items():
            if flow_id not in switch_flowid_status_dict[switch_id]:
                temp_flowid_status = {'active': True, 'flow_size': [], 'last_monitor_time': current_time, 'potential_start': -1, 'potential_stop': -1}
                switch_flowid_status_dict[switch_id][flow_id] = temp_flowid_status
            
            switch_flowid_status_dict[switch_id][flow_id]['flow_size'].append(flow_size)
            switch_flowid_status_dict[switch_id][flow_id]['last_monitor_time'] = current_time
            switch_flowid_status_dict[switch_id][flow_id]['active'] = True

    for switch_id, flow_dataset in switch_flowid_status_dict.items():
        for flow_id, flow_status in flow_dataset.items():
            if flow_status['active'] == False:
                continue
            if current_time - flow_status['last_monitor_time'] > t_reset / 2:
                flow_status['active'] = False
                continue


def filter_flows():
    global switch_flowid_status_dict
    switch_upload_dict = {}

    for switch_id, single_flowid_status_dict in switch_flowid_status_dict.items():
        if switch_id not in switch_upload_dict:
            flow_type_list_dict = {'large': [], 'potential_large': [], 'small': []}
            switch_upload_dict[switch_id] = flow_type_list_dict
        
        for flow_id, flow_status in single_flowid_status_dict.items():
            flow_active = flow_status['active']
            flow_size = sum(flow_status['flow_size'])
            
            if flow_active:
                if flow_size >= large_flow_threshold:
                    switch_upload_dict[switch_id]['large'].append(flow_id)
                    if flow_id in switch_upload_dict[switch_id]['potential_large']:
                        switch_upload_dict[switch_id]['potential_large'].remove(flow_id)
                        flow_status['potential_stop'] = current_time

                else:
                    if is_flow_keep_active_within_window(flow_status['flow_size'], window_size):
                        switch_upload_dict[switch_id]['potential_large'].append(flow_id)
                        if flow_status['potential_start'] == -1:
                            flow_status['potential_start'] = current_time

                        if flow_id in switch_upload_dict[switch_id]['small']:
                            switch_upload_dict[switch_id]['small'].remove(flow_id)
                    else:
                        switch_upload_dict[switch_id]['small'].append(flow_id)
            else:
                if flow_id in switch_upload_dict[switch_id]['large']:
                    switch_upload_dict[switch_id]['large'].remove(flow_id)

                if flow_id in switch_upload_dict[switch_id]['potential_large']:
                    switch_upload_dict[switch_id]['potential_large'].remove(flow_id)
                    if flow_status['potential_stop'] == -1:
                        flow_status['potential_stop'] = current_time

                if flow_id in switch_upload_dict[switch_id]['small']:
                    switch_upload_dict[switch_id]['small'].remove(flow_id)
    
    return switch_upload_dict


def load_sketch():
    file_column = ['time', 'switch']
    current_flowid_size_dict = {}

    for i in range(10000):
        file_column.append('p' + str(i))
    data = pd.read_table(sketch_heavypart_path, names=file_column, header=None, sep='\s+', low_memory=False).fillna(value='-1na')

    for index, row in data.iterrows():
        current_time = row[0]
        switch_id = row[1]

        if switch_id not in current_flowid_size_dict:
            current_flowid_size_dict[switch_id] = {}

        if row.shape[0] > 1:
            for i in range(2, row.shape[0]):
                if 'na' not in row[i]:
                    flowid_size_pair = row[i]
                    flow_id = flowid_size_pair.split('-')[0]
                    flow_size = int(flowid_size_pair.split('-')[1])
                    current_flowid_size_dict[switch_id][flow_id] = flow_size
                else:
                    break

    return current_time, current_flowid_size_dict


def upload_ratio(switch_upload_dict, current_time):
    global current_flow_ratio
    for switch_id, flow_type_list_dict in switch_upload_dict.items():
        for flow_type, flow_type_list in flow_type_list_dict.items():
            if flow_type == 'large':
                current_flow_ratio['large'] += len(flow_type_list)
            elif flow_type == 'small':
                current_flow_ratio['small'] += len(flow_type_list)
            elif flow_type == 'potential_large':
                for flow_id in flow_type_list:
                    potential_start = switch_flowid_status_dict[switch_id][flow_id]['potential_start']
                    potential_duration = current_time - potential_start
                    current_flow_ratio['large'] += potential_weight(potential_duration)


def compute_divergence(current_flow_ratio, previous_flow_ratio):
    current_prob = np.array(list(current_flow_ratio.values())) / sum(current_flow_ratio.values())
    previous_prob = np.array(list(previous_flow_ratio.values())) / sum(previous_flow_ratio.values())
    kl_divergence = entropy(current_prob, previous_prob)

    return kl_divergence

# rate increase
time_reset = [i for i in range(1, 131071)]                       # default = 300
ai_rate = [i for i in range(1, 500)]                             # default = 5
hai_rate = [i for i in range(10, 5000)]                       # default = 50

# rate decrease
rate_to_set_on_first_cnp = [i for i in np.arange(0.1, 1, 0.1)]       # default = 0.5
rpg_min_dec_fac = [i for i in np.arange(0.1, 1, 0.1)]                # default = 0.5
rpg_min_rate = [i for i in range(1, 5000)]                         # default = 1Mbps
rpg_gd = [i for i in range(1, 13)]                                 # default = 11
min_time_between_cnps = [i for i in range(0, 4095)]              # default = 4

# alpha update
dce_tcp_g = [i for i in range(0, 1019)]                              # default = 1019
initial_alpha_value = [i for i in range(1023)]                    # default = 1023

# ecn threshold
kmin = [i for i in range(10, 1600)]                          # default = 400
kmax = [i for i in range(40, 6400)]                          # default = 1600

DCQCN_parameter = [time_reset, ai_rate, hai_rate, rate_to_set_on_first_cnp, rpg_min_dec_fac, 
                   rpg_min_rate, rpg_gd, min_time_between_cnps, dce_tcp_g, initial_alpha_value, kmin, kmax]
DCQCN_parameter_name = ['time_reset', 'ai_rate', 'hai_rate', 'rate_to_set_on_first_cnp', 
                        'rpg_min_dec_fac', 'rpg_min_rate', 'rpg_gd', 'min_time_between_cnps', 
                        'dce_tcp_g', 'initial_alpha_value', 'kmin', 'kmax']
DCQCN_name_index_mapping = {'time_reset': 0, 'ai_rate': 1, 'hai_rate': 2, 'rate_to_set_on_first_cnp': 3, 
                        'rpg_min_dec_fac': 4, 'rpg_min_rate': 5, 'rpg_gd': 6, 'min_time_between_cnps': 7, 
                        'dce_tcp_g': 8, 'initial_alpha_value': 9, 'kmin': 10, 'kmax': 11}

parameter_min_value = [1, 1, 10, 0.1, 0.1, 1, 1, 0, 1, 1, 10, 40]
parameter_max_value = [131071, 500, 5000, 1, 1, 5000, 13, 4095, 1019, 1023, 1600, 6400]
parameter_step = [50, 50, 100, 0.1, 0.1, 300, 1, 16, 16, 16, 100, 400]
default_parameters = [300, 5, 50, 0.5, 0.5, 1, 11, 4, 1019, 1023, 400, 1600]


def utility_function(throughput, rtt, pfc, throughput_weight, rtt_weight, pfc_weight):
    return throughput_weight * throughput / base_throughput + rtt_weight * base_rtt / rtt + pfc_weight * (1 - pfc*pfc_pause_time/t_tune)


def rtt_handling2():
    monitor_rtt_node = [i for i in range(128)]
    sip_dip_time_matrix = np.zeros((len(monitor_rtt_node), len(monitor_rtt_node)))

    data = pd.read_table(rtt_input_file, header=None, sep='\s+', names=['time', 'sip-dip', 'rtt'], low_memory=False).dropna()
    for index, row in data.iterrows():
        current_time = row[0]
        sip_dip_pair = row[1]
        rtt_value = row[2] / 1000
        sip_node = int(sip_dip_pair.split('-')[0])
        dip_node = int(sip_dip_pair.split('-')[1])
        sip_dip_time_matrix[sip_node][dip_node] = rtt_value

    return sip_dip_time_matrix
    

def throughput_handling2():
    monitor_tor_id = [128, 129, 130, 131, 132, 133, 134, 135]
    monitor_tor_port = [i for i in range(1, 17)]

    data = pd.read_table(throughput_input_file, header=None, sep='\s+', names=['time', 'switch', 'port', 'rxBytes'], low_memory=False).dropna()
    throughput_switch_port_dict = {}
    throuhgput_time_switch_port_dict = {}

    for index, row in data.iterrows():
        metric_time = float(row[0])
        switch_index = row[1]
        port_id = row[2]
        rx_bytes = row[3]
        switch_port_key = str(switch_index) + '-' + str(port_id)

        if switch_index in monitor_tor_id and port_id in monitor_tor_port:
            throughput_switch_port_dict[switch_port_key] = rx_bytes
            throuhgput_time_switch_port_dict[switch_port_key] = metric_time
            
    
    throughput_matrix = np.zeros((len(monitor_tor_id), len(monitor_tor_port)))
    for key, value in throughput_switch_port_dict.items():
        switch_index = int(float(key.split('-')[0])) - monitor_tor_id[0]
        port_index = int(float(key.split('-')[1])) - 1
        throughput_value = value * 8 / t_tune / 1e9
        throughput_matrix[switch_index][port_index] = throughput_value

    return throughput_matrix

def pfc_handling(start_time_this_round, stop_time_this_round, start_line):
    node_if_pfc_count_matrix = np.zeros((1000, 1000))

    data = pd.read_table(pfc_input_file, header=None, sep='\s+', names=['time', 'node_id', 'node_type', 'if_index', 'pfc_type'], low_memory=False).dropna()
    global pfc_start_line
    last_pfc_time = 0

    for index, row in data[start_line:].iterrows():
        current_time = row[0]
        node_id = int(row[1])
        if_index = int(row[3])
        pfc_type = int(row[-1])

        if current_time < start_time_this_round:
            continue
        elif start_time_this_round <= current_time <= stop_time_this_round:
            if pfc_type == 20000:
                node_if_pfc_count_matrix[node_id][if_index] += 1
        else:
            pfc_start_line = index
            break

    return node_if_pfc_count_matrix


def get_new_solution_direction(parameter_mode):
    global current_flow_ratio, previous_flow_ratio

    if current_flow_ratio['large'] != 0 and current_flow_ratio['small'] != 0:
        large_flow_number = current_flow_ratio['large']
        small_flow_number = current_flow_ratio['small']
    else:
        large_flow_number = previous_flow_ratio['large']
        small_flow_number = previous_flow_ratio['small']
    
    large_flow_ratio = large_flow_number / (large_flow_number + small_flow_number)
    small_flow_ratio = small_flow_number / (large_flow_number + small_flow_number)
    random_number = random.random()

    if parameter_mode == 'aggressive':
        if random_number <= min(large_flow_ratio, 0.8):
            return 1
        else:
            return -1
    else:
        if random_number <= min(small_flow_ratio, 0.8):
            return 1
        else:
            return -1


def generate_new_parameters(parameter_mode, current_solution):
    new_solution = []
    for i in range(len(DCQCN_parameter)):
        parameter_name = DCQCN_parameter_name[i]
        parameter_index = DCQCN_name_index_mapping[parameter_name]

        if parameter_mode == 'default':
            new_solution.append(default_parameters[i])
            continue

        if parameter_name == 'time_reset' or parameter_name == 'rpg_min_dec_fac' or parameter_name == 'dce_tcp_g' or parameter_name == 'initial_alpha_value':
            if parameter_mode == 'aggressive':
                parameter_value = current_solution[parameter_index] - get_new_solution_direction(parameter_mode) * parameter_step[parameter_index] * random.uniform(0.5, 1)
            else:
                parameter_value = current_solution[parameter_index] + get_new_solution_direction(parameter_mode) * parameter_step[parameter_index] * random.uniform(0.5, 1)
        else:
            if parameter_mode == 'aggressive':
                parameter_value = current_solution[parameter_index] + get_new_solution_direction(parameter_mode) * parameter_step[parameter_index] * random.uniform(0.5, 1)
            else:
                parameter_value = current_solution[parameter_index] - get_new_solution_direction(parameter_mode) * parameter_step[parameter_index] * random.uniform(0.5, 1)


        if DCQCN_parameter_name[i] != 'rate_to_set_on_first_cnp' and DCQCN_parameter_name[i] != 'rpg_min_dec_fac':
            parameter_value = round(parameter_value)
        else:
            parameter_value = 1 if parameter_value > 1 else parameter_value

        if parameter_value > parameter_max_value[parameter_index]:
            parameter_value = parameter_max_value[parameter_index]

        if parameter_value < parameter_min_value[parameter_index]:
            parameter_value = parameter_min_value[parameter_index]

        new_solution.append(parameter_value)

    if new_solution[-2] > new_solution[-1]:
        temp = new_solution[-2]
        new_solution[-2] = new_solution[-1]
        new_solution[-1] = temp

    return new_solution

def judge_mode(large_flow_num, small_flow_num):
    if large_flow_num >= 1 * small_flow_num:
        throughput_weight = 0.5
        rtt_weight = 0.2
        pfc_weight = 0.3
        return 'aggressive', throughput_weight, rtt_weight, pfc_weight
    else:
        throughput_weight = 0.2
        rtt_weight = 0.5
        pfc_weight = 0.3
        return 'conservative', throughput_weight, rtt_weight, pfc_weight


def load_current_parameters():
    data = []
    with open('mix/parameter.txt', 'r') as file:
        lines = file.readlines()
        for line in lines:
            if line.strip():
                key, value = line.strip().split('=')
                if key == 'rpg_min_dec_fac' or key == 'rate_to_set_on_first_cnp':
                    data.append(float(value.strip()))
                else:
                    data.append(int(value.strip()))
    return data

def output_metric(current_time, avg_throughput, avg_rtt, avg_pfc, utility_value):
    with open(metric_output_file, 'a') as f:
        output_str = str(current_time) + ' ' + str(avg_throughput) + ' ' + str(avg_rtt) + ' ' + str(avg_pfc) + ' ' + str(utility_value)
        print(output_str, file=f)

def aggressive_tuning(throughput_weight, rtt_weight, pfc_weight, current_time):
    start_time_this_round = current_time - t_tune
    stop_time_this_round = current_time

    current_solution = load_current_parameters()
    throughput_matrix = throughput_handling2()
    rtt_matrix = rtt_handling2()
    pfc_matrix = pfc_handling(start_time_this_round, stop_time_this_round, pfc_start_line)

    new_avg_throughput = np.mean(throughput_matrix[throughput_matrix > 5])
    new_avg_rtt = np.mean(rtt_matrix[rtt_matrix > 0])
    if np.any(pfc_matrix > 0):
        new_avg_pfc = np.mean(pfc_matrix[pfc_matrix > 0])
    else:
        new_avg_pfc = 0
        
    current_value = -1
    output_metric(stop_time_this_round, new_avg_throughput, new_avg_rtt, new_avg_pfc, current_value)

    best_solution = current_solution.copy()
    best_value = current_value
    temperature = initial_temperature

    tuning_rounds = 0

    new_solution = generate_new_parameters('default', current_solution)
    with open(parameter_output_file, 'w') as f:
        for i in range(len(DCQCN_parameter)):
            output_parameter_str = DCQCN_parameter_name[i] + '=' + str(new_solution[i])
            print(output_parameter_str, file=f)

    while temperature > final_temperature:
        for i in range(attempt_times):
            start_time_this_round = stop_time_this_round
            stop_time_this_round += t_tune

            while True:
                temp_throughput_data = pd.read_table(throughput_input_file, header=None, sep='\s+', names=['time', 'switch', 'port', 'rxBytes'], low_memory=False).dropna()
                new_time = temp_throughput_data.iloc[-1, 0]

                if new_time > stop_time_this_round:
                    break
                else:
                    time.sleep(0.5)
            
            throughput_matrix = throughput_handling2()
            rtt_matrix = rtt_handling2()
            pfc_matrix = pfc_handling(start_time_this_round, stop_time_this_round, pfc_start_line)
            new_avg_throughput = np.mean(throughput_matrix[throughput_matrix > 5])
            new_avg_rtt = np.mean(rtt_matrix[rtt_matrix != 0])
            if np.any(pfc_matrix > 0):
                new_avg_pfc = np.mean(pfc_matrix[pfc_matrix > 0])
            else:
                new_avg_pfc = 0

            new_value = utility_function(new_avg_throughput, new_avg_rtt, new_avg_pfc, throughput_weight, rtt_weight, pfc_weight)
            output_metric(stop_time_this_round, new_avg_throughput, new_avg_rtt, new_avg_pfc, new_value)
            
            delta = new_value - current_value

            if delta > 0 or math.exp(delta / temperature) > random.random():
                current_solution = new_solution.copy()
                current_value = new_value

            if current_value > best_value:
                best_solution = current_solution.copy()
                best_value = current_value

            new_solution = generate_new_parameters('aggressive', current_solution)
            with open(parameter_output_file, 'w') as f:
                for i in range(len(DCQCN_parameter)):
                    output_parameter_str = DCQCN_parameter_name[i] + '=' + str(new_solution[i])
                    print(output_parameter_str, file=f)
        
            tuning_rounds += 1

        temperature *= cooling_rate
        
    with open(parameter_output_file, 'w') as f:
        for i in range(len(DCQCN_parameter)):
            output_parameter_str = DCQCN_parameter_name[i] + '=' + str(best_solution[i])
            print(output_parameter_str, file=f)


def conservative_tuning(throughput_weight, rtt_weight, pfc_weight, current_time):
    start_time_this_round = current_time - t_tune
    stop_time_this_round = current_time

    current_solution = load_current_parameters()
    throughput_matrix = throughput_handling2()
    rtt_matrix = rtt_handling2()
    pfc_matrix = pfc_handling(start_time_this_round, stop_time_this_round, pfc_start_line)

    new_avg_throughput = np.mean(throughput_matrix[throughput_matrix > 5])
    new_avg_rtt = np.mean(rtt_matrix[rtt_matrix != 0])
    if np.any(pfc_matrix > 0):
        new_avg_pfc = np.mean(pfc_matrix[pfc_matrix > 0])
    else:
        new_avg_pfc = 0

    current_value = -1
    output_metric(stop_time_this_round, new_avg_throughput, new_avg_rtt, new_avg_pfc, current_value)

    best_solution = current_solution.copy()
    best_value = current_value
    temperature = initial_temperature

    previous_avg_throughput = new_avg_throughput
    previous_avg_rtt = new_avg_rtt
    tuning_rounds = 0

    new_solution = generate_new_parameters('default', current_solution)
    with open(parameter_output_file, 'w') as f:
        for i in range(len(DCQCN_parameter)):
            output_parameter_str = DCQCN_parameter_name[i] + '=' + str(new_solution[i])
            print(output_parameter_str, file=f)

    while temperature > final_temperature:
        for i in range(attempt_times):
            start_time_this_round = stop_time_this_round
            stop_time_this_round += t_tune

            while True:
                temp_throughput_data = pd.read_table(throughput_input_file, header=None, sep='\s+', names=['time', 'switch', 'port', 'rxBytes'], low_memory=False).dropna()
                new_time = temp_throughput_data.iloc[-1, 0]

                if new_time > stop_time_this_round:
                    break
                else:
                    time.sleep(0.5)
            
            throughput_matrix = throughput_handling2()
            rtt_matrix = rtt_handling2()
            pfc_matrix = pfc_handling(start_time_this_round, stop_time_this_round, pfc_start_line)

            new_avg_throughput = np.mean(throughput_matrix[throughput_matrix > 5])
            new_avg_rtt = np.mean(rtt_matrix[rtt_matrix != 0])
            if np.any(pfc_matrix > 0):
                new_avg_pfc = np.mean(pfc_matrix[pfc_matrix > 0])
            else:
                new_avg_pfc = 0

            new_value = utility_function(new_avg_throughput, new_avg_rtt, new_avg_pfc, throughput_weight, rtt_weight, pfc_weight)
            output_metric(stop_time_this_round, new_avg_throughput, new_avg_rtt, new_avg_pfc, new_value)
            
            delta = new_value - current_value

            if delta > 0 or math.exp(delta / temperature) > random.random():
                current_solution = new_solution.copy()
                current_value = new_value

            if current_value > best_value:
                best_solution = current_solution.copy()
                best_value = current_value
            
            new_solution = generate_new_parameters('conservative', current_solution)
            with open(parameter_output_file, 'w') as f:
                for i in range(len(DCQCN_parameter)):
                    output_parameter_str = DCQCN_parameter_name[i] + '=' + str(new_solution[i])
                    print(output_parameter_str, file=f)
                    
            previous_avg_throughput = new_avg_throughput
            previous_avg_rtt = new_avg_rtt
            tuning_rounds += 1
        
        temperature *= cooling_rate

    with open(parameter_output_file, 'w') as f:
        for i in range(len(DCQCN_parameter)):
            output_parameter_str = DCQCN_parameter_name[i] + '=' + str(best_solution[i])
            print(output_parameter_str, file=f)

def start_tuning(current_time):
    global current_flow_ratio, total_tuning_rounds, is_tuning
    total_tuning_rounds += 1

    large_flow_num = current_flow_ratio['large']
    small_flow_num = current_flow_ratio['small']
    parameter_mode, throughput_weight, rtt_weight, pfc_weight = judge_mode(large_flow_num, small_flow_num)

    if parameter_mode == 'aggressive':
        aggressive_tuning(throughput_weight, rtt_weight, pfc_weight, current_time)
    else:
        conservative_tuning(throughput_weight, rtt_weight, pfc_weight, current_time)

    is_tuning = False

sketch_heavypart_path = 'mix/switch_sketch_heavypart.tr'
sketch_lightpart_path = 'mix/switch_sketch_lightpart.tr'
window_size = 2
t_reset = 0.001
large_flow_threshold = 1024
trigger_threshold = 0.01
is_tuning = False
switch_flowid_status_dict = {}
previous_sketch_hash = "100"
previous_flow_ratio = {'large': -1, 'small': -1}
current_flow_ratio = {'large': 0, 'small': 0}

base_throughput = 100
base_rtt = 40
t_tune = 0.001
pfc_pause_time = 0.000005

throughput_input_file = 'mix/switch_portrate.tr'
rtt_input_file = 'mix/rtt.tr'
pfc_input_file = 'mix/pfc.txt'
metric_output_file = 'mix/metric_output.tr'

rtt_start_line = 0
throughput_start_line = 0
pfc_start_line = 0
parameter_output_file = 'mix/parameter.txt'
aggressive_mode_flag = 0
conservative_mode_flag = 0


initial_temperature = 80
final_temperature = 10
attempt_times = 10
cooling_rate = 0.85

lower_bounds = [min(DCQCN_parameter[i]) for i in range(len(DCQCN_parameter))]
upper_bounds = [max(DCQCN_parameter[i]) for i in range(len(DCQCN_parameter))]

total_tuning_rounds = 0
total_monitor_rounds = 0

if os.path.exists(metric_output_file):
    os.remove(metric_output_file)

while True:
    current_sketch_hash = calculate_file_hash(sketch_heavypart_path)
    if current_sketch_hash != previous_sketch_hash:
        previous_sketch_hash = current_sketch_hash
        total_monitor_rounds += 1

        current_time, current_flowid_size_dict = load_sketch()
        update_all_flows(current_flowid_size_dict, current_time)
        switch_upload_dict = filter_flows()
        upload_ratio(switch_upload_dict, current_time)
        kl_divergence = compute_divergence(current_flow_ratio, previous_flow_ratio)

        if (kl_divergence > trigger_threshold and is_tuning == False) or previous_flow_ratio['small'] == -1:
            is_tuning = True
            tuning_thread = threading.Thread(target=start_tuning, args=(current_time,))
            tuning_thread.start()
            
        previous_flow_ratio.update(current_flow_ratio)
        current_flow_ratio['large'] = 0
        current_flow_ratio['small'] = 0

    else:
        time.sleep(3)
