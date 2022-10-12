# given a set of DCs from holoclean, try deleting a set of rules
# until we get the expected label
import logging
from holoclean.examples.holoclean_repair import main 
import psycopg2
from itertools import combinations
import pickle
import time
from datetime import datetime
from labelling_explanation.dc_src.src.classes import Complaint, RulePruner, DataPruner
from holoclean.helper_functions import gen_gt_given_tids
import random

def random_subset(s):
	# return s
    out = []
    for el in s:                                                                                                                    
        # random coin flip
        if(random.random()<=1/len(s)):
            out.append(el)

    return out

def merge_timer(dc_timer, holo_timer):
	for k in holo_timer.params:
		dc_timer.params[k]+=holo_timer.params[k]

def retrain(dc_input, new_rules, complaint, pruned=True):	
	conn = dc_input.connection
	cur = conn.cursor()
	# logging.debug(f"filename:{filename}")
	# logging.debug(f"new_rules: {new_rules}")
	# main(filename)
	table_name=dc_input.input_csv_file.split('.')[0]
	filename=dc_input.input_dc_dir+'subset_'+dc_input.input_dc_file
	with open(filename, 'w') as file:
		file.write(''.join(new_rules))
	file.close()

	attr_name=complaint.attr_name
	tid=complaint.tid
	
	holo_timer, holo_query_timer=main(table_name='pruned_'+table_name, csv_dir=dc_input.input_csv_dir,
	    csv_file='pruned_'+dc_input.input_csv_file, dc_dir=dc_input.input_dc_dir, dc_file='subset_'+dc_input.input_dc_file, gt_dir=dc_input.ground_truth_dir, 
	    gt_file='pruned_'+dc_input.ground_truth_file, initial_training=False, pruned=True)

	query = f"""
	SELECT t1._tid_ FROM  "pruned_{table_name}_repaired" as t1, "pruned_{table_name}_clean" as t2 
	WHERE t1._tid_ = t2._tid_ AND t2._attribute_ = '{attr_name}' and t1."{attr_name}"!=t2._value_;
	"""

	print(query)

	merge_timer(dc_input.stats, holo_timer)
	merge_timer(dc_input.stats, holo_query_timer)

	cur.execute(query)
	results = cur.fetchall()
	print("result")
	print(results)
	# logging.debug("!!!!!results!!!!!!!!!")
	logging.debug(results)
	# conn.close()
	if((tid,) in results):
		return False
	else:
		return True


def rule_responsibility(complaint, dc_input):

	input_file=dc_input.input_dc_dir+dc_input.input_dc_file
	dataset_name=dc_input.input_csv_file.split('.')[0]
	file = open(f'{input_file}', mode='r', encoding = "ISO-8859-1")

	all_rules = file.readlines()

	rule_contingencies  = {}
	contingency_cand_dict = {}
	responsibilities = {f:[-1] for f in all_rules}
	model_results = {}

	# if prune, we only consider rules directly 
	# related to the complaint attribute, and only
	# use those as contingency candidate generation step
	# for each rule. Note in retraining step we still
	# use all rules.

	# if(dc_input.prune_rules):
	rp=RulePruner()
	dp=DataPruner()
	_, rules_after_pruning = rp.prune_and_return(complaint=complaint, rules=all_rules)
	print('After prunning rules, the rules we consider are:')
	print(rules_after_pruning)
	tids, res_df = dp.dc_prune_and_return(db_conn=dc_input.connection,target_table=dataset_name,pruned_rules=rules_after_pruning)

	logging.debug(f'After prunning dataset, we have {len(res_df)} rows')
	new_pruned_dataset_name = dc_input.input_csv_dir+'pruned_'+dc_input.input_csv_file
	res_df.to_csv(new_pruned_dataset_name, index=False)
	gt_df = gen_gt_given_tids(res_df)
	new_gt_file = dc_input.ground_truth_dir+'pruned_'+dc_input.ground_truth_file
	gt_df.to_csv(new_gt_file, index=False)

	for i in range(0, len(rules_after_pruning)):
		rule_contingencies[rules_after_pruning[i]]=[]
		contingency_cand_dict[rules_after_pruning[i]] = [fc for fc in rules_after_pruning if fc!=rules_after_pruning[i]]
	print(f"contingency_cand_dict: {contingency_cand_dict}")
	if(dc_input.prune_rules):
		for f in rules_after_pruning:
			print(f"current rule: {f}")
			for j in range(0, dc_input.contingency_sample_times):
				f_contingency_cands = contingency_cand_dict[f]
				cause_cand = random_subset(f_contingency_cands)
				print(f'random_subset result number {j}:')
				print(cause_cand)
				contingency_cand = frozenset(cause_cand)
				cause_cand.append(f)
				rule_contingencies[f].append(cause_cand)
				model_funs = [mf for mf in all_rules if (mf not in cause_cand)]
				cause_set = frozenset(cause_cand)
				if(cause_set in model_results):
					logging.debug(f"{cause_set} in model_results, and is {model_results[cause_set]}")
					# look_up_cnt+=1
					# already_cached=True
					result = model_results[cause_set]
					dc_input.stats.incr('lookup_count')
					logging.debug(f'look up: {cause_set}')
						# logging.debug(f"{cause_set} is already_cached and is {result}")
				else:
					# new_model_cnt+=1
					logging.debug("$$$$$retraining!!!!$$$$$$$$$$$$$$$4")
					dc_input.stats.startTimer('retrain')
					result = retrain(dc_input=dc_input,new_rules=model_funs, complaint=complaint, pruned=True)
					dc_input.stats.incr('count_retrains')
					logging.debug(f"retrain using :{model_funs}")
					dc_input.stats.stopTimer('retrain')
					model_results[cause_set]=result
				if(result):
					if(len(contingency_cand)>0):
						if(responsibilities[f][0]==-1):
							if(contingency_cand not in model_results):
								model_funs = [mf for mf in all_rules if (mf not in contingency_cand)]
								dc_input.stats.startTimer('retrain')
								result = retrain(dc_input=dc_input,new_rules=model_funs, complaint=complaint, pruned=True)
								dc_input.stats.incr('count_retrains')
								logging.debug(f"retrain using :{model_funs}")
								dc_input.stats.stopTimer('retrain')
								model_results[contingency_cand] = result
							else:
								logging.debug(f'lookup: {contingency_cand}')
								dc_input.stats.incr('lookup_count')
							if(not model_results[contingency_cand]):
								responsibilities[f][0]=1/len(cause_cand)
								responsibilities[f].append(cause_cand)
								responsibility = 1/len(cause_cand)
								# logger.critical((responsibility, f))
								# heapq.heappush(heap_list, (responsibility, id(f), f))
								break
					else:
						responsibilities[f][0]=1
						break
		# with open(f'result_{datetime.now().strftime("%d-%m-%Y-%H-%M-%S")}.pickle', 'wb') as handle:
		#     pickle.dump(responsibilities, handle)
	else:
		for f in rules_after_pruning:
			found_responsibility=False
			for j in range(0, dc_input.contingency_size_threshold):
				f_contingency_cands = contingency_cand_dict[f]
				total_cands = len(list(combinations(f_contingency_cands,j)))
				print(f"total_cands: when j={j}:{total_cands}")
				for con in combinations(f_contingency_cands,j):
					# contigency candidate, that is the set that needs to be
					# removed first before f being removed
					cause_cand = list(con)
					print(f"current contingency_cand: {cause_cand}")
					contingency_cand = frozenset(cause_cand)
					# already_cached=False
					cause_cand.append(f)
					logging.debug("!!!!!!!!!!!!!!!!!!!!1cause cand!!!!!!!!!!!!!!!!!1")
					logging.debug(cause_cand)
					rule_contingencies[f].append(cause_cand)
					model_funs = [mf for mf in all_rules if (mf not in cause_cand)]
					cause_set = frozenset(cause_cand)

					if(cause_set in model_results):
						logging.debug(f"{cause_set} in model_results, and is {model_results[cause_set]}")
						# look_up_cnt+=1
						# already_cached=True
						result = model_results[cause_set]
						dc_input.stats.incr('lookup_count')
						logging.debug(f'look up: {cause_set}')
						# logging.debug(f"{cause_set} is already_cached and is {result}")
					else:
						# new_model_cnt+=1
						logging.debug("$$$$$retraining!!!!$$$$$$$$$$$$$$$4")
						dc_input.stats.startTimer('retrain')
						result = retrain(dc_input=dc_input,new_rules=model_funs, complaint=complaint, pruned=False)
						dc_input.stats.incr('count_retrains')
						logging.debug(f"retrain using :{model_funs}")
						dc_input.stats.stopTimer('retrain')
						model_results[cause_set]=result
					if(result):
						if(len(contingency_cand)>0):
							if(responsibilities[f][0]==-1):
								if(contingency_cand not in model_results):
									model_funs = [mf for mf in all_rules if (mf not in contingency_cand)]
									dc_input.stats.startTimer('retrain')
									result = retrain(dc_input=dc_input,new_rules=model_funs, complaint=complaint, pruned=True)
									dc_input.stats.incr('count_retrains')
									logging.debug(f"retrain using :{model_funs}")
									dc_input.stats.stopTimer('retrain')
									model_results[contingency_cand] = result
								else:
									logging.debug(f'lookup: {contingency_cand}')
									dc_input.stats.incr('lookup_count')
								if(not model_results[contingency_cand]):
									responsibilities[f][0]=1/len(cause_cand)
									responsibilities[f].append(cause_cand)
									responsibility = 1/len(cause_cand)
									print(f"found a responsibility: {responsibility}")
									# logger.critical((responsibility, f))
									# heapq.heappush(heap_list, (responsibility, id(f), f))
									found_responsibility=True
									break
						else:
							print(f"found a responsibility: 1")
							responsibilities[f][0]=1
							found_responsibility=True
							break
			# logging.debug("model_results:")
			# logging.debug(model_results)
				if(found_responsibility):
					break
	logging.debug('model_results')
	logging.debug(model_results)
	# logging.debug(rule_contingencies)
	print(dc_input.stats.formatStats())
	print(responsibilities)


# # logging.debug(query)

# attributes = ["ProviderNumber","HospitalName","Address1","City","State","ZipCode","CountyName","PhoneNumber","HospitalType","HospitalOwner","EmergencyService",
# "Condition","MeasureCode","MeasureName","Score","Sample","Stateavg"]

# for a in attributes:
# 	q = f"""
# 	SELECT t1._tid_, t1.{a} as repaired_{a}_val, t2.{a} as ground_{a}_val FROM  "hospital_repaired" as t1, "hospital_clean" as t2 
# 	WHERE t1._tid_ = t2._tid_ AND t2._attribute_ = {a} AND t1.{a} != t2._value_
# 	"""