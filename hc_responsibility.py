# given a set of DCs from holoclean, try deleting a set of rules
# until we get the expected label
from examples.holoclean_repair_example import main 
import psycopg2
from itertools import combinations
import pickle
import time
# print(all_rules)




def retrain(filename, new_rules, attr_name, tid):
	
	conn = psycopg2.connect(dbname="holo", user="holocleanuser", password="abcd1234")
	cur = conn.cursor()
	with open(filename+'.txt', 'w') as file:
		file.write(''.join(new_rules))
	file.close()
	print(f"filename:{filename}")
	print(f"new_rules: {new_rules}")
	main(filename)

	query = f"""
	SELECT t1._tid_ FROM  "hospital_repaired" as t1, "hospital_clean" as t2 
	WHERE t1._tid_ = t2._tid_ AND t2._attribute_ = '{attr_name}' and t1."{attr_name}"!=t2._value_;
	"""

	cur.execute(query)
	results = cur.fetchall()
	# print("!!!!!results!!!!!!!!!")
	print(results)
	conn.close()
	if((tid,) in results):
		return False
	else:
		return True

def rule_responsibility(attr_name,tid, size):

	start_time = time.time()
	rule_dir = '/home/opc/chenjie/holoclean/testdata/'
	file = open(rule_dir+f'dc_finder_hospital_rules_{size}.txt', mode='r', encoding = "ISO-8859-1")

	all_rules = file.readlines()

	rule_contingencies  = {}
	contingency_cand_dict = {}
	responsibilities = {f:[-1] for f in all_rules}
	model_results = {}


	rule_contingencies[all_rules[-1]]=[]
	for i in range(0, len(all_rules)):
		rule_contingencies[all_rules[i]]=[]
		contingency_cand_dict[all_rules[i]] = [fc for fc in all_rules if fc!=all_rules[i]]

	# iterate over each function, using a heap structure to
	# track the smallest function with its responsibility
	# if the highest possible responsibility of the current iteration
	# is smaller than the smallest element in the heap root, then
	# we can early stop
	try_ind = 0
	for j in range(0, 2):
		for f in all_rules:
			try_ind+=1
			f_contingency_cands = contingency_cand_dict[f]

			cand_cnt = 1
			total_cands = len(list(combinations(f_contingency_cands,j)))
			for con in combinations(f_contingency_cands,j):
				print(f"progress: combsize={j}, current: {cand_cnt}/{total_cands}")
				# contigency candidate, that is the set that needs to be
				# removed first before f being removed
				cause_cand = list(con)
				contingency_cand = frozenset(cause_cand)
				# already_cached=False
				cause_cand.append(f)
				print("!!!!!!!!!!!!!!!!!!!!1cause cand!!!!!!!!!!!!!!!!!1")
				print(cause_cand)
				rule_contingencies[f].append(cause_cand)
				model_funs = [mf for mf in all_rules if (mf not in cause_cand)]
				cause_set = frozenset(cause_cand)

				if(cause_set in model_results):
					print(f"{cause_set} in model_results, and is {model_results[cause_set]}")
					# look_up_cnt+=1
					# already_cached=True
					result = model_results[cause_set]
					# print(f"{cause_set} is already_cached and is {result}")
				else:
					# new_model_cnt+=1
					print("$$$$$retraining!!!!$$$$$$$$$$$$$$$4")
					result = retrain(filename=f'/home/opc/chenjie/holoclean/testdata/dc_finder_hospital_rules_{size}_test', new_rules=model_funs, attr_name=attr_name, tid=tid)
					model_results[cause_set]=result
					# logger.critical(f'model_results len : {len(model_results)}')
					# logger.critical(f"after training using {model_funs}: we get {flabel}")
				if(result):
					# if(not already_cached):
					# 	conn = psycopg2.connect(dbname="holo", user="holocleanuser", password="abcd1234")
					# 	conn.autocommit = True
					# 	cur = conn.cursor()
					# 	cur.execute(f'ALTER TABLE hospital_repaired RENAME TO hospital_repaired_{try_ind}')
					# 	conn.close()
					# 	print(f"cause: {cause_cand} flipped to correct result, saved results to hospital_repaired_{try_ind}")
					# logger.critical(f'flipped to {flabel}')
					if(len(contingency_cand)>0):
						if(responsibilities[f][0]==-1):
							if(contingency_cand not in model_results):
								model_funs = [mf for mf in all_rules if (mf not in contingency_cand)]
								result = retrain(filename=f'/home/opc/chenjie/holoclean/testdata/dc_finder_hospital_rules_{size}_test', new_rules=model_funs, attr_name=attr_name, tid=tid)
								# if(result):
									# try_ind+=1
									# conn = psycopg2.connect(dbname="holo", user="holocleanuser", password="abcd1234")
									# conn.autocommit = True
									# cur = conn.cursor()
									# cur.execute(f'ALTER TABLE hospital_repaired RENAME TO hospital_repaired_{try_ind}')
									# conn.close()
									# print(f"cause: {contingency_cand} flipped to correct result, saved results to hospital_repaired_{try_ind}")
								model_results[contingency_cand] = result
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
				print("model_results:")
				print(model_results)
				cand_cnt+=1
		# print(f'we are done with {f}')
		print(model_results)
		# print(rule_contingencies)
		print(responsibilities)
		responsibilities['time']=round(time.time() - start_time,2)

		with open(f'result_{size}.pickle', 'wb') as handle:
		    pickle.dump(responsibilities, handle)

# # print(query)

# attributes = ["ProviderNumber","HospitalName","Address1","City","State","ZipCode","CountyName","PhoneNumber","HospitalType","HospitalOwner","EmergencyService",
# "Condition","MeasureCode","MeasureName","Score","Sample","Stateavg"]

# for a in attributes:
# 	q = f"""
# 	SELECT t1._tid_, t1.{a} as repaired_{a}_val, t2.{a} as ground_{a}_val FROM  "hospital_repaired" as t1, "hospital_clean" as t2 
# 	WHERE t1._tid_ = t2._tid_ AND t2._attribute_ = {a} AND t1.{a} != t2._value_
# 	"""