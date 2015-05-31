from mrjob.job import MRJob
import re, sys
import numpy as np

# data imported
Q_ID = 0
ASKER_ID = 1
CATE_ID = 2
POINTS_AWARDED = 3
CREAT_DATE = 4
QUESTION_STATE = 5
EXPIRE_DATE = 6
POINTS_ASKER = 7
EXTENSION_DATE = 8
AWARD_DATE = 9

# missing value
# use a large negative value if any information is missing
MISSING = -sys.float_info.max

# user list
USER_FEATURES = ["u_num_ask", "u_categories", "u_num_extension",
                 "u_pnt_ask", "u_pnt_awd_ask", "u_duration", "u_qid_ask"]

# question list
QUESTION_FEATURES = ["q_feature_dict"]

# category list
CATEGORY_FEATURES = ["c_pop"]

# identities
IS_USER = 0
IS_QUESTION = 1
IS_CATE = 2

# helper functions
def list_helper(l):
  rv = {}
  if len(l) == 0:
    rv["avg"] = MISSING
    rv["mid"] = MISSING
    rv["min"] = MISSING
    rv["max"] = MISSING
    rv["std"] = MISSING
  else:
    rv["avg"] = np.mean(l)
    rv["mid"] = np.median(l)
    rv["min"] = np.min(l)
    rv["max"] = np.max(l)
    rv["std"] = np.std(l)
  return rv


class MRParseQuestions(MRJob):
  
  def mapper(self, _, line):
    question = line.strip().replace("|", "\t").split("\t")
    if question[ASKER_ID] != "" and question[Q_ID] != "":
      yield question[ASKER_ID], ("u_num_ask", 1)
      yield question[ASKER_ID], ("u_categories", question[CATE_ID])
      # missing value
      point = np.nan
      if question[POINTS_ASKER] != "":
        point = float(question[POINTS_ASKER])
      yield question[ASKER_ID], ("u_pnt_ask", point)
      point_awd = np.nan
      if question[POINTS_AWARDED] != "":
        point_awd = float(question[POINTS_AWARDED])
      yield question[ASKER_ID], ("u_pnt_awd_ask", point_awd)
      yield question[ASKER_ID], ("u_num_extension", int(question[EXTENSION_DATE] != ""))
      duration = np.nan
      if question[AWARD_DATE] != "" and question[CREAT_DATE] != "":
        duration=float(question[AWARD_DATE])-float(question[CREAT_DATE])
        if duration < 0:
          duration = np.nan
      yield question[ASKER_ID], ("u_duration", duration)
      yield question[ASKER_ID], ("u_qid_ask", question[Q_ID])
      feature_dict = {"q_asker": question[ASKER_ID], "q_category": question[CATE_ID],\
                      "q_pnt_award": point_awd, "q_has_extension":int(question[EXTENSION_DATE] != ""),\
                      "q_duration": duration}
      yield question[Q_ID], ("q_feature_dict", feature_dict)
      if question[CATE_ID] != "":
        yield question[CATE_ID], ("c_pop", 1)

  def combiner(self, obs, results):
    l = list(results)
    dic = {"u_num_ask": 0, "u_num_extension": 0, "c_pop": 0}
    identity = IS_USER
    for key, val in l:
      if key == "q_feature_dict":
        identity = IS_QUESTION
      elif key == "c_pop":
        identity = IS_CATE
      # for sums
      if key in ["u_num_ask", "u_num_extension", "c_pop"]:
        dic[key] += val
      # don't use combiner
      elif key in ["u_categories", "u_qid_ask", "u_pnt_ask", "u_pnt_awd_ask", \
      "u_duration", "q_feature_dict"]:
        yield obs, (key, val)
    if identity == IS_CATE:
      yield obs, ("c_pop", dic["c_pop"])
    elif identity == IS_USER:
      yield obs, ("u_num_ask", dic["u_num_ask"])
      yield obs, ("u_num_extension", dic["u_num_extension"])

  def reducer(self, obs, results):
    l = list(results)
    dic = {"u_num_ask": 0, "u_num_extension": 0, "c_pop": 0,\
          "u_duration":[], "u_pnt_ask":[], "u_pnt_awd_ask":[], "u_qid_ask":[]}
    identity = IS_USER
    cate_dic = {}
    for key, val in l:
      if key == "q_feature_dict":
        identity = IS_QUESTION
      elif key == "c_pop":
        identity = IS_CATE
      # for sums
      if key in ["u_num_ask", "u_num_extension", "c_pop"]:
        dic[key] += val
      elif key in ["u_qid_ask"]:
        dic[key] += [val]
      elif key in ["u_categories"]:
        if val != "":
          cate_dic[val] = cate_dic.get(val, 0) + 1
      elif key in ["u_duration", "u_pnt_ask", "u_pnt_awd_ask"]:
        dic[key].append(val)
      elif key in ["q_feature_dict"]:
        yield obs, (key, val)
    if identity == IS_CATE:
      yield obs, ("c_pop", dic["c_pop"])
    elif identity == IS_USER:
      for feature in ["u_duration", "u_pnt_ask", "u_pnt_awd_ask"]:
        dic[feature] = filter(lambda x: x==x, dic[feature])
        dic[feature] = list_helper(dic[feature])
        for key in dic[feature]:
          yield obs, (feature+"_"+key, dic[feature][key])
      for feature in ["u_num_ask", "u_num_extension", "u_qid_ask"]:
        yield obs, (feature, dic[feature])
      yield obs, ("u_num_cate_ask", len(cate_dic))
      frequency = [(y,x) for (x,y) in cate_dic.items()]
      frequency.sort(reverse=True)
      yield obs, ("u_freq_cate_ask", frequency[0][1])

if __name__ == '__main__':
  MRParseQuestions.run()