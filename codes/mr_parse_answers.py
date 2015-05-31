from mrjob.job import MRJob
import re, sys
import numpy as np

# data imported
Q_ID = 0
ANSWERERID = 1
POINTSSNAP = 2
ANSWERDATE = 3

# missing value
# use a large negative value if any information is missing
MISSING = -sys.float_info.max

# user list
USER_FEATURES = ["u_num_answer", "u_pnt_answer", "u_qid_answer"]

# question list
QUESTION_FEATURES = ["q_num_answer", "q_first_answerer"]

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


class MRParseAnswers(MRJob):
  
  def mapper(self, _, line):
    answer = line.strip().replace("|", "\t").split("\t")
    if len(answer) > 1 and answer[Q_ID] != "" and answer[ANSWERERID] != "":
      yield answer[ANSWERERID], ("u_num_answer", 1)
      # missing value
      point = np.nan
      if answer[POINTSSNAP] != "":
        point = float(answer[POINTSSNAP])
      yield answer[ANSWERERID], ("u_pnt_answer", point)
      yield answer[ANSWERERID], ("u_qid_answer", answer[Q_ID])
      yield answer[Q_ID], ("q_num_answer", 1)
      date = -MISSING
      if answer[ANSWERDATE] != "":
        date = float(answer[ANSWERDATE])
      yield answer[Q_ID], ("q_first_answerer", (answer[ANSWERERID], date))

  def combiner(self, obs, results):
    l = list(results)
    dic = {"u_num_answer": 0, "q_num_answer": 0, "q_first_answerer": (" ", sys.maxint)}
    is_user = False
    for key, val in l:
      if key in USER_FEATURES:
        is_user = True
      # for sums
      if key in ["u_num_answer", "q_num_answer"]:
        dic[key] += val
      # for min
      elif key in ["q_first_answerer"]:
        if val[1] < dic[key][1]:
          dic[key] = val
      # don't use combiner for averages
      elif key in ["u_pnt_answer", "u_qid_answer"]:
        yield obs, (key, val)
    if is_user:
        yield obs, ("u_num_answer", dic["u_num_answer"])
    else:
        yield obs, ("q_num_answer", dic["q_num_answer"])
        yield obs, ("q_first_answerer", dic["q_first_answerer"])

  def reducer(self, obs, results):
    l = list(results)
    dic = {"u_num_answer": 0, "q_num_answer": 0, \
          "q_first_answerer": (" ", sys.maxint), "u_qid_answer": []}
    temp = []
    is_user = False
    for key, val in l:
      if key in USER_FEATURES:
        is_user = True
      if key in ["u_num_answer", "q_num_answer"]:
        dic[key] += val
      elif key in ["q_first_answerer"]:
        if val[1] < dic[key][1]:
          dic[key] = val
      elif key in ["u_qid_answer"]:
        dic[key] += [val]
      elif key in ["u_pnt_answer"]:
        temp.append(val)
    temp = filter(lambda x: x==x, temp)
    point_features = list_helper(temp)
    if is_user:
      yield obs, ("u_num_answer", dic["u_num_answer"])
      for key in point_features:
        yield obs, ("u_pnt_answer_"+key, point_features[key])
      yield obs, ("u_qid_answer", dic["u_qid_answer"])
    else:
      yield obs, ("q_num_answer", dic["q_num_answer"])
      yield obs, ("q_first_answerer", dic["q_first_answerer"][0])

if __name__ == '__main__':
  MRParseAnswers.run()