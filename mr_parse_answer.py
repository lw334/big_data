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
# MISSING = -sys.float_info.max
MISSING = None

# feature helpers
AVG = 0
MID = 1
MIN = 2
MAX = 3
STD = 4

# feature list
NUM_ANSWER = 20
Q_ANSWER = 21
PNT_ANSWER = 22
QUES_NUM_ANSWER = 0
QUES_FIRST_ANS = 1

# user list
USER_FEATURES = [NUM_ANSWER, PNT_ANSWER, Q_ANSWER]

# question list
QUESTION_FEATURES = [QUES_NUM_ANSWER, QUES_FIRST_ANS]

# helper functions
def list_helper(l):
  rv = {}
  if len(l) == 0:
    rv[AVG] = MISSING
    rv[MID] = MISSING
    rv[MIN] = MISSING
    rv[MAX] = MISSING
    rv[STD] = MISSING
  else:
    rv[AVG] = np.mean(l)
    rv[MID] = np.median(l)
    rv[MIN] = np.min(l)
    rv[MAX] = np.max(l)
    rv[STD] = np.std(l)
  return rv


class MRParseAnswers(MRJob):
  
  def mapper(self, _, line):
    answer = line.strip().replace("|", "\t").split("\t")
    if len(answer) > 1 and answer[Q_ID] != "" and answer[ANSWERERID] != "":
      yield answer[ANSWERERID], (NUM_ANSWER, 1)
      # missing value
      point = np.nan
      if answer[POINTSSNAP] != "":
        point = float(answer[POINTSSNAP])
      yield answer[ANSWERERID], (PNT_ANSWER, point)
      yield answer[ANSWERERID], (Q_ANSWER, answer[Q_ID])
      yield answer[Q_ID], (QUES_NUM_ANSWER, 1)
      date = -1
      if answer[ANSWERDATE] != "":
        date = float(answer[ANSWERDATE])
      yield answer[Q_ID], (QUES_FIRST_ANS, (answer[ANSWERERID], date))

  def combiner(self, obs, results):
    l = list(results)
    dic = {NUM_ANSWER: 0, QUES_NUM_ANSWER: 0, QUES_FIRST_ANS: (" ", sys.maxint)}
    is_user = False
    for key, val in l:
      if key in USER_FEATURES:
        is_user = True
      # for sums
      if key in [NUM_ANSWER, QUES_NUM_ANSWER]:
        dic[key] += val
      # for min
      elif key in [QUES_FIRST_ANS]:
        if val[1] >= 0 and val[1] < dic[key][1]:
          dic[key] = val
      # don't use combiner for averages
      elif key in [PNT_ANSWER, Q_ANSWER]:
        yield obs, (key, val)
    if is_user:
        yield obs, (NUM_ANSWER, dic[NUM_ANSWER])
    else:
        yield obs, (QUES_NUM_ANSWER, dic[QUES_NUM_ANSWER])
        yield obs, (QUES_FIRST_ANS, dic[QUES_FIRST_ANS])

  def reducer(self, obs, results):
    l = list(results)
    dic = {NUM_ANSWER: 0, QUES_NUM_ANSWER: 0, \
          QUES_FIRST_ANS: (" ", sys.maxint), Q_ANSWER: []}
    temp = []
    is_user = False
    for key, val in l:
      if key in USER_FEATURES:
        is_user = True
      if key in [NUM_ANSWER, QUES_NUM_ANSWER]:
        dic[key] += val
      elif key in [QUES_FIRST_ANS]:
        if val[1] >= 0 and val[1] < dic[key][1]:
          dic[key] = val
      elif key in [Q_ANSWER]:
        dic[key] += [val]
      elif key in [PNT_ANSWER]:
        temp.append(val)
    temp = filter(lambda x: x==x, temp)
    point_features = list_helper(temp)
    if is_user:
      yield obs, (NUM_ANSWER, dic[NUM_ANSWER])
      for key in point_features:
        yield obs, (PNT_ANSWER+key, point_features[key])
      yield obs, (Q_ANSWER, dic[Q_ANSWER])
    else:
      yield obs, (QUES_NUM_ANSWER, dic[QUES_NUM_ANSWER])
      yield obs, (QUES_FIRST_ANS, dic[QUES_FIRST_ANS][0])

if __name__ == '__main__':
  MRParseAnswers.run()