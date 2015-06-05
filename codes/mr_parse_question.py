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
# MISSING = -sys.float_info.max
MISSING = None

# feature list
NUM_ASK = 0 
NUM_EXTENSION = 1
QL_ASK = 2
CATEGORIES = 3
FREQ_CATE = 4
DURATION = 5
PNT_ASK = 10
PNT_AWD_ASK = 15
# CATE_POP = 20
QUES_DICT = 21

# identities
IS_USER = 0
IS_QUES = 1
# IS_CATE = 2

# feature helpers
AVG = 0
MID = 1
MIN = 2
MAX = 3
STD = 4

# user list
USER_FEATURES = [NUM_ASK, CATEGORIES, NUM_EXTENSION,
                 PNT_ASK, PNT_AWD_ASK, DURATION, QL_ASK]

QUES_FEATURES = [QUES_DICT]
# category list
# CATEGORY_FEATURES = [CATE_POP]

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


class MRParseQuestions(MRJob):
  
  def mapper(self, _, line):
    question = line.strip().replace("|", "\t").split("\t")
    if question[ASKER_ID] != "" and question[Q_ID] != "":
      yield question[ASKER_ID], (NUM_ASK, 1)
      yield question[ASKER_ID], (CATEGORIES, question[CATE_ID])
      # missing value
      point = np.nan
      if question[POINTS_ASKER] != "":
        point = float(question[POINTS_ASKER])
      yield question[ASKER_ID], (PNT_ASK, point)
      point_awd = np.nan
      if question[POINTS_AWARDED] != "":
        point_awd = float(question[POINTS_AWARDED])
      yield question[ASKER_ID], (PNT_AWD_ASK, point_awd)
      yield question[ASKER_ID], (NUM_EXTENSION, int(question[EXTENSION_DATE] != ""))
      duration = np.nan
      if question[AWARD_DATE] != "" and question[CREAT_DATE] != "":
        duration=float(question[AWARD_DATE])-float(question[CREAT_DATE])
        if duration < 0:
          duration = np.nan
      yield question[ASKER_ID], (DURATION, duration)
      yield question[ASKER_ID], (QL_ASK, question[Q_ID])
      # if question[CATE_ID] != "":
      #   yield question[CATE_ID], (CATE_POP, 1)
      yield question[Q_ID], (QUES_DICT, [question[ASKER_ID],question[CATE_ID],point_awd])

  def combiner(self, obs, results):
    l = list(results)
    dic = {NUM_ASK: 0, NUM_EXTENSION: 0} #CATE_POP: 0
    identity = IS_USER
    for key, val in l:
      if key in QUES_FEATURES:
        identity = IS_QUES
      # if key == CATE_POP:
      #   identity = IS_CATE
      # for sums
      if key in [NUM_ASK, NUM_EXTENSION]: #CATE_POP
        dic[key] += val
      # don't use combiner
      elif key in [CATEGORIES, QL_ASK, PNT_ASK, PNT_AWD_ASK, DURATION, QUES_DICT]:
        yield obs, (key, val)
    # if identity == IS_CATE:
    #   yield obs, (CATE_POP, dic[CATE_POP])
    if identity == IS_USER:
      yield obs, (NUM_ASK, dic[NUM_ASK])
      yield obs, (NUM_EXTENSION, dic[NUM_EXTENSION])

  def reducer(self, obs, results):
    l = list(results)
    dic = {NUM_ASK: 0, NUM_EXTENSION: 0, \
          #CATE_POP: 0,\
          DURATION:[], PNT_ASK:[], PNT_AWD_ASK:[], QL_ASK:[]}
    identity = IS_USER
    cate_dic = {}
    for key, val in l:
      if key in QUES_FEATURES:
        identity = IS_QUES
        yield obs, (key, val)
      # if key == CATE_POP:
      #   identity = IS_CATE
      # for sums
      if key in [NUM_ASK, NUM_EXTENSION]: #CATE_POP
        dic[key] += val
      elif key in [QL_ASK]:
        dic[key] += [val]
      elif key in [CATEGORIES]:
        if val != "":
          cate_dic[val] = cate_dic.get(val, 0) + 1
      elif key in [DURATION, PNT_ASK, PNT_AWD_ASK]:
        dic[key].append(val)
    # if identity == IS_CATE:
    #   yield obs, (CATE_POP, dic[CATE_POP])
    if identity == IS_USER:
      for feature in [DURATION, PNT_ASK, PNT_AWD_ASK]:
        dic[feature] = filter(lambda x: x==x, dic[feature])
        dic[feature] = list_helper(dic[feature])
        for key in dic[feature]:
          yield obs, (feature+key, dic[feature][key])
      for feature in [NUM_ASK, NUM_EXTENSION, QL_ASK]:
        yield obs, (feature, dic[feature])
      yield obs, (CATEGORIES, len(cate_dic))
      frequency = [(y,x) for (x,y) in cate_dic.items()]
      frequency.sort(reverse=True)
      yield obs, (FREQ_CATE, frequency[0][1])

if __name__ == '__main__':
  MRParseQuestions.run()
