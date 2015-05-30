from mrjob.job import MRJob
import numpy as np

QL_ANSWER = 21
QL_ASK = 2
CATEGORIES = 28
FREQ_CATE = 29
PNT_AWD = 30
NUM_ASKER = 35
POP_ANSWER = 36##
POP_ASK = 41##

QUES_ASKER = 0
QUES_CATE = 1
QUES_PNT_AWD = 2
QUES_POP = 3

MISSING = None

# feature helpers
AVG = 0
MID = 1
MIN = 2
MAX = 3
STD = 4

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

def parse(filename):
  rv = {}
  f = open(filename, "r")
  for line in f:
    l = line.strip().split("|")
    rv[l[0]] = l[1:]
  f.close()
  return rv

class MRFeatures(MRJob):

  def configure_options(self):
    super(MRFeatures, self).configure_options()
    self.add_file_option('--question')

  def mapper_init(self):
    self.question_profile = parse(self.options.question)

  def mapper(self, _, line):
    user = line.strip().split("|")
    questions = user[1:][QL_ANSWER].strip("[").strip("]").split(",")
    for i in questions:
      i = i.strip("'")
      if i in self.question_profile:
        yield user[0], (CATEGORIES, self.question_profile[i][QUES_CATE])
        point_awd = self.question_profile[i][QUES_PNT_AWD]
        if point_awd != "":
          point_awd = float(point_awd)
        else:
          point_awd = np.nan
        pop_answer = self.question_profile[i][QUES_POP]
        if pop_answer != "":
          pop_answer = float(pop_answer)
        else:
          pop_answer = np.nan
        yield user[0], (PNT_AWD, point_awd)
        yield user[0], (NUM_ASKER, self.question_profile[i][QUES_ASKER])
        yield user[0], (POP_ANSWER, pop_answer)
    asked_questions = user[1:][QL_ASK].strip("[").strip("]").split(",")
    for i in asked_questions:
      i = i.strip("'")
      if i in self.question_profile:
        pop_ask = self.question_profile[i][QUES_POP]
        if pop_ask != "":
          pop_ask = float(pop_ask)
        else:
          pop_ask = np.nan
        yield user[0], (POP_ASK, pop_ask)

  def combiner(self, user, results):
    l = list(results)
    dic = {NUM_ASKER:[]}
    for key, val in l:
      if key in [CATEGORIES, PNT_AWD, POP_ANSWER, POP_ASK]:
        yield user, (key, val)
      elif key in [NUM_ASKER]:
        if val not in dic[key]:
          dic[key].append(val)
          yield user, (key, val)

  def reducer(self, user, results):
    l = list(results)
    dic = {PNT_AWD:[], NUM_ASKER:[], POP_ANSWER:[], POP_ASK:[]}
    cate_dic = {}
    for key, val in l:
      if key in [CATEGORIES]:
        if val != "":
          cate_dic[val] = cate_dic.get(val, 0) + 1
      elif key in [PNT_AWD, POP_ANSWER, POP_ASK]:
        dic[key].append(val)
      elif key in [NUM_ASKER]:
        if val not in dic[key]:
          dic[key].append(val)
    yield user, (CATEGORIES, len(cate_dic))
    yield user, (NUM_ASKER, len(dic[NUM_ASKER]))
    if len(cate_dic) > 0:
      frequency = [(y,x) for (x,y) in cate_dic.items()]
      frequency.sort(reverse=True)
      yield user, (FREQ_CATE, frequency[0][1])
    for feature in [PNT_AWD, POP_ANSWER, POP_ASK]:
      dic[feature] = filter(lambda x: x==x, dic[feature])
      dic[feature] = list_helper(dic[feature])
      for key in dic[feature]:
        yield user, (feature+key, dic[feature][key])

if __name__ == '__main__':
  MRFeatures.run()