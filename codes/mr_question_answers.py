from mrjob.job import MRJob
import re
import sys

# data imported
Q_ID = 0
ANSWERERID = 1
POINTSSNAP = 2
ANSWERDATE = 3

class MRQuestionAnswers(MRJob):
  
  def mapper(self, _, line):
    answer = line.strip().replace("|", "\t").split("\t")
    yield answer[Q_ID], ("num_answer", 1)
    yield answer[Q_ID], ("first_answerer", (answer[ANSWERERID], float(answer[ANSWERDATE])))

  def combiner(self, question, results):
    l = list(results)
    dic = {}
    for key, val in l:
      # for sum
      if key in ["num_answer"]:
        dic[key] = dic.get(key, 0) + val
      # for min
      else:
        dic[key] = dic.get(key, (" ", sys.maxint))
        if val[1] < dic[key][1]:
          dic[key] = val
    yield question, ("num_answer", dic["num_answer"])
    yield question, ("first_answerer", dic["first_answerer"])

  def reducer(self, question, results):
    l = list(results)
    dic = {}
    for key, val in l:
      # for sum
      if key in ["num_answer"]:
        dic[key] = dic.get(key, 0) + val
      # for min
      else:
        dic[key] = dic.get(key, (" ", sys.maxint))
        if val[1] < dic[key][1]:
          dic[key] = val
    yield question, ("num_answer", dic["num_answer"])
    yield question, ("first_answerer", dic["first_answerer"][0])

if __name__ == '__main__':
  MRQuestionAnswers.run()