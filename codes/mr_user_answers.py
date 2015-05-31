from mrjob.job import MRJob
import re

# data imported
Q_ID = 0
ANSWERERID = 1
POINTSSNAP = 2
ANSWERDATE = 3

class MRUserAnswers(MRJob):
  
  def mapper(self, _, line):
    answer = line.strip().replace("|", "\t").split("\t")
    yield answer[ANSWERERID], ("num_answer", 1)
    yield answer[ANSWERERID], ("avg_pnt_answer", float(answer[POINTSSNAP]))
    yield answer[ANSWERERID], ("qid_answers", answer[Q_ID])

  def combiner(self, answerer, results):
    l = list(results)
    dic = {}
    for key, val in l:
      # for sums
      if key in ["num_answer"]:
        dic[key] = dic.get(key, 0) + val
      # don't use combiner for averages
      else:
        yield answerer, (key, val)
    yield answerer, ("num_answer", dic["num_answer"])

  def reducer(self, answerer, results):
    l = list(results)
    dic = {}
    cnt = 0
    for key, val in l:
      if key not in ["qid_answers"]:
        dic[key] = dic.get(key, 0) + val
        cnt += 1
      else:
        dic[key] = dic.get(key, []) + [val]
    yield answerer, ("num_answer", dic["num_answer"])
    yield answerer, ("avg_pnt_answer", dic["avg_pnt_answer"]/cnt)
    yield answerer, ("qid_answers", dic["qid_answers"])

if __name__ == '__main__':
  MRUserAnswers.run()