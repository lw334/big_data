from mrjob.job import MRJob
import re
import sys

# data imported
Q_ID = 0
ANSWERERID = 1
POINTSSNAP = 2
ANSWERDATE = 3

class MRParseAnswers(MRJob):
  
  def mapper(self, _, line):
    answer = line.strip().replace("|", "\t").split("\t")
    if len(answer) > 1:
      yield answer[ANSWERERID], ("u_num_answer", 1)
      yield answer[ANSWERERID], ("u_avg_pnt_answer", float(answer[POINTSSNAP]))
      yield answer[ANSWERERID], ("u_qid_answer", answer[Q_ID])
      yield answer[Q_ID], ("q_num_answer", 1)
      yield answer[Q_ID], ("q_first_answerer", (answer[ANSWERERID], float(answer[ANSWERDATE])))

  def combiner(self, obs, results):
    l = list(results)
    dic = {}
    is_user = -1
    for key, val in l:
      # for sums
      if key in ["u_num_answer", "q_num_answer"]:
        dic[key] = dic.get(key, 0) + val
      # for min
      elif key in ["q_first_answerer"]:
        dic[key] = dic.get(key, (" ", sys.maxint))
        if val[1] < dic[key][1]:
          dic[key] = val
        is_user = False
      # don't use combiner for averages
      elif key in ["u_avg_pnt_answer", "u_qid_answer"]:
        yield obs, (key, val)
        is_user = True
    assert(is_user != -1)
    if is_user:
      yield obs, ("u_num_answer", dic["u_num_answer"])
    else:
      yield obs, ("q_num_answer", dic["q_num_answer"])
      yield obs, ("q_first_answerer", dic["q_first_answerer"])

  def reducer(self, obs, results):
    l = list(results)
    dic = {}
    cnt = 0
    is_user = -1
    for key, val in l:
      if key in ["u_num_answer", "u_avg_pnt_answer", "q_num_answer"]:
        dic[key] = dic.get(key, 0) + val
        cnt += 1
      elif key in ["q_first_answerer"]:
        dic[key] = dic.get(key, (" ", sys.maxint))
        if val[1] < dic[key][1]:
          dic[key] = val
        is_user = False
      elif key in ["u_qid_answer"]:
        dic[key] = dic.get(key, []) + [val]
        is_user = True
    assert(is_user != -1)
    if is_user:
      yield obs, ("u_num_answer", dic["u_num_answer"])
      yield obs, ("u_avg_pnt_answer", dic["u_avg_pnt_answer"]/cnt)
      yield obs, ("u_qid_answer", dic["u_qid_answer"])
    else:
      yield obs, ("q_num_answer", dic["q_num_answer"])
      yield obs, ("q_first_answerer", dic["q_first_answerer"][0])

if __name__ == '__main__':
  MRParseAnswers.run()