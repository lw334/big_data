from mrjob.job import MRJob

Q_ID = 0
QUES_FIRST_ANS = 5

class QFeatures(MRJob):
  
  def mapper(self, _, line):
    question = line.strip().split("|")
    yield question[QUES_FIRST_ANS], 1

  def combiner(self, obs, results):
    yield obs, sum(results)

  def reducer(self, obs, results):
  	yield obs, sum(results)

if __name__ == '__main__':
  QFeatures.run()