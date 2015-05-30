from mrjob.job import MRJob

class MRQFeatures(MRJob):
  
  def mapper(self, _, line):
    question = line.strip().split("|")
    yield question[QUES_FIRST_ANS], 1

  def combiner(self, obs, results):
    yield obs, sum(results)

  def reducer(self, obs, results):
  	yield obs, sum(results)

if __name__ == '__main__':
  MRQFeatures.run()