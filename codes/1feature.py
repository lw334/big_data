from mrjob.job import MRJob
import re

Q_ID = 0
ANSWERERID = 1
POINTSSNAP = 2
ANSWERDATE = 3

class MRCntAnswers(MRJob):
  
  def mapper(self, _, line):
    answer = line.strip().replace("|", "\t").split("\t")
    yield answer[ANSWERERID], 1

  def combiner(self, answerer, counts):
    yield answerer, sum(counts)

  def reducer(self, answerer, counts):
    yield answerer, sum(counts)

if __name__ == '__main__':
  MRCntAnswers.run()