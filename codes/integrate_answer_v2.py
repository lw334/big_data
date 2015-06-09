from parse_answer_new import MRParseAnswers
import sys, json, csv

USER_LIST = 0
QUESTION_LIST = 1

DIRECTORY = "/mnt/data/profiles/"

if __name__ == '__main__':
    user_profile = {}
    question_profile = {}

    job = MRParseAnswers(args=sys.argv[1:])
    with job.make_runner() as runner:
        print "start running"
        runner.run()
        
        print "yielding"
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            if key == USER_LIST:
                user_profile[obs] = val
            else:
                question_profile[obs] = val

    print "start writing user"
    with open(DIRECTORY+"user.csv",'r') as csvinput:
        with open(DIRECTORY+"user1.csv", 'w') as csvoutput:
            writer = csv.writer(csvoutput, delimiter="|")
            for row in csv.reader(csvinput, delimiter="|"):
                if row[0] in user_profile:
                    writer.writerow(row+[user_profile[row[0]]])
                else:
                    writer.writerow(row+[None]*7)

    print "start writing question"
    with open(DIRECTORY+"question.csv",'r') as csvinput:
        with open(DIRECTORY+"question1.csv", 'w') as csvoutput:
            writer = csv.writer(csvoutput, delimiter="|")
            for row in csv.reader(csvinput, delimiter="\t"):
                if row[0] in question_profile:
                    writer.writerow(row+[user_profile[row[0]]])
                else:
                    writer.writerow(row+[None]*2)
