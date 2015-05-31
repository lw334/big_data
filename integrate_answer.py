from mr_parse_answer import MRParseAnswers
import sys, json, csv

QUES_NUM_ANSWER = 0
QUES_FIRST_ANS = 1

# question list
QUESTION_FEATURES = [QUES_NUM_ANSWER, QUES_FIRST_ANS]

DIRECTORY = "/var/tmp/"
#"data/"
#/../../var/tmp/xiaoruit/
#/var/tmp/

if __name__ == '__main__':
    user_profile = {}
    question_profile = {}

    f = open(DIRECTORY+"user.csv", "r")
    for line in f:
        l = line.strip().split("|")
        user_profile[l[0]] = l[1:] + [None]*7
    f.close()

    print "finish loading user data"
    job = MRParseAnswers(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        print "yielding"
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            if key not in QUESTION_FEATURES:
                if obs not in user_profile:
                    user_profile[obs] = [None]*27
                user_profile[obs][key] = val
            else:
                if obs not in question_profile:
                    question_profile[obs] = [None]*2
                question_profile[obs][key] = val

    print "start writing user"
    with open(DIRECTORY+'user.csv', 'wb') as myfile:
        wr = csv.writer(myfile, delimiter="|")
        for i in user_profile:
            wr.writerow([i]+user_profile[i])

    print "start writing question"
    with open(DIRECTORY+'question.csv', 'wb') as myfile:
        wr = csv.writer(myfile, delimiter="|")
        for i in question_profile:
            wr.writerow([i]+question_profile[i])

    # with open(DIRECTORY+"question.csv",'r') as csvinput:
    #     with open(DIRECTORY+"question2.csv", 'w') as csvoutput:
    #         writer = csv.writer(csvoutput, delimiter="|")
    #         for row in csv.reader(csvinput, delimiter="|"):
    #             if row[0] in question_profile:
    #                 writer.writerow(row+question_profile[row[0]])
    #             else:
    #                 writer.writerow(row+[None]*2)
