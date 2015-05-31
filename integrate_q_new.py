from parse_question_new import MRParseQuestions
import sys, json, csv

# CATE_POP = 20
USER_LIST = 20
QUES_DICT = 21

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
        user_profile[l[0]] = [None]*20 + l[1:]
    f.close()

    print "finish loading user data"
    f = open(DIRECTORY+"question.csv", "r")
    for line in f:
        l = line.strip().split("|")
        question_profile[l[0]] = [None]*3 + l[1:]
    f.close()

    print "finish loading question data"
    job = MRParseQuestions(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        print "yielding"
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            if key == USER_LIST:
                if obs not in user_profile:
                    user_profile[obs] = val + [None]*7
                else:
                    user_profile[obs][:20] = val
            else:
                if obs not in question_profile:
                    question_profile[obs] = val + [None]*2
                else:
                    question_profile[obs][:3] = val

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