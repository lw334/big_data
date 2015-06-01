from parse_answer import MRParseAnswers
import sys, json, csv

USER_LIST = 0
QUESTION_LIST = 1

DIRECTORY = "/var/tmp/"
#"data/"
#/../../var/tmp/xiaoruit/
#/var/tmp/

if __name__ == '__main__':
    user_profile = {}

    print "start"
    f = open(DIRECTORY+"user.csv", "r")
    for line in f:
        l = line.strip().split("|")
        user_profile[l[0]] = l[1:] + [None]*7
    f.close()

    # f = open(DIRECTORY+"question.csv", "r")
    # for line in f:
    #     l = line.strip().split("|")
    #     question_profile[l[0]] = l[1:] + [None]*2
    # f.close()

    print "finish loading user data"
    job = MRParseAnswers(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        print "yielding"
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            if key == USER_LIST:
                if obs not in user_profile:
                    user_profile[obs] = [None]*20 + val
                else:
                    user_profile[obs][20:] = val
            # else:
            #     if obs not in question_profile:
            #         question_profile[obs] = [None]*3 + val
            #     else:
            #         question_profile[obs][3:] = val

    print "start writing user"
    with open(DIRECTORY+'user.csv', 'wb') as myfile:
        wr = csv.writer(myfile, delimiter="|")
        for i in user_profile:
            wr.writerow([i]+user_profile[i])

    # print "start writing question"
    # with open(DIRECTORY+'question.csv', 'wb') as myfile:
    #     wr = csv.writer(myfile, delimiter="|")
    #     for i in question_profile:
    #         wr.writerow([i]+question_profile[i])