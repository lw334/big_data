from mr_parse_answer import MRParseAnswers
import sys, json, csv

QUES_NUM_ANSWER = 0
QUES_FIRST_ANS = 1

# question list
QUESTION_FEATURES = [QUES_NUM_ANSWER, QUES_FIRST_ANS]

if __name__ == '__main__':
    user_profile = {}
    question_profile = {}

    f = open("data/user.csv", "r")
    for line in f:
        l = line.strip().split("|")
        user_profile[l[0]] = l[1:] + [None]*7
    f.close()

    job = MRParseAnswers(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            if key not in QUESTION_FEATURES:
                if obs not in user_profile:
                    user_profile[obs] = user_profile.get(obs, [None]*27)
                user_profile[obs][key] = val
            else:
                question_profile[obs] = question_profile.get(obs, [None]*2)
                question_profile[obs][key] = val

    with open('data/user.csv', 'wb') as myfile:
        wr = csv.writer(myfile, delimiter="|")
        for i in user_profile:
            wr.writerow([i]+user_profile[i])
    with open('data/question.csv', 'wb') as myfile:
        wr = csv.writer(myfile, delimiter="|")
        for i in question_profile:
            wr.writerow([i]+question_profile[i])