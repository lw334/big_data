from mr_parse_question import MRParseQuestions
import sys, json, csv

# CATE_POP = 20
QUES_DICT = 21

# category list
# CATEGORY_FEATURES = [CATE_POP]
QUES_FEATURES = [QUES_DICT]

DIRECTORY = "/var/tmp/"
#"data/"
#/../../var/tmp/xiaoruit/
#/var/tmp/

if __name__ == '__main__':
    user_profile = {}
    question_profile = {}
    # category_profile = []

    job = MRParseQuestions(args=sys.argv[1:])
    with job.make_runner() as runner:
        print "start running"
        runner.run()
        
        print "yielding"
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            # if key in CATEGORY_FEATURES:
            #     category_profile.append([obs, val])
            if key in QUES_FEATURES:
                question_profile[obs] = val
            else:
                if obs not in user_profile:
                    user_profile[obs] = [None]*20
                user_profile[obs][key] = val

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

    # with open(DIRECTORY+'category.csv', 'wb') as myfile:
    #     wr = csv.writer(myfile, delimiter="|")
    #     for i in category_profile:
    #         wr.writerow(i)
