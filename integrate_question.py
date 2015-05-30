from mr_parse_question import MRParseQuestions
import sys, json, csv

# CATE_POP = 20
QUES_DICT = 21

# category list
# CATEGORY_FEATURES = [CATE_POP]
QUES_FEATURES = [QUES_DICT]

if __name__ == '__main__':
    user_profile = {}
    question_profile = {}
    # category_profile = []

    job = MRParseQuestions(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            # if key in CATEGORY_FEATURES:
            #     category_profile.append([obs, val])
            if key in QUES_FEATURES:
                question_profile[obs] = val
            else:
                user_profile[obs] = user_profile.get(obs, [None]*20)
                user_profile[obs][key] = val

    with open('data/user.csv', 'wb') as myfile:
    # with open('/../../var/tmp/xiaoruit/user.csv', 'wb') as myfile:
    # with open('/var/tmp/xiaoruit/user.csv', 'wb') as myfile:
        wr = csv.writer(myfile, delimiter="|")
        for i in user_profile:
            wr.writerow([i]+user_profile[i])

    with open('data/question.csv', 'wb') as myfile:
    # with open('/../../var/tmp/xiaoruit/question.csv', 'wb') as myfile:
    # with open('/var/tmp/xiaoruit/question.csv', 'wb') as myfile:
        wr = csv.writer(myfile, delimiter="|")
        for i in question_profile:
            wr.writerow([i]+question_profile[i])

    # with open('data/category.csv', 'wb') as myfile:
    # # with open('/../../var/tmp/xiaoruit/category.csv', 'wb') as myfile:
    # # with open('/var/tmp/xiaoruit/category.csv', 'wb') as myfile:
    #     wr = csv.writer(myfile, delimiter="|")
    #     for i in category_profile:
    #         wr.writerow(i)
