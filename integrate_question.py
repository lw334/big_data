from mr_parse_question import MRParseQuestions
import sys, json, csv

CATE_POP = 20

# category list
CATEGORY_FEATURES = [CATE_POP]

if __name__ == '__main__':
    user_profile = {}
    category_profile = []

    job = MRParseQuestions(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            if key in CATEGORY_FEATURES:
                category_profile.append([obs, val])
            else:
                user_profile[obs] = user_profile.get(obs, [None]*20)
                user_profile[obs][key] = val

    with open('/../../var/tmp/xiaoruit/user.csv', 'wb') as myfile:
        wr = csv.writer(myfile, delimiter="|")
        for i in user_profile:
            wr.writerow([i]+user_profile[i])
    with open('/../../var/tmp/xiaoruit/category.csv', 'wb') as myfile:
        wr = csv.writer(myfile, delimiter="|")
        for i in category_profile:
            wr.writerow(i)
