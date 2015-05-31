from mr_parse_questions import MRParseQuestions
import sys, json, csv

# question list
QUESTION_FEATURES = ["q_feature_dict"]

# category list
CATEGORY_FEATURES = ["c_pop"]

if __name__ == '__main__':
    user_profile = json.load(open("data/user.txt"))
    question_profile = json.load(open("data/question.txt"))
    category_profile = {}

    job = MRParseQuestions(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            if key in QUESTION_FEATURES:
                question_profile[obs] = question_profile.get(obs, {})
                question_profile[obs] = val
            elif key in CATEGORY_FEATURES:
                category_profile[obs] = category_profile.get(obs, {})
                category_profile[obs][key] = val
            else:
                user_profile[obs] = user_profile.get(obs, {})
                user_profile[obs][key] = val

    json.dump(user_profile, open("data/user.txt","w"))
    json.dump(question_profile, open("data/question.txt","w"))
    json.dump(category_profile, open("data/category.txt","w"))
    # with open('data/user.csv', 'wb') as f:
    #     w = csv.writer(f)
    #     w.writerows(user_profile.items())
    # with open('data/question.csv', 'wb') as f:
    #     w = csv.writer(f)
    #     w.writerows(question_profile.items())
    # with open('data/category.csv', 'wb') as f:
    #     w = csv.writer(f)
    #     w.writerows(category_profile.items())
