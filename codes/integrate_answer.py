from mr_parse_answers import MRParseAnswers
import sys, json, csv

# user list
USER_FEATURES = ["u_num_answer", "u_qid_answer", "u_pnt_answer_avg",
                "u_pnt_answer_mid", "u_pnt_answer_min",
                "u_pnt_answer_max", "u_pnt_answer_std"]

# question list
QUESTION_FEATURES = ["q_num_answer", "q_first_answerer"]

if __name__ == '__main__':
    user_profile = {}
    question_profile = {}

    job = MRParseAnswers(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            if key not in QUESTION_FEATURES:
                user_profile[obs] = user_profile.get(obs, {})
                user_profile[obs][key] = val
            else:
                question_profile[obs] = question_profile.get(obs, {})
                question_profile[obs][key] = val

    json.dump(user_profile, open("data/user.txt","w"))
    json.dump(question_profile, open("data/question.txt","w"))
    # with open('data/user.csv', 'wb') as f:
    #     w = csv.writer(f)
    #     w.writerows(user_profile.items())
    # with open('data/question.csv', 'wb') as f:
    #     w = csv.writer(f)
    #     w.writerows(question_profile.items())
