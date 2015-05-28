from mr_parse_answers import MRParseAnswers
import sys
import json

if __name__ == '__main__':
    user_profile = {}
    question_profile = {}

    job = MRParseAnswers(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            if key in ["u_num_answer", "u_avg_pnt_answer", "u_qid_answer"]:
                user_profile[obs] = user_profile.get(obs, {})
                user_profile[obs][key] = val
            else:
                question_profile[obs] = question_profile.get(obs, {})
                question_profile[obs][key] = val

    json.dump(user_profile, open("user.txt","w"))
    json.dump(question_profile, open("question.txt","w"))