from mr_cnt_answers import MRCntAnswers
import sys
import json

if __name__ == '__main__':
    user_profile = {}
    job = MRCntAnswers(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        for line in runner.stream_output():
            user, cnt = job.parse_output_line(line)
            user_profile[user] = user_profile.get(user, {})
            user_profile[user]["num_answers"] = cnt

    json.dump(user_profile, open("user_profile.txt","w"))