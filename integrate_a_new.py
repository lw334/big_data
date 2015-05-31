from parse_answer_new import MRParseAnswers
import sys, json, csv

USER_LIST = 0
QUESTION_LIST = 1

DIRECTORY = "/var/tmp/"
#"data/"
#/../../var/tmp/xiaoruit/
#/var/tmp/

if __name__ == '__main__':

    with open(DIRECTORY+'user.csv', 'wb') as userfile:
        wru = csv.writer(userfile, delimiter="|")
        with open(DIRECTORY+'question.csv', 'wb') as questionfile:
            wrq = csv.writer(questionfile, delimiter="|")

            job = MRParseAnswers(args=sys.argv[1:])
            with job.make_runner() as runner:
                print "start running"
                runner.run()
            
                print "yielding"
                for line in runner.stream_output():
                    obs, (key, val) = job.parse_output_line(line)
                    if key == QUESTION_LIST:
                        wrq.writerow([obs]+val)
                    else:
                        wru.writerow([obs]+val)