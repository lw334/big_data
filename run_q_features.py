from q_features import QFeatures
import sys, csv

DIRECTORY = "/var/tmp/"
#"data/"
#/../../var/tmp/xiaoruit/
#/var/tmp/

if __name__ == '__main__':
    user_profile = {}

    # f = open("data/user.csv", "r")
    # for line in f:
    #     l = line.strip().split("|")
    #     user_profile[l[0]] = l[1:] + [0]
    # f.close()

    job = QFeatures(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        for line in runner.stream_output():
            obs, val = job.parse_output_line(line)
            user_profile[obs] = val

    with open(DIRECTORY+"user.csv",'r') as csvinput:
        with open(DIRECTORY+"user1.csv", 'w') as csvoutput:
            writer = csv.writer(csvoutput, delimiter="|")
            for row in csv.reader(csvinput, delimiter="|"):
                if row[0] in user_profile:
                    writer.writerow(row+[user_profile[row[0]]])
                else:
                    writer.writerow(row+[0])
					
    #         if obs in user_profile:
    #         	user_profile[obs][-1] = val

    # with open('data/user.csv', 'wb') as myfile:
	   #  wr = csv.writer(myfile, delimiter="|")
	   #  for i in user_profile:
	   #      wr.writerow([i]+user_profile[i])
