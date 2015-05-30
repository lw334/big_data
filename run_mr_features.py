from mr_features import MRFeatures
import sys, csv

NUM_FEATURES = 28

if __name__ == '__main__':
    user_profile = {}

    job = MRFeatures(args=sys.argv[1:])
    with job.make_runner() as runner:
        runner.run()
        
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            user_profile[obs] = user_profile.get(obs, [None]*18)
            user_profile[obs][(key-NUM_FEATURES)] = val

    with open("data/user1.csv",'r') as csvinput:
		with open("data/user2.csv", 'w') as csvoutput:
			writer = csv.writer(csvoutput, delimiter="|")
			for row in csv.reader(csvinput, delimiter="|"):
				if row[0] in user_profile:
					writer.writerow(row+user_profile[row[0]])
				else:
					writer.writerow(row+[None]*18)