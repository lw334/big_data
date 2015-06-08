from mr_features import MRFeatures
import sys, csv

NUM_FEATURES = 28

DIRECTORY = "/mnt/data/profiles/"
#"data/"
#/../../var/tmp/xiaoruit/
#/var/tmp/

if __name__ == '__main__':
    csv.field_size_limit(sys.maxsize)
    user_profile = {}

    job = MRFeatures(args=sys.argv[1:])
    with job.make_runner() as runner:
        print "start running"
        runner.run()
        
        print "yielding"
        for line in runner.stream_output():
            obs, (key, val) = job.parse_output_line(line)
            if obs not in user_profile:
            	user_profile[obs] = [None]*18
            user_profile[obs][(key-NUM_FEATURES)] = val

    print "start writing user"
    with open(DIRECTORY+"user2.csv",'r') as csvinput:
		with open(DIRECTORY+"user3.csv", 'w') as csvoutput:
			writer = csv.writer(csvoutput, delimiter="|")
			for row in csv.reader(csvinput, delimiter="|"):
				if row[0] in user_profile:
                                        print row[0]
					writer.writerow(row+user_profile[row[0]])
				else:
					writer.writerow(row+[None]*18)
