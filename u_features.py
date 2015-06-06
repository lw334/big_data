import csv

NUM_ASK = 1
NUM_ANSWER = 21
FREQ_CATE_ANSWER = 30
FREQ_CATE_ASK = 5

DIRECTORY = "/var/tmp/xiaoruit/"
#"data/"
#/../../var/tmp/xiaoruit/
#/var/tmp/

if __name__ == '__main__':
	with open(DIRECTORY+"user3.csv",'r') as csvinput:
		with open(DIRECTORY+"user4.csv", 'w') as csvoutput:
			writer = csv.writer(csvoutput, delimiter="|")
			for row in csv.reader(csvinput, delimiter="|"):
				l = [0]*2
				if row[NUM_ANSWER]!="" and float(row[NUM_ANSWER]) > 0 and row[NUM_ASK] != "" and float(row[NUM_ASK]) > 0:
					l[0] = 1
				if row[FREQ_CATE_ASK]!="" and row[FREQ_CATE_ANSWER]!="" and row[FREQ_CATE_ANSWER] == row[FREQ_CATE_ASK]:
					l[1] = 1
				writer.writerow(row+l)
