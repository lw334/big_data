import sys, json, csv

USER_LIST = 0
QUESTION_LIST = 1

DIRECTORY = "../data/profiles/"
#"data/"
#/../../var/tmp/xiaoruit/
#/var/tmp/

if __name__ == '__main__':
    user_profile = {}
    question_profile = {}


    for i in range(126):
        if i < 10:
            f = open("part-0000"+str(i))
        elif i < 100:
            f = open("part-000"+str(i))
        else:
            f = open("part-00"+str(i))
        l = f.readlines()
        l = l[:10]
        for line in l:
            line = [x.strip("'") for x in line.strip().split("\t")]
            obs = line[0].strip('"')
            key = int(line[1].strip("[")[0])
            val = line[1].strip("[").strip("]")[4:]
            if key == USER_LIST:
                tmp = [int(val[0]), [i.strip().strip('"') for i in val[val.find("[")+1:val.find("]")].split(",")]]
                tmp += [float(i) for i in val[val.find("]")+3:].split(",")]
                user_profile[obs] = tmp
            else:
                val = val.split(",")
                val = [int(val[0]), val[1].strip().strip('"')]
                question_profile[obs] = val

    print "start writing user"
    with open(DIRECTORY+"user.csv",'r') as csvinput:
        with open(DIRECTORY+"user1.csv", 'w') as csvoutput:
            writer = csv.writer(csvoutput, delimiter="|")
            for row in csv.reader(csvinput, delimiter="|"):
                if row[0] in user_profile:
                    writer.writerow(row+[user_profile[row[0]]])
                else:
                    writer.writerow(row+[None]*7)

    print "start writing question"
    with open(DIRECTORY+"question.csv",'r') as csvinput:
        with open(DIRECTORY+"question1.csv", 'w') as csvoutput:
            writer = csv.writer(csvoutput, delimiter="|")
            for row in csv.reader(csvinput, delimiter="\t"):
                if row[0] in question_profile:
                    writer.writerow(row+[user_profile[row[0]]])
                else:
                    writer.writerow(row+[None]*2)