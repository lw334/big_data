import sys, json, csv
import numpy as np

USER_LIST = 0
QUESTION_LIST = 1

DIRECTORY = "/var/tmp/xiaoruit/"
#"data/"
#/../../var/tmp/xiaoruit/
#/var/tmp/

if __name__ == '__main__':
    csv.field_size_limit(sys.maxsize)

    user_profile = {}
    question_profile = {}


    for i in range(126):
        if i < 10:
            f = open("/var/tmp/xiaoruit/temp/parse_answer_new.ec2-user.20150604.195854.915961/output/part-0000"+str(i))
        elif i < 100:
            f = open("/var/tmp/xiaoruit/temp/parse_answer_new.ec2-user.20150604.195854.915961/output/part-000"+str(i))
        else:
            f = open("/var/tmp/xiaoruit/temp/parse_answer_new.ec2-user.20150604.195854.915961/output/part-00"+str(i))
	print "reading a file" + str(i)
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
    #        else:
     #       if key == QUESTION_LIST:
      #          val = val.split(",")
       #         val = [int(val[0]), val[1].strip().strip('"')]
        #        question_profile[obs] = val

    print "start writing user"
    with open(DIRECTORY+"user.csv",'r') as csvinput:
        with open(DIRECTORY+"user1.csv", 'w') as csvoutput:
            writer = csv.writer(csvoutput, delimiter="|")
            for row in csv.reader(csvinput, delimiter="|"):
                if row[0] in user_profile:
                    if len(user_profile[row[0]]) == 1:
                        print "wrong"
                    writer.writerow(row+user_profile[row[0]])
                    del user_profile[row[0]]
                else:
                    writer.writerow(row+[None]*7)
            for user in user_profile:
                print user+" not match"
                writer.writerow([user]+[None]*20+user_profile[user])

  #  print "start writing question"
   # with open(DIRECTORY+"question.csv",'r') as csvinput:
    #    with open(DIRECTORY+"question1.csv", 'w') as csvoutput:
     #       writer = csv.writer(csvoutput, delimiter="|")
      #      for row in csv.reader(csvinput, delimiter="\t"):
       #         question = row[1].split("|")
	#	if question[2] != "":
        #            temp = [question[0], question[1], float(question[2])]
         ##       else:
           #         temp = [question[0], question[1], np.nan]
            #    if row[0] in question_profile:
             #       question = row[1].split("|")
              #      writer.writerow([row[0]]+temp+question_profile[row[0]])
               # else:
                #    writer.writerow([row[0]]+temp+[None]*2)
