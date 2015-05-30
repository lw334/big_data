import numpy as np
import json
from scipy.stats.stats import pearsonr
from collections import Counter

Q_ID = 0
ASKER_ID = 1
CATE_ID = 2
POINTS_AWARDED = 3
CREAT_DATE = 4
QUESTION_STATE = 5
EXPIRE_DATE = 6
POINTS_ASKER = 7
EXTENSION_DATE = 8
AWARD_DATE = 9
ANSWERERID = 1
POINTSSNAP = 2
ANSWERDATE = 3


#<------------------------------------------------------>

def read_question_data(filename, data):
	f = open(filename,"r")
	l = [x.strip().replace("|","\t").split("\t") for x in f.readlines()]
	for i in l:
		data[i[ASKER_ID]] = data.get(i[ASKER_ID], {"questions":{},"answers":{}})
		data[i[ASKER_ID]]["questions"][i[Q_ID]] = {
										"cate_id": i[CATE_ID],
										"points_awarded": float(i[POINTS_AWARDED]),
										"create_date": i[CREAT_DATE],
										"state": i[QUESTION_STATE],
										"expire_date": i[EXPIRE_DATE],
										"points_asker": float(i[POINTS_ASKER]),
										"extension_date": i[EXTENSION_DATE],
										"award_date": i[AWARD_DATE]}


def read_answer_data(filename, data):
    f = open(filename,"r")
    for line in f:
		l = line.strip().replace("|", "\t").split("\t")
		data[l[ANSWERERID]] = data.get(l[ANSWERERID], {"questions":{},"answers":{}})
		data[l[ANSWERERID]]["answers"][l[ANSWERDATE]] = {
										        "answerdate":l[ANSWERDATE],
										        "questionID":l[Q_ID],
										        }
		if l[POINTSSNAP] == "":
			data[l[ANSWERERID]]["answers"][l[ANSWERDATE]]["points_answerer"] = None
		else:
			data[l[ANSWERERID]]["answers"][l[ANSWERDATE]]["points_answerer"]=float(l[POINTSSNAP])


def read_question(filename, question_profile):
	f = open(filename,"r")
	l = [x.strip().replace("|","\t").split("\t") for x in f.readlines()]
	for i in l:
		question_profile[i[Q_ID]] = question_profile.get(i[Q_ID],{})
		question_profile[i[Q_ID]]["points_awarded"] = float(i[POINTS_AWARDED])
		question_profile[i[Q_ID]]["points_asker"] = float(i[POINTS_ASKER])
		question_profile[i[Q_ID]]["category"] = i[CATE_ID]
		

#<---------------------------feature generation--------------------------------------->

def question_popularity(filename, question_profile):
	f = open(filename,"r")
	l = [x.strip().replace("|","\t").split("\t") for x in f.readlines()]
	for i in l:
		question_profile[i[Q_ID]] = question_profile.get(i[Q_ID],{})
		question_profile[i[Q_ID]]["num_answers"] = question_profile[i[Q_ID]].get("num_answers", 0)+1
		question_profile[i[Q_ID]]["answer_dates"] = question_profile[i[Q_ID]].get("answer_dates", [])+[i[ANSWERDATE]]


def num_questions_asked(data, user_profile):
	for user in data:
		user_profile[user] = user_profile.get(user, {})
		cnt = 0
		for qid in data[user]["questions"]:
			cnt += 1
		user_profile[user]["num_questions_asked"] = cnt


def list_helper(l, user_profile, user, feature):
	if len(l) == 0:
		user_profile[user]["avg_"+feature] = None
		user_profile[user]["min_"+feature] = None
		user_profile[user]["mid_"+feature] = None
		user_profile[user]["max_"+feature] = None
		user_profile[user]["std_"+feature] = None
	else:
		user_profile[user]["avg_"+feature] = np.mean(l)
		user_profile[user]["mid_"+feature] = np.median(l)
		user_profile[user]["min_"+feature] = np.min(l)
		user_profile[user]["max_"+feature] = np.max(l)
		user_profile[user]["std_"+feature] = np.std(l)


def points_asking(data, user_profile, feature):
	for user in data:
		user_profile[user] = user_profile.get(user, {})
		l = []
		for qid in data[user]["questions"]:
			l.append(data[user]["questions"][qid][feature])
		list_helper(l, user_profile, user, "asking_"+feature)


def extensions(data, user_profile):
	for user in data:
		user_profile[user] = user_profile.get(user, {})
		cnt = 0
		for qid in data[user]["questions"]:
			if data[user]["questions"][qid]["extension_date"] != "":
				cnt += 1
		user_profile[user]["num_extensions"] = cnt


def creation_award_period(data, user_profile):
	for user in data:
		user_profile[user] = user_profile.get(user, {})
		l = []
		for qid in data[user]["questions"]:
			create_date = data[user]["questions"][qid]["create_date"]
			award_date = data[user]["questions"][qid]["award_date"]
			if create_date != "" and award_date != "":
				l.append(float(award_date) - float(create_date))
		list_helper(l, user_profile, user, "creation_award_period")


def categories_ask(data, user_profile):
	for user in data:
		user_profile[user] = user_profile.get(user, {})
		dic = {}
		for qid in data[user]["questions"]:
			cate = data[user]["questions"][qid]["cate_id"]
			dic[cate] = dic.get(cate, 0) + 1
		user_profile[user]["cate_ask"] = dic
		user_profile[user]["num_cate_ask"] = len(dic)
		temp = [(y, x) for x,y in dic.items()]
		temp.sort(reverse=True)
		if len(dic) == 0:
			user_profile[user]["mostfreq_cate_ask"] = None
		else:
			user_profile[user]["mostfreq_cate_ask"] = temp[0][1]


def popularity_ask(data, user_profile, question_profile):
	for user in data:
		user_profile[user] = user_profile.get(user, {})
		l = []
		for qid in data[user]["questions"]:
			l.append(question_profile[qid]["num_answers"])
		list_helper(l, user_profile, user, "popularity_ask")


def corr_award_point(data, user_profile, question_profile):
	for user in data:
		user_profile[user] = user_profile.get(user, {})
		l1 = []
		l2 = []
		for qid in data[user]["questions"]:
			l1.append(question_profile[qid]["points_awarded"])
			l2.append(question_profile[qid]["points_asker"])
		if len(l1) > 1:
			user_profile[user]["corr_award_point"] = pearsonr(l1, l2)
		else:
			user_profile[user]["corr_award_point"] = None

# <------------- features related to how a user answer questions ------------------->

def create_answer_points(data, user_profile):
	for user in data:
		user_profile[user]["numanswers"] = len(data[user]["answers"])

		#features related to points of the questions to which the person answered
		answerpointlist = []
		for answer in data[user]["answers"]:
			answerpointlist.append(data[user]["answers"][answer]["points_answerer"])
			list_helper(answerpointlist, user_profile, user, "answerpoints")

		#features related to points awarded to this user
		awardpointlist = []
		for question in data[user]["questions"]:
			awardpointlist.append(data[user]["questions"][question]["points_awarded"])
			list_helper(awardpointlist, user_profile, user, "awardpoints")

#features related to how the user answers questions
# how many times was the user the first one to answer a question
def firstAnswers(data, user_profile, question_profile):
    for user in data:
    	for answer in data[user]["answers"]:
    		q_id = data[user]["answers"][answer]["questionID"]
    		useranswerdate = data[user]["answers"][answer]["answerdate"]
    		if q_id in question_profile:
	    		dates = question_profile[q_id]["answer_dates"]
	    		first = min(dates)
	    		if useranswerdate == first:
	    			user_profile["num1stanswer"].get("num1stanswer", 0) + 1

def categories_answer(data, user_profile, question_profile):
	for user in data:
		for answer in data[user]["answers"]:
			dic = {}
			q_id = data[user]["answers"][answer]["questionID"]
			if q_id in question_profile:
				category = question_profile[q_id]["category"]
				dic[category] = dic.get(category, 0) + 1
		user_profile[user]["cate_answer"] = dic
		user_profile[user]["num_cate_answer"] = len(dic)
		temp = [(y, x) for x,y in dic.items()]
		temp.sort(reverse=True)
		if len(dic) == 0:
			user_profile[user]["mostfreq_cate_answer"] = None
		else:
			user_profile[user]["mostfreq_cate_answer"] = temp[0][1]

def popularity_answer(data, user_profile, question_profile):
	for user in data:
		user_profile[user] = user_profile.get(user, {})
		l = []
		for answer in data[user]["answers"]:
			qid = data[user]["answers"][answer]["questionID"]
			if qid in question_profile:
				l.append(question_profile[qid]["num_answers"])
		list_helper(l, user_profile, user, "popularity_answer")

def corr_award_point_answer(data, user_profile, question_profile):
	for user in data:
		user_profile[user] = user_profile.get(user, {})
		l1 = []
		l2 = []
		for answer in data[user]["answers"]:
			qid = data[user]["answers"][answer]["questionID"]
			if qid in question_profile:
				l1.append(question_profile[qid]["points_awarded"])
				l2.append(data[user]["answers"][answer]["points_answerer"])
		if len(l1) > 1:
			user_profile[user]["corr_award_point_answer"] = pearsonr(l1, l2)
		else:
			user_profile[user]["corr_award_point_answer"] = None


if __name__ == '__main__':
	# data = {}
	# read_question_data("part-00000_question", data)
	# read_answer_data("part-00000_answer", data)

# 	question_profile = {}
# 	read_question("part-00000_question", question_profile)
	# question_popularity("part-00000_answer", question_profile)
	
# 	num_questions_asked(data, user_profile)
	# points_asking(data, user_profile, "points_asker")
	# points_asking(data, user_profile, "points_awarded")
	# extensions(data, user_profile)
	# creation_award_period(data, user_profile)
	# categories_ask(data, user_profile)
	# # popularity_ask(data, user_profile, question_profile)
	# # corr_award_point(data, user_profile, question_profile)
	# create_answer_points(data, user_profile)
	# # firstAnswers(data, user_profile, question_profile)
	# # categories_answer(data, user_profile, question_profile)
	# # popularity_answer(data, user_profile, question_profile)
	# # corr_award_point_answer(data, user_profile, question_profile)

# 	json.dump(data, open("data.txt","w"))
# 	data = json.load(open("data.txt"))
# 	json.dump(user_profile, open("user.txt","w"))
# 	user_profile = json.load(open("user.txt"))
# 	json.dump(question_profile, open("question.txt","w"))
# 	question_profile = json.load(open("question.txt"))
	pass