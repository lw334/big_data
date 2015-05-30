import json
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN


if __name__ == '__main__':
	# import data
	user_dic = json.load(open("user.txt"))
	user_profile = pd.DataFrame.from_dict(user_dic, orient="index", na_values=[None])
	# question_dic = json.load(open("question.txt"))
	# question_profile = pd.DataFrame.from_dict(question_dic, orient="index")

	# summary statistics
	user_profile.describe().to_csv("summary.csv")
	# print question_profile.describe()

	x_cols = user_profile.columns[30:40]
	user_profile[x_cols] = user_profile[x_cols].fillna(-999999)
	X=user_profile[x_cols].as_matrix().astype(np.float)
	db = DBSCAN(leaf_size = 300).fit(X)
	core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
	core_samples_mask[db.core_sample_indices_] = True
	labels = db.labels_
	n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)