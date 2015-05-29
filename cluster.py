import numpy as np
import pandas as pd
import json

from sklearn.cluster import DBSCAN
from sklearn.cluster import Birch 
from sklearn.tree import DecisionTreeClassifier as DT
from sklearn.tree import export_graphviz
from sklearn import metrics
from sklearn.datasets.samples_generator import make_blobs
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression

def read_data():
  user_data = json.load((open("user.txt")))
  userprofiles = pd.DataFrame.from_dict(user_data, orient="index")
  # question_data = json.load((open("question.txt")))
  # question_profiles = pd.DataFrame.from_dict(question_data, orient="index")
  return userprofiles #, question_profiles

def fill_missing(userprofiles):
  userprofiles = userprofiles.fillna(-999)
  return userprofiles



def preprocessing(userprofiles):
  userprofiles = userprofiles.drop('cate_ask',1)
  userprofiles = userprofiles.drop('cate_answer',1)
  userprofiles = userprofiles.drop('corr_award_point',1)
  userprofiles = userprofiles.drop('corr_award_point_answer',1)
  X = userprofiles.as_matrix().astype(np.float)
  return X

def cluster(X):
  db = DBSCAN(eps=0.5, min_samples=5, leaf_size = 3000).fit(X)
  core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
  core_samples_mask[db.core_sample_indices_] = True
  labels = db.labels_

  # Number of clusters in labels, ignoring noise if present.
  n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)

  print('Estimated number of clusters: %d' % n_clusters_)
  # print("Silhouette Coefficient: %0.3f"
  #       % metrics.silhouette_score(X, labels))
  return labels

def birchcluster(X):
  brc = Birch()
  brc.fit(X)
  # Plot result
  labels = brc.labels_
  centroids = brc.subcluster_centers_
  n_clusters = np.unique(labels).size
  print("n_clusters : %d" % n_clusters)
  return labels

def tree(labels,X,userprofiles):
  tree = DT()
  tree.fit(X,labels)
  impt = tree.feature_importances_
  para = tree.get_params()
  export_graphviz(tree, out_file = "tree.dot", feature_names = userprofiles.columns)
  return impt, para

def logit(X, labels, userprofiles):
  lt = LogisticRegression()
  lt.fit(X,labels)
  return lt.coef_



