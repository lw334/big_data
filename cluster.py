import numpy as np
import pandas as pd
import json, sys

from pandas import Series
from scipy.stats import mode
from matplotlib import pyplot as plt
from sklearn.cluster import DBSCAN, Birch, KMeans, MiniBatchKMeans
from sklearn.tree import DecisionTreeClassifier as DT
from sklearn.tree import export_graphviz
from sklearn import metrics
from sklearn.datasets.samples_generator import make_blobs
from sklearn.linear_model import LogisticRegression

MISSING_TIME = -sys.float_info.max
DIRECTORY = "data/"
OUTPUT_DIRECTORY = "output/"

def read_data(filename, columns):
  # user_data = json.load((open(filename)))
  # userprofiles = pd.DataFrame.from_dict(user_data, orient="index")
  # # question_data = json.load((open("question.txt")))
  # # question_profiles = pd.DataFrame.from_dict(question_data, orient="index")
  # return userprofiles #, question_profiles
  rv = pd.read_csv(filename, delimiter="|", names=columns)
  return rv

def summary_statistics(df):
  summary=df.describe().T
  summary.rename(columns={'50%': 'median'}, inplace=True)
  summary['mode']=Series(np.array([mode(df[var])[0][0] for var in list(df.columns.values)]), index=summary.index)
  summary['cnt_missing']=len(df.index)-summary['count']
  to_drop = ['count']
  summary.drop(to_drop, axis=1, inplace=True)
  summary.to_csv(OUTPUT_DIRECTORY+"summary_stats.csv") 
  return summary.T

def plot_distribution(df, bar_cols):
  for key in df.columns:
    plt.clf()
    if key in bar_cols:
      df.groupby(key).size().plot(kind='bar')
    else:
      df[key].hist(bins=5)
    histogram_helper(key)

def histogram_helper(key):
  plt.title("Probability Distribution for "+key)
  plt.xlabel("Value")
  plt.ylabel("Frequency")
  plt.savefig(OUTPUT_DIRECTORY+key+".png")

def standardize(df):
  df = (df-df.median())/df.std()
  return df.dropna(axis=1, how="all")

def fill_missing(df):
  return df.fillna(-10000*df.median())

def preprocessing(df):
  return df.as_matrix().astype(np.float)

def kmeans(X, k):
  km = KMeans(n_clusters=k, n_jobs=-1, init="random")
  km.fit(X)
  labels = km.labels_
  return labels

def minibatchkmeans(X, k):
  mbk = MiniBatchKMeans(n_clusters=k)
  mbk.fit(X)
  labels = mbk.labels_
  return labels

def dbscan(X):
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

def tree(labels,X,df,i):
  tree = DT()
  tree.fit(X,labels)
  impt = tree.feature_importances_
  para = tree.get_params()
  export_graphviz(tree, out_file = OUTPUT_DIRECTORY+str(i)+"_tree.dot", feature_names = df.columns)
  return impt

def logit(X, labels):
  lt = LogisticRegression()
  lt.fit(X,labels)
  return lt.coef_

if __name__ == '__main__':
  # columns = ["user_id","num_ask", "num_extension", "ql_ask", "num_cate_ask", \
  # "freq_cate_ask", "avg_duration", "mid_duration", "min_duration",\
  # "max_duration", "std_duration", "avg_pnt_ask", "mid_pnt_ask", "min_pnt_ask",\
  # "max_pnt_ask", "std_pnt_ask", "avg_awd_pnt_ask", "mid_awd_pnt_ask", \
  # "min_awd_pnt_ask","max_awd_pnt_ask", "std_awd_pnt_ask",
  # "num_answer", "ql_answer", "avg_pnt_answer", "mid_awd_pnt_answer", \
  # "min_awd_pnt_answer","max_awd_pnt_answer", "std_awd_pnt_answer",
  # "num_first_answer", "num_cate_answer", "freq_cate_answer", "avg_pnt_awd_answer", \
  # "mid_pnt_awd_answer","min_pnt_awd_answer","max_pnt_awd_answer","std_pnt_awd_answer",\
  # "num_asker", "avg_pop_answer","mid_pop_answer","min_pop_answer","max_pop_answer",\
  # "std_pop_answer", "avg_pop_ask","mid_pop_ask","min_pop_ask","max_pop_ask","std_pop_ask",\
  # "asker_and_answerer", "same_freq_cate"]
  columns = ["user_id", "num_ask", "num_extension", "ql_ask", "num_cate_ask", \
  "freq_cate_ask", "avg_duration", "mid_duration", "min_duration",\
  "max_duration", "std_duration", "avg_pnt_ask", "mid_pnt_ask", "min_pnt_ask",\
  "max_pnt_ask", "std_pnt_ask", "avg_awd_pnt_ask", "mid_awd_pnt_ask", \
  "min_awd_pnt_ask","max_awd_pnt_ask", "std_awd_pnt_ask"]

  # BINARY = ["asker_and_answerer", "same_freq_cate"]
  BINARY = []

  # cols_to_drop = ["user_id", "ql_ask", "freq_cate_ask", "ql_answer", "freq_cate_answer"]
  cols_to_drop = ["user_id", "ql_ask", "freq_cate_ask"]

  df = read_data(DIRECTORY+"user.csv", columns)
  for col in cols_to_drop:
    df.drop(col, axis=1, inplace=True)

  summary_statistics(df)
  plot_distribution(df, BINARY)

  df = standardize(df)
  df = fill_missing(df)
  # df.to_csv(DIRECTORY+"user_clean.csv")
  X = preprocessing(df)

  dbscan_labels = dbscan(X)
  birch_labels = birchcluster(X)
  kmean_labels = kmeans(X, 4)
  mbk_labels = minibatchkmeans(X, 4)

  labels = [dbscan_labels, birch_labels, kmean_labels, mbk_labels]
  importance = pd.DataFrame(columns=pd.Series(df.columns))
  for i in range(len(labels)):
    importance.loc[i] = tree(labels[i], X, df, i)
  importance.to_csv(OUTPUT_DIRECTORY+"feature_importance.csv")
  print importance