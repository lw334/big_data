# big_data
Analyzing Yahoo! Answers data using techniques in machine learning and parallel programming.

The Python files under the home directory are the major codes used for analyzing the datasets. Please run the codes in the order specified as follows:
    
    - integrate_a_new.py: This file calls the MRJob class in parse_answer_new.py, and uses the answer datasets to generate
    features about answerers. It will create a user profile and a question profile (stored in csv format).
    
    - integrate_q_new.py: It calls the MRJob class in parse_question_new.py, and uses the question datasets to generate features
    about askers. It takes the user.csv and question.csv created in the first step as input, and updates the user and question
    profiles.
    
    - run_q_features.py: This file calls the MRJob class in q_features.py. It uses the question profile as input and generates a
    user feature (num_first_answer). It updates the user profile.
    
    - run_mr_features.py: This file calls the MRJob class in mr_features.py. It integrates the information about questions and
    answers and generates some features about answerers. It updates the user profile.
    
    - u_features.py: This file takes the user profile as input and generate some additional features using the existing
    features. It updates the user profile.
    
    - cluster.py: After having the final user profile ready, use this file to do data preprocessing and run clustering and
    decision tree algorithms.

The "codes/" directory contains part of the other versions of codes we used to assist with feature generation.

The "test/" directory contains a small amount of sample data. Using the sample data, you can use the pre-scale-up version of code (data_exploration.py) to get a sense of what features we generated, or test the whole pipeline described above.
