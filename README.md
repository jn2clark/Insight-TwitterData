## #Holla-A-Hashtag - Jesse Clark
Scripts and functions for collecting, cleaning and processing tweets to obtain a model for # recommendations.
Steps.
1. Collect twitter data - run GetCityTweetsUSA.py to start collecting tweets
2. Process the data and store to a database using - ProcessTweets.py
3. Create a vector-space model to recommend #’s using - TrainSaveModel.py

LoadCleanTweets.py contains all the functions and a class for processing the tweets.  HollaFunctions.py contains functions for processing the data and training the model.