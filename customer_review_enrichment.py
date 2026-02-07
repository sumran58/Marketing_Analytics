import nltk
import pandas as pd
import pyodbc
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')

#define a function to fetch the data from sql server management 
def fetch_data_from_sql():
    #define the connection string with the parameters for the database connection 
   conn_str = (
    "Driver={SQL Server};"
    "Server=localhost\SQLEXPRESS;"
    "Database=PortfolioProject_MarketingAnalytics;"
    "Trusted_Connection=yes;"
)
   #establish the connetion to the database 
   conn=pyodbc.connect(conn_str)

   #define the sql query to fetch the customer review data from the table 
   query="Select ReviewID,CustomerID, ReviewDate,Rating, ProductID, ReviewText from dbo.vw_customer_reviews_clean"

   #execute the query and fetch the data into datafra.me 
   df=pd.read_sql(query,conn)

   #closing the connection to free up the resources 
   conn.close()

   return df

#feth the customer review data from the sql database 
customer_reviews_df=fetch_data_from_sql()

#initilaize the vader sentiment analyser for analyzing the sentiment of the data 
sia=SentimentIntensityAnalyzer()

#defie the function to calculate the sentiments scores 
def caluclate_sentiments(review):
   #get the sentiment scores for the review text 
   sentiment=sia.polarity_scores(review)
   #return the compiiund scored which is the normalized scres between -1(negative) and 1(positive)
   return sentiment['compound']

#defining the function that will be categorizing the sentiments using both the scores and the rating 
def categorize_sentiments(score,rating):
   # Use both the text sentiment score and the numerical rating to determine sentiment category
    if score > 0.05:  # Positive sentiment score
        if rating >= 4:
            return 'Positive'  # High rating and positive sentiment
        elif rating == 3:
            return 'Mixed Positive'  # Neutral rating but positive sentiment
        else:
            return 'Mixed Negative'  # Low rating but positive sentiment
    elif score < -0.05:  # Negative sentiment score
        if rating <= 2:
            return 'Negative'  # Low rating and negative sentiment
        elif rating == 3:
            return 'Mixed Negative'  # Neutral rating but negative sentiment
        else:
            return 'Mixed Positive'  # High rating but negative sentiment
    else:  # Neutral sentiment score
        if rating >= 4:
            return 'Positive'  # High rating with neutral sentiment
        elif rating <= 2:
            return 'Negative'  # Low rating with neutral sentiment
        else:
            return 'Neutral'  # Neutral rating and neutral sentiment

# Define a function to bucket sentiment scores into text ranges
def sentiment_bucket(score):
    if score >= 0.5:
        return '0.5 to 1.0'  # Strongly positive sentiment
    elif 0.0 <= score < 0.5:
        return '0.0 to 0.49'  # Mildly positive sentiment
    elif -0.5 <= score < 0.0:
        return '-0.49 to 0.0'  # Mildly negative sentiment
    else:
        return '-1.0 to -0.5'  # Strongly negative sentiment

#applly sentiment analysis to caluclate sentiment score for every review tesxt 
customer_reviews_df['SentimentScores']=customer_reviews_df['ReviewText'].apply(caluclate_sentiments)

customer_reviews_df['SentimentCategory'] = customer_reviews_df.apply(
    lambda row: categorize_sentiments(row['SentimentScores'], row['Rating']),
    axis=1
)

customer_reviews_df['SentimentBucket'] = customer_reviews_df['SentimentScores'].apply(sentiment_bucket)

print(customer_reviews_df.head())
customer_reviews_df.to_csv('customer_reviews_with_sentiments.csv',index=False)



