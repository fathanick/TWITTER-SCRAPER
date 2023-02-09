import snscrape.modules.twitter as twitter
import pandas as pd
import argparse
from datetime import date


today = date.today()

parse = argparse.ArgumentParser()
parse.add_argument('--src_file', type=str, default=None)
parse.add_argument('--max_tweet', type=int, default=None)
args = parse.parse_args()

def get_tweets_by_username(max_tweet=2, filename=''):

    username_list = open(filename, 'r')
    all_tweets = []
    tweet_list = []

    for n, username in enumerate(username_list):
        for i, tweet in enumerate(twitter.TwitterSearchScraper(f'from:{username}').get_items()):
            if i > max_tweet:
                break
            print(i)
            print(tweet.user.username)
            print(tweet.rawContent)
            print(tweet.url)
            print('\n')
            tweet_list.append([tweet.date, tweet.id, tweet.rawContent, tweet.user.username])
        all_tweets.extend(tweet_list)

    # convert list to dataframe
    df = pd.DataFrame(all_tweets, columns=['Datetime', 'Tweet Id', 'Text', 'Username'])

    return df


# read filename
filename = f'search_keywords/{args.src_file}'
df = get_tweets_by_username(max_tweet=args.max_tweet, filename=filename)
new_df = df.drop_duplicates()

file_exist = True
count = 1
saved_file = ''

while file_exist:
    saved_file = f'raw_data/tweets-by-username-{today}'+'-v{:02.0f}.csv'.format(count)
    try:
        f = open(saved_file, 'x')
        file_exist = False
    except FileExistsError:
        count += 1
f.close()

new_df.to_csv(saved_file, index=False)
print(new_df)
print(len(new_df))
