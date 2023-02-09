import snscrape.modules.twitter as twitter
import pandas as pd
import argparse
from datetime import date

today = date.today()

parse = argparse.ArgumentParser()
parse.add_argument('--src_file', type=str, default=None)
parse.add_argument('--max_tweet', type=int, default=None)
parse.add_argument('--start_date', type=str, default=None)
parse.add_argument('--end_date', type=str, default=None)
args = parse.parse_args()

def get_tweets_by_keywords(max_tweet=2, keyword='', start='', end=''):
    tweet_list = []
    
    # issue: https://github.com/JustAnotherArchivist/snscrape/issues/634
    scrp_param = f'{keyword} since:{start} until:{end} filter:safe'
    for i, tweet in enumerate(twitter.TwitterSearchScraper(scrp_param).get_items()):
        if i > max_tweet:
            break
        print(i)
        print(tweet.user.username)
        print(tweet.rawContent)
        print(tweet.url)
        print('\n')
        tweet_list.append([tweet.date, tweet.id, tweet.rawContent, tweet.user.username])

    df = pd.DataFrame(tweet_list, columns=['Datetime', 'Tweet Id', 'Text', 'Username'])

    return df


all_df = pd.DataFrame()
filename = f'search_keywords/{args.src_file}'
with open(filename, 'r') as f:
    for keyword in f:
        df = get_tweets_by_keywords(max_tweet=args.max_tweet,
                                    keyword=keyword,
                                    start=args.start_date,
                                    end=args.end_date)
        all_df = pd.concat([all_df, df]).drop_duplicates()

file_exist = True
count = 1
saved_file = ''

while file_exist:
    saved_file = f'raw_data/tweets-by-keywords-{today}'+'-v{:02.0f}.csv'.format(count)
    try:
        f = open(saved_file, 'x')
        file_exist = False
    except FileExistsError:
        count += 1
f.close()

all_df.to_csv(saved_file, index=False)
print(all_df)
print(len(all_df))

# command 
# python scrape_by_keywords.py --src_file keywords-47.txt --max_tweet 30000 --start_date 2017-01-01 --end_date 2023-01-31