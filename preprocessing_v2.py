import os
import re
import glob
import time
import emoji
import argparse
from tqdm import tqdm
from nltk.tokenize import sent_tokenize
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

parse = argparse.ArgumentParser()

parse.add_argument('--input_file', type=str, default=None) # csv file
parse.add_argument('--output_file', type=str, default=None)
args = parse.parse_args()

time_start = time.time()
# create spark dataframe
df = spark.read \
        .option("header",True) \
        .option("multiLine",True) \
        .option('sep', ',') \
        .option("escape", "\"") \
        .csv(args.input_file)

# get text from dataframe
get_text = df.select("Text")
print("N data before: ",get_text.count())
#get_text.show()

# Pre-processing 1
# get unique data from text => filter based on unique tweets
get_text = get_text.distinct()
print("N data after removing duplicate tweets: ",get_text.count())


# Pre-processing 2
# remove tweets with token length less than 3
get_text = get_text.filter( F.size(F.split('text', ' ')) >= 3)
print("N data after removing tweets with length less than 3: ",get_text.count())

# Pre-processing 3
# remove multiple space, replace @user and HTTPURL
filtered_data = get_text.collect()

preprocessed_data = []
for row in tqdm(filtered_data):
    tw = re.sub(r'(\n\n|\n)', ' ', row[0])
    tw = emoji.demojize(tw, language = 'en', delimiters=(' ', ' '))
    tw = tw.lower()
    clean_text = []
    for t in tw.split():
        if len(t) > 1:
            t = '@USER' if t[0] == '@' and t.count('@') == 1 else t
            t = 'HTTPURL' if t.startswith('http') else t
        clean_text.append(t)
    new_text = ' '.join(clean_text)
    preprocessed_data.append(new_text)
    
with open(args.output_file, 'w', encoding="utf-8") as f:   
    for item in preprocessed_data:
        f.write(item + '\n')

'''
python preprocessing_v2.py --input_file ../raw_data/tweets-by-username-2023-02-13-v01.csv --output_file ../clean_data/clean_tweet.txt                           
'''