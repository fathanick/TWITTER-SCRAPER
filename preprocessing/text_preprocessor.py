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

#spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder \
            .config("spark.ui.enabled", "false") \
            .config("spark.executor.memory", "16g") \
            .config("spark.driver.memory", "16g") \
            .config("spark.memory.offHeap.enabled","true") \
            .config("spark.memory.offHeap.size","16g") \
            .getOrCreate()

parse = argparse.ArgumentParser()

parse.add_argument('--input_file', type=str, default=None) # csv file
parse.add_argument('--outdir_path', type=str, default=None) # directory where to save multiple txt files
parse.add_argument('--n_lines', type=int, default=None) # number of lines for each txt file
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
sentences = []
for row in tqdm(filtered_data):
    preprocessed_text = []
    tw = re.sub(r'(\n\n|\n)', ' ', row[0])
    tw = emoji.demojize(tw, language = 'en', delimiters=(' ', ' '))
    tw = tw.lower()
    sentence = sent_tokenize(tw)
    sentences.append(sentence)

sentences_flatten = [item for sublist in sentences for item in sublist]
print("N sentence before filtering: ",len(sentences_flatten))

filtered_sentences = []
for s in tqdm(sentences_flatten):
    # append to list if length of the sentence >= 3
    if len(s.split()) >= 3:
        filtered_sentences.append(s)

# print(filtered_sentences)
print("N sentence after filtering: ",len(filtered_sentences))

preprocessed_sentences = []
for stc in tqdm(filtered_sentences):
    clean_text = []
    for s in stc.split():
        if len(s) > 1:
            s = '@USER' if s[0] == '@' and s.count('@') == 1 else s
            s = 'HTTPURL' if s.startswith('http') else s
        clean_text.append(s)
    new_text = ' '.join(clean_text)
    preprocessed_sentences.append(new_text)

print("Number of sentences: ",len(preprocessed_sentences))
# print(preprocessed_sentences)

n_items = args.n_lines
sent_len = (len(preprocessed_sentences))

count = 0
dir_path = args.outdir_path

# to get the latest number of current txt files
# Iterate directory
for file in tqdm(os.listdir(dir_path)):
    # check if current path is a file
    if(file.endswith('txt')):
        #print(file)
        count += 1

print('Total existing text file count:', count)

for item in tqdm(range(0, sent_len, n_items)):
    with open(dir_path + "/file_" + str((item//n_items)+count+1) + ".txt", "w") as f:
        f.write("\n".join(preprocessed_sentences[i] for i in range(item, min(item+n_items, len(preprocessed_sentences) - 1))))
    f.close()
    
time_end = time.time()
running_time = time_end-time_start
print("Execution time in seconds: ",(running_time))


# To execute:
# python text_preprocessor.py --input_file ../raw_data/tweets-by-keywords-2023-01-31-v01.csv --outdir_path txt_files --n_lines 500000 