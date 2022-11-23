# PYTHON 3.9

"""
    SENTIMENT ANALYSIS ON AMAZON REVIEWS
    Analysis and processing of massive data
"""



################################################################
##              Download database content                     ##
################################################################

from termcolor import colored
import pandas as pd
import csv

# Reading the file
bd = pd.read_csv("Amazon_Unlocked_Mobile.csv")
print(colored("\nAmazon_Unlocked_Mobile.csv file: \n", 'blue'), bd)
print(colored('\nColonnes: \n', 'blue'), bd.columns)

# Display of brands, 'Brand Name' column
bd_select = pd.read_csv("Amazon_Unlocked_Mobile.csv", usecols=[1, 3])
print(colored('\nBrand Name Head 1000: \n', 'blue'),bd_select.head(1000))
print(colored('\nBrand Name Tail 1000: \n', 'blue'),bd_select.tail(1000),'\n')

# Data Processing
bd.columns = ['Product', 'Brand', 'Price', 'Rating', 'Reviews', 'Votes']
del (bd['Product'], bd['Price'], bd['Rating'], bd['Votes'])
df = bd.dropna(subset=['Brand', 'Reviews'])

"""
    Explanation: 
        We can see that there are missing values in the "Amazon_Unlocked_Mobile.csv" file in the essential 
        columns like the product brand "Brand Name" and that of the comments "Reviews" which will be useful 
        for us for the next questions. it was therefore necessary to delete the lines concerned. I created 
        a new file in preparation for the next question. I deleted the columns that are not of interest 
        to us to keep only those of "Brand Name" and "Reviews".
"""



################################################################
## Find the 20 most frequent brands in the database, and      ##
## display them in descending order.                          ##
################################################################

bd_sorted = df.sort_values('Brand', ascending=False)
freq1 = bd_sorted.groupby(['Brand']).size()
print(colored('\nfreq Brand Name: \n', 'blue'), freq1[0:])
select = freq1.sort_values(ascending=False)
print(
   colored('\nThe 20 most frequent brands in descending order: \n','blue'),
   select[0:20]
)

"""
    Explanation: 
        We see that the brand "Samsung" is found twice in the list under the name "Samsung" and "samsung".
        It is therefore necessary to merge the two brands. This would give "Samsung" a total of 68178 
        occurrences and allows to add the brand "Casio". To avoid this kind of problem, it is necessary 
        to put all the brand names, as well as the comments in lower case, which is to be done afterwards 
        to have better results*.
"""



################################################################
## Map Reduce functions that calculate the frequency of words ##
## in the review text of each brand found previously          ##
################################################################

import collections
base_donnees_resultat = select
new_bd = base_donnees_resultat.dropna(subset=['Marque', 'Mot'])
line = base_donnees_resultat['Mot'].dropna()

def mapper(line):
    liste_sentences = line.str.split()
    return liste_sentences

def reducer(liste_sentences):
    liste_words = []
    for liste in liste_sentences.tolist():
    list_of_tuples = [value for key, value in collections.Counter(liste).items()]
    liste_words.append(list_of_tuples)
    return liste_words


################################################################
##      Calculate execution time for functions                ##
################################################################

import time

# Calculate execution time
start = time.time()
a = mapper(line)
b = reducer(a)
end = time.time()
print(colored("\nLe temps d'exécution: ", 'blue'), (end - start))

# File creation
df = pd.DataFrame({'Marque': new_bd['Marque'] ,'Mot': a , 'Frequency': b})
print('\n', df)

"""
    Explanation:
        From what I understood from the statement, it was necessary to put the list of words used in the 
        column "Mot" and the correspondence of their frequency in that of "Frequence". With more time, 
        the word list should be sorted to remove at least stop words, punctuation and emoticons for 
        better results.
"""


################################################################
## Export the previous results in a CSV file with the columns ##
##              (Marque, Mot, Frequence)                      ##
################################################################
import os

path = r'./data/resultatq3.csv'
file = open(path, 'w', newline='', encoding="utf8")
assert os.path.isfile(path)

with file as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(['Marque', 'Mot', 'Frequence'])
    for index, value in df.iterrows():
        wr.writerow(value)

file.close()

"""
    Explanation: 
        I have inserted the list of words and their frequencies but afterwards, it would be necessary 
        to keep the most frequent ones or a single word, the one that comes up most often.
"""


################################################################
##    Create a new MongoDB database with name finalproject    ##
################################################################
import pymongo
from pymongo import Connection
from pymongo import MongoClient

client = pymongo.MongoClient("mongodb://localhost:27017/")
database = client["projetfinal"]
collection = database ["projetfinal"]

"""
    Explanation: 
        After manipulation with the SQL queries, I find that it would be easier to put them in pairs 
        in order to work on them. With more time, the word list should be sorted to at least remove 
        stop words, punctuation, and emoticons.
"""


################################################################
##   Import the new CSV file into the finalproject database   ##
################################################################

data = r'./data/resultatq4.csv'
collection.insert_many(data)



################################################################
##  SQL query to display data from the projectfinal database  ##
################################################################

for doc in collection.find():
    print(doc)

"""
    Explanation: 
        After manipulation with the SQL queries, I find that it would be easier to put the words and 
        their frequencies in the form of pairs in order to work on them or to keep only the most
        frequent word or words for each Mark.
"""


################################################################
## SQL query to display the brand with the most frequent word ##
################################################################

for doc in collection.find('{$and: {Marque, Frequence:-1}}'):
    print(doc)

"""
    Explanation: 
    Unfortunately, with the word and frequency lists, I wasn't able to come up with a decent query.
"""


