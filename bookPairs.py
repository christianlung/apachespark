#Python script for computing High-Frequency Book Pairs from Goodreads

#set up SparkContext for PairCount application
from pyspark import SparkContext
sc = SparkContext("local", "BookPairs")

#utility function
def permute(arr):
    return [(int(arr[i]), int(arr[j])) for i in range(len(arr)-1) for j in range(i+1, len(arr))]

#map-reduce
lines = sc.textFile("/home/cs143/data/goodreads.dat")
books_string = lines.map(lambda line: line.split(":")[-1])  #remove everything before colons
books = books_string.map(lambda book: book.split(","))      #each line turns into array of string books
pairs = books.flatMap(lambda book: permute(book))           #all permutations

bookCounts = pairs.map(lambda pair: (pair,1))               #map each pair to count 1
freqCounts = bookCounts.reduceByKey(lambda a,b: a+b)
result = freqCounts.filter(lambda occ: occ[1]>20)
result.saveAsTextFile("/home/cs143/output1")