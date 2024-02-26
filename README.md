<h1>Apache Spark</h1>
<p>Goal: Display book pairs that are most often reviewed together</p>

<h2>Goodreads Data (sample)</h2>

```
1:950,963
2:1072,1074,1210
3:1488
4:1313,1527,1557,1566,1616,1620
5:5316
7:1386,1387,1543,536,5770,7232,7233
8:7407
9:1473,1591,3352,3353,4466,4501,5211,5235,5236,5238,5333,5903,6175,6229,6294,6353,6452,6544,6545,6651,6744,6808,6866,6867,6868,7460,7463,7466,7467,7468,7471,7474,7475,7478,7490,7493,7506,7508,999
12:6183,6304,7051,7351,8096
13:919
```

<p>Here, the data is noted by book_ids that each user has reviewed.</p>

### MapReduce Steps
```
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
freqCounts = bookCounts.reduceByKey(lambda a,b: a+b)        #aggregate total count for pairs
result = freqCounts.filter(lambda occ: occ[1]>20)           #filters out pairs with more than 20 reviews
result.saveAsTextFile("/home/cs143/output1")                #saves output into directory
```

### MapReduce Output
```
((1386, 536), 283)
((1473, 1591), 39)
((1473, 7475), 27)
((1473, 7493), 30)
((1473, 999), 36)
((5236, 5238), 87)
((6744, 7474), 25)
((6744, 7490), 25)
((7460, 7490), 21)
((7474, 7490), 31)
((1000, 1116), 181)
((1000, 12710), 22)
((1000, 1628), 29)
((1000, 536), 110)
((1000, 66), 141)
```
<p>Tuples of book_ids with their associated occurences</p>

<h2>Goodreads with SparkSQL</h2>

- Join dataset with Goodread Titles

- Select book1, book2, and frequency

- Output data with tab delimiter

### SparkSQL output
```
The Odyssey     The Lovely Bones        319
Cliffs Notes on Homer's The Odyssey     The Odyssey     300
Cliffs Notes on Homer's The Odyssey     The Lovely Bones        283
The Oresteia    The Oresteia: Agamemnon/The Libation Bearers/The Eumenides      189
```
