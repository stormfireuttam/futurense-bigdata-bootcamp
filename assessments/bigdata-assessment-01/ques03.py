------------------------

local directory : python3 mrjob.py -r hadoop hdfs://localhost:9000/user/training/movies/movies1.csv

Shell Script File:
--------------------

from mrjob.job import MRJob

class countRatings(MRJob):

    def mapper(self, key, line):
        if line.split(',')[2][0] != 't':
            yield(line.split(',')[2],1)

    def reducer(self, rating, count):
        yield(float(rating), sum(count))

if name == 'main':
    countRatings.run()
