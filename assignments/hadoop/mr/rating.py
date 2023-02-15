from mrjob.job import MRJob


class ratingcount(MRJob):
	""" Mapper function for mapping """
        def mapper(self, _, line):
        	#print(line.split('\t'))
                (userID,movieID, rating, timestamp) = line.split(',')
                yield(rating, 1)
	""" Reducer function to reduce and produce results """
        def reducer(self, rate, counts):
                yield(rate, sum(counts))
if __name__ == '__main__':
	ratingcount.run()
