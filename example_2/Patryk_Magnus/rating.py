from mrjob.job import MRJob

class MRHotelRaitingCount(MRJob):
    def mapper(self, _, line):
        (userId, movieId, rating, timestamp) = line.split(',')
        if rating != 'rating':
            result = [movieId, float(rating)]
            yield result
        

    def reducer(self, key, values):
        s = 0 
        c = 0 
        for value in values:
            s += value
            c += 1
        result = [key, (s / c)]
        yield result

if __name__ == '__main__':
    MRHotelRaitingCount.run()