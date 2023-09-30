from mrjob.job import MRJob

class MRMovies(MRJob):

    def mapper(self, _, line):
        userId, _, _, _ = line.split(',')
        yield userId, 1

    def combiner(self, userid, counts):
        yield userid, sum(counts)

    def reducer(self, userid, counts):
        yield userid, sum(counts)

if __name__ == '__main__':
    MRMovies.run()

