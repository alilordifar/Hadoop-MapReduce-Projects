from mrjob.job import MRJob
from mrjob.step import MRStep


class MostPopularMovie(MRJob):

    def configure_args(self):
        """
        This function is going to add file option,
        so it sends the name files to all machines in the cluster
        """
        super(MostPopularMovie, self).configure_args()
        self.add_file_arg('--items', help='path to u.item')

    def steps(self):
        # Define the steps
        return [
            MRStep(mapper=self.map_get_ratings,
                   reducer_init=self.reducer_init
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_find_max)
        ]

    def map_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, rating

    def reducer_init(self):
        self.movieNames = {}

        with open("u.ITEM") as f:
            for line in f:
                fileds = line.split('|')
                self.movieNames[fileds[0]] = fields[1]

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), self.movieNames[key])

    def reducer_find_max(self, key, values):
        yield max(values)


if __name__ == '__main__':
    MostPopularMovie.run()

# to run: !pyhton MostPopularMovie.py --items=ml-100k/u.ITEM ml-100k/u.data
