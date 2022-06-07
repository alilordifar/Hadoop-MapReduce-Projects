from mrjob.job import MRJob
from mrjob.step import MRStep


class MostPopularSuperhero(MRJob):

    def configure_args(self):
        """
        This function is going to add file option,
        so it sends the name files to all machines in the cluster
        """
        super(MostPopularSuperhero, self).configure_args()
        self.add_file_arg('--names', help='path to Marvel-names.txt')

    def steps(self):
        # Define the steps
        return [
            MRStep(mapper=self.mapper_friends_count_per_line,
                   reducer=self.reducer_combine_friends),
            MRStep(mapper=self.mapper_prep_for_sort,
                   reducer_init=self.load_name_dictionary,
                   reducer=self.reducer_find_max_friends)
        ]

    def get_friends_count(self, _, line):
        fields = lins.split()
        heroID = fields[0]
        numFriends = len(fields) - 1
        yield int(heroID), int(numFriends)

    def reducer_combine_friends(self, heroID, friendCounts):
        yield heroID, sum(friendCounts)

    def mapper_prep_for_sort(self, heroID, friendCounts):
        heroName = self.heroNames[heroID]
        yield None, (frienCounts, heroName)

    def reducer_find_max_friends(self, key, value):
        yield max(value)

    def load_name_dictionary(self):
        self.heroNames = {}

        with open("Marvel-names.txt") as f:
            for line in f:
                fields = line.split('"')
                heroID = int(fields[0])
                self.heroNames[heroID] = fields[1]


if __name__ == '__main__':
    MostPopularSuperhero.run()

# to run: !python MosMostPopularSuperhero.py --names=Marvel-names.txt Marvel-graph.txt
