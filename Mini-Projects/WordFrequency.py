from mrjob.job import MRJob
import re

WORD_REGEXP = re.compile(r"[\w']+")


class WordFrequency(MRJob):

    def mapper(self, _, line):
        # words = line.split() -> split based on the whitespace
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    WordFrequency.run()
