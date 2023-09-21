

from mrjob.job import MRJob
import re

# Regular expression to split words by non-alphabetic characters
WORD_RE = re.compile(r"\b\w+\b")

class WordCount(MRJob):

    def mapper(self, _, line):
        # Split the line into words using the regular expression
        words = re.findall(WORD_RE, line.lower())
        # Emit each word as a key with a value of 1
        for word in words:
            yield (word, 1)

    def reducer(self, word, counts):
        # Sum the counts for each word
        yield (word, sum(counts))

if __name__ == '__main__':
    WordCount.run()