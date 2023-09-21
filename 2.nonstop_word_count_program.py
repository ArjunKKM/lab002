# -*- coding: utf-8 -*-
"""
Created on Wed Sep 20 18:43:50 2023

@author: 91701
"""

from mrjob.job import MRJob
import re

# List of stopwords
STOPWORDS = set(["the", "and", "of", "a", "to", "in", "is", "it"])

# Regular expression to split words by non-alphabetic characters
WORD_RE = re.compile(r"\b\w+\b")

class NonStopWordCount(MRJob):

    def mapper(self, _, line):
        # Split the line into words using the regular expression
        words = re.findall(WORD_RE, line.lower())
        # Emit each non-stop word as a key with a value of 1
        for word in words:
            if word not in STOPWORDS:
                yield (word, 1)

    def reducer(self, word, counts):
        # Sum the counts for each word
        yield (word, sum(counts))

if __name__ == '__main__':
    NonStopWordCount.run()
