# -*- coding: utf-8 -*-
"""
Created on Wed Sep 20 22:33:05 2023

@author: 91701
"""

from mrjob.job import MRJob

class BigramCount(MRJob):

    def configure_args(self):
        super(BigramCount, self).configure_args()
        self.add_passthru_arg('--stopwords', type=str, help='Comma-separated list of stopwords to exclude')

    def mapper(self, _, line):
        # Tokenize the line into words
        words = line.split()

        # Get the list of stopwords from the command line argument
        stopwords = set(self.options.stopwords.split(','))

        # Generate bigrams and emit them as key-value pairs
        for i in range(len(words) - 1):
            word1 = words[i].lower()
            word2 = words[i + 1].lower()

            # Exclude bigrams with stopwords
            if word1 not in stopwords and word2 not in stopwords:
                yield f"{word1},{word2}", 1

    def reducer(self, bigram, counts):
        # Sum the counts for each bigram
        yield bigram, sum(counts)

if __name__ == '__main__':
    BigramCount.run()
