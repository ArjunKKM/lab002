# -*- coding: utf-8 -*-
"""
Created on Wed Sep 20 22:57:06 2023

@author: 91701
"""

from mrjob.job import MRJob
import re

# Regular expression to split words by non-alphabetic characters
WORD_RE = re.compile(r"\b\w+\b")

class InvertedIndex(MRJob):

    def mapper(self, _, line):
    # Check if the line has the expected format
     if ":" in line:
        document_number, text = line.split(':', 1)
        words = re.findall(WORD_RE, text.lower())
        for word in words:
            yield (word, document_number.strip())
     else:
        # Handle lines with unexpected format (e.g., empty lines)
        pass
        

    def reducer(self, word, documents):
        # Create a set to store unique document IDs
        unique_documents = set()

        # Add each document ID to the set
        for document in documents:
            unique_documents.add(document)

        # Emit the word and the list of documents where it appears
        yield (word, sorted(list(unique_documents)))

if __name__ == '__main__':
    InvertedIndex.run()
