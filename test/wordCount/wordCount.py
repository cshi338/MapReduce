import string
import random

# Map function counts different words, puts each line's results into its own intermediate file
def map(key, value):
    fileName = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5))
    fileName = 'test/wordCount/intermediates/intermediate' + fileName
    with open(fileName + ".txt", 'a') as line:
        for x in value.split():
            for char in string.punctuation:
                x = x.replace(char, '')
            line.write(x.lower() + " 1" + '\n')

# Reduce function goes through intermediate files, counts up each key's frequencies, creates individual output files for each key
def reduce(key, values, outputfileLocation):
    count = 0
    for intermediates in values:
        intermediate = open(intermediates)
        words = intermediate.readlines()
        for i in words:
            j = (i.strip().split(' '))[0]
            if j == key:
                count += 1
    keyValue = (key, count)
    fileName = 'output' + key
    with open(outputfileLocation + "/" + fileName + ".txt", 'a') as line:
        line.write(' '.join(str(i) for i in keyValue))