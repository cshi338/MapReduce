import string
import random

# Map function counts differently lengthed words, puts each line's results into its own intermediate file
def map(key, value):
    fileName = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5))
    fileName = 'test/wordLength/intermediates/intermediate' + fileName
    with open(fileName + ".txt", 'a') as line:
        for x in value.split():
            for char in string.punctuation:
                x = x.replace(char, '')
            line.write(str(len(x)) + " 1" + '\n')

# Reduce function goes through intermediate files, counts up each key's frequencies, creates individual output files for each key
def reduce(key, values, outputFileLocation):
    count = 0
    for file in values:
        intermediate = open(file)
        words = intermediate.readlines()
        for i in words:
            j = (i.strip().split(' '))[0]
            if j == key:
                count += 1
    keyValue = (key, count)
    fileName = 'output' + key
    with open(outputFileLocation + "/" + fileName + ".txt", 'a') as line:
        line.write(' '.join(str(i) for i in keyValue))