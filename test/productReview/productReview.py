import string
import random

# Map function counts product reviews, puts each line's results into its own intermediate file
def map(key, value):
    fileName = ''.join(random.choices(string.ascii_uppercase + string.digits, k = 5))
    fileName = 'test/productReview/intermediates/intermediate' + fileName
    with open(fileName + ".txt", 'a') as line:
        user_id, product_id, rating_stars_out_of_5, time_stamp = value.split("::")
        line.write(str(product_id) + " 1" + '\n')

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
    fileName = 'output_' + key
    with open(outputfileLocation + "/" + fileName + ".txt", 'a') as line:
        line.write('product_id:'+ str(key) + ' --> ' + str(count)+' reviews')