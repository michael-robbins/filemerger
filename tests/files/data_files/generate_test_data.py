#!/usr/bin/python

import csv

with open("bigdata1.tsv", "wb") as data3_file:
    writer = csv.writer(data3_file, delimiter='\t')

    for data in range(111111, 999999, 2):
        writer.writerow([str(data), "abcde", "blah123"])

with open("bigdata2.tsv", "wb") as data3_file:
    writer = csv.writer(data3_file, delimiter='\t')

    for data in range(111112, 999999, 2):
        writer.writerow([str(data), "abcdf", "blah124"])
