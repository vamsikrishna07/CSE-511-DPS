#!/usr/bin/python2.7## Assignment2 Interface#
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()


def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    connection1 = openconnection
    cursor1 = connection1.cursor()
    rows = []
    cursor1.execute('SELECT * FROM RangeRatingsMetadata')
    eachRow = cursor1.fetchone()
    while eachRow is not None:
        partition_number, minRating, maxRating = eachRow[0], eachRow[1], eachRow[2]
        if (ratingMinValue <= maxRating and ratingMaxValue >= maxRating) or (ratingMinValue <= minRating and ratingMaxValue >= minRating):
            connection2 = openconnection
            cursor2 = connection2.cursor()
            cursor2.execute('SELECT * FROM {0} as r WHERE r.Rating>={1} and r.Rating<={2}'.format(
                "RangeRatingsPart"+str(partition_number), ratingMinValue, ratingMaxValue))
            row = cursor2.fetchone()
            while row is not None:
                row = list(row)
                row.insert(0, "RangeRatingsPart" + str(partition_number))
                rows.append(row)
                row = cursor2.fetchone()
        eachRow = cursor1.fetchone()

    cursor1.execute('SELECT * FROM RoundRobinRatingsMetadata')
    eachRow = cursor1.fetchone()
    partition_number, tableNextInsert = eachRow[0], eachRow[1]
    connection2 = openconnection
    cursor2 = connection2.cursor()
    for i in range(partition_number):
        cursor2.execute('SELECT * FROM {0} as r WHERE r.Rating>={1} and r.Rating<={2}'.format(
            "RoundRobinRatingsPart"+str(i), ratingMinValue, ratingMaxValue))
        row = cursor2.fetchone()
        while row is not None:
            row = list(row)
            row.insert(0, "RoundRobinRatingsPart" + str(i))
            rows.append(row)
            row = cursor2.fetchone()
    writeToFile('RangeQueryOut.txt', rows)


def PointQuery(ratingsTableName, ratingValue, openconnection):
    connection1 = openconnection
    cursor1 = connection1.cursor()
    rows = []
    cursor1.execute('SELECT * FROM RangeRatingsMetadata')
    eachRow = cursor1.fetchone()
    while eachRow is not None:
        partition_number, minRating, maxRating = eachRow[0], eachRow[1], eachRow[2]
        if ratingValue >= minRating and ratingValue <= maxRating:
            connection2 = openconnection
            cursor2 = connection2.cursor()
            cursor2.execute('SELECT * FROM {0} as r WHERE r.Rating={1}'.format(
                "RangeRatingsPart"+str(partition_number), ratingValue))
            row = cursor2.fetchone()
            while row is not None:
                row = list(row)
                row.insert(0, "RangeRatingsPart" + str(partition_number))
                rows.append(row)
                row = cursor2.fetchone()
        eachRow = cursor1.fetchone()

    cursor1.execute('SELECT * FROM RoundRobinRatingsMetadata')
    eachRow = cursor1.fetchone()
    partition_number, tableNextInsert = eachRow[0], eachRow[1]
    connection2 = openconnection
    cursor2 = connection2.cursor()
    for i in range(partition_number):
        cursor2.execute('SELECT * FROM {0} as r WHERE r.Rating={1}'.format(
            "RoundRobinRatingsPart"+str(i), ratingValue))
        row = cursor2.fetchone()
        while row is not None:
            row = list(row)
            row.insert(0, "RoundRobinRatingsPart" + str(i))
            rows.append(row)
            row = cursor2.fetchone()
    writeToFile('PointQueryOut.txt', rows)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
