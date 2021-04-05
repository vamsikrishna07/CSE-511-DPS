# !/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2, psycopg2.extras

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    file = open(ratingsfilepath)
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    # create ratings Table
    tablename = 'ratings'
    try:
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor1 = openconnection.cursor()
        command1 = (
            """ DROP TABLE IF EXISTS {}""".format(tablename),
            """
            CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            userid INT NOT NULL,
            movieid INT NOT NULL,
            rating decimal NOT NULL
            )
          """.format(tablename)
        )
        for each in command1:
            cursor1.execute(each)
        openconnection.commit();
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)    
        
    with openconnection.cursor() as cursor2:
        iterate = ({ 'userid': each.split("::")[0],'movieid': each.split("::")[1],'rating': each.split("::")[2],} for each in file)
        command2 = "INSERT INTO " + ratingstablename+ " (userid, movieid, rating) VALUES (%(userid)s, %(movieid)s, %(rating)s);"
        psycopg2.extras.execute_batch(cursor2,command2,iterate)
    


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    # finding total number of rows
    cursor1 = openconnection.cursor();
    cursor1.execute('SELECT COUNT(rating) from {}'.format(ratingstablename));
    rowCount = cursor1.fetchone()[0];
    
    # maximum of the ratings 
    cursor2 = openconnection.cursor();
    cursor2.execute('SELECT MAX(rating) from ' + ratingstablename);
    maxRating = cursor2.fetchone()[0];
    
    # list of partitions
    level = maxRating / numberofpartitions
    partitionList =  [[level*i, level*(i+1)] for i in range(numberofpartitions)]
    
    # Creating range partitions tables (metadata)
    try:
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor3 = openconnection.cursor()
        tablename = "metadata_range_table"
        command = (
            """ DROP TABLE IF EXISTS {}""".format(tablename),
            """
            CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            no_of_partitions INT NOT NULL,
            max_rating DECIMAL NOT NULL,
            total_count INT NOT NULL
            )
          """.format(tablename)
        )
        for each in command: 
            cursor3.execute(each)
        openconnection.commit();
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    # inserting to range partition tables (metadata)
    try:
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor4 = openconnection.cursor()
        tablename = "metadata_range_table"
        command = ("""INSERT INTO {} (no_of_partitions, max_rating, total_count) values( {}, {}, {} )""").format(
            tablename,
            numberofpartitions,
            maxRating,
            rowCount
        );
        cursor4.execute(command)
        openconnection.commit();
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)    
        
    # Creating range partitions
    cursor5 = openconnection.cursor();
    for index in range(len(partitionList)):
        tableName = "range_part{}".format(index);
        cursor5.execute("DROP TABLE IF EXISTS " + tableName);
        sql = "DROP TABLE IF EXISTS  {}; ".format(tableName);
        sql += "Select * into {}  from".format(tableName);
        if index==0:    sql += " (select * from ratings where rating >= {}".format(partitionList[index][0])
        else:   sql += " (select * from ratings where rating > {}".format(partitionList[index][0])
        sql += " and rating <={} ) as partition".format(partitionList[index][1])
        cursor5.execute(sql);        
    

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    # finding total number of rows
    cursor1 = openconnection.cursor();
    cursor1.execute('SELECT COUNT(rating) from {}'.format(ratingstablename));
    rowCount = cursor1.fetchone()[0];
    
    # creating round robin tables (metadata)
    try:
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor2 = openconnection.cursor()
        tablename = "metadata_round_table"
        command = (
            """ DROP TABLE IF EXISTS {}""".format(tablename),
            """
            CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            total_count INT NOT NULL,
            no_of_partitions INT NOT NULL
            )
          """.format(tablename)
        )
        for each in command:
            cursor2.execute(each)
        openconnection.commit();
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    # inserting into round robin tables (metadata)
    try:
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor3 = openconnection.cursor()
        tablename = "metadata_round_table"
        command = ("""INSERT INTO {} (total_count,no_of_partitions) values( {}, {} )""").format(tablename, rowCount, numberofpartitions);
        cursor3.execute(command)
        openconnection.commit();
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    # round robin partitioning
    cursor4 = openconnection.cursor()
    for index in range(1,numberofpartitions+1):
        tablename = "rrobin_part{}".format(index);    
        try:
            openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cursor5 = openconnection.cursor()
    
            command = (
                """ DROP TABLE IF EXISTS {}""".format(tablename),
                """
                CREATE TABLE {} (
                id SERIAL PRIMARY KEY,
                UserID INT NOT NULL,
                MovieID INT NOT NULL,
                Rating decimal NOT NULL
                )
              """.format(tablename)
            )
            for each in command:
                cursor5.execute(each)
            openconnection.commit();

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        sql = "Insert into " + tableName + " select * from " + ratingstablename + " where id = %(id)s";
        generator = ({'id': each } for each in xrange(index,rowCount+1,numberofpartitions))
        psycopg2.extras.execute_batch(cursor4,sql,generator)
    

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor1 = openconnection.cursor()
    cursor1.execute("select total_count, no_of_partitions from metadata_round_table");
    rowCount, partitionCount = cursor1.fetchone()
    partTable = "rrobin_part{}".format((rowCount%partitionCount)) 
    totalRowCount = rowCount+1
    command = (
        "Insert into {} (userid, movieid, rating) values ({}, {}, {})".format(
            ratingstablename,
            userid,
            itemid,
            rating

        ),
        "Insert into {} (id, userid, movieid, rating) values({}, {}, {}, {})".format(
            partTable,
            totalRowCount,
            userid,
            itemid,
            rating
        )
    )
    cursor2 = openconnection.cursor();
    for each in command:
        cursor2.execute(each);
        
    # update Round robing metadata
    try:
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor3 = openconnection.cursor()
        tablename = "metadata_round_table"
        command = ("""UPDATE {} set total_count = {}""").format(tablename, totalRowCount);
        cursor3.execute(command)
        openconnection.commit();
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)    
    


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    cursor1 = openconnection.cursor();
    cursor1.execute("select no_of_partitions, max_rating, total_count from metadata_range_table");
    partitionCount, maxRating, rowCount = cursor1.fetchone();
    
    level = maxRating / partitionCount
    partitionsArray = [[level*i, level*(i+1)] for i in range(partitionCount)]

    expectedPartition = 0;

    for index in range(len(partitionsArray)):
        if index == 0:
            if partitionsArray[index][0] <= rating <= partitionsArray[index][1]:
                expectedPartition = index;
                break
        else:
            if  partitionsArray[index][0] < rating <= partitionsArray[index][1]:
                expectedPartition = index;
                break;


    partTableName = "range_part{}".format(expectedPartition)
    totalRowCount = rowCount+1;

    command = (
        "Insert into {} (userid, movieid, rating) values ({}, {}, {})".format(
            ratingstablename,
            userid,
            itemid,
            rating

        ),
        "Insert into {} (id, userid, movieid, rating) values({}, {}, {}, {})".format(
            partTableName,
            totalRowCount,
            userid,
            itemid,
            rating
        )
    )
    cursor2 = openconnection.cursor();
    for each in command:
        cursor2.execute(each);    
    try:
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor3 = openconnection.cursor()
        tablename = "metadata_range_table"
        command = ("""UPDATE {} set total_count = {}""").format(tablename, totalRowCount);
        cursor3.execute(command)
        openconnection.commit();
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)    
    cursor2.close();    
    

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))
    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
