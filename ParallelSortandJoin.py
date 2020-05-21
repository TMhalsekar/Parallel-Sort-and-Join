#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    cur=openconnection.cursor()
    cur.execute("Select min("+ SortingColumnName+")from "+ InputTable+";")
    lower=cur.fetchone()[0]
    cur.execute("select max("+SortingColumnName+")from "+InputTable+";")
    upper=cur.fetchone()[0]
    noofthreads=5
    inter=float(upper-lower)/noofthreads
    cur.execute("Select column_name,data_type from information_schema.columns where table_name =\'" + InputTable + "\';")
    TempTable=cur.fetchall()
    for i in range(noofthreads):
        tname="range_part"+ str(i)
        cur.execute("create table "+ tname + "(" + TempTable[0][0]+ " " + TempTable[0][1] + ");")
        for j in range(1,len(TempTable)):
            cur.execute("alter table " + tname + " add column "+ TempTable[j][0] + " " + TempTable[j][1]+";")
    thread=[0,0,0,0,0]
    for i in range(noofthreads):
        if i==0:
            lowerv=lower
            upperv=lowerv + inter
        else:
            lowerv=upperv
            upperv=upperv + inter
        thread[i] = threading.Thread(target=ParaSort,
                                      args=(InputTable, SortingColumnName, i, lowerv, upperv, openconnection))
        thread[i].start()

    for i in range(noofthreads):
        thread[i].join()

    temp = "create table " + OutputTable + " (" + TempTable[0][0] + " INTEGER);"
    cur.execute(temp)
    for i in range(1, len(TempTable)):
        temp = "alter table " + OutputTable + " add column " + TempTable[i][0] + " " + TempTable[i][1] + ";"
        cur.execute(temp)
    for i in range(noofthreads):
        temp = "insert INTO " + OutputTable + " select * from range_part" + str(i) + ";"
        cur.execute(temp)

    for i in range(noofthreads):
        temp = "drop table if EXISTS range_part" + str(i) + ";"
        cur.execute(temp)

    openconnection.commit()
#Remove this once you are done with implementation
def ParaSort(InputTable,SortingColumnName,i,minvalue,maxvalue,openconnection):
    cur = openconnection.cursor()
    tblName = "range_part" + str(i)
    if i == 0:
        temp = "INSERT INTO " + tblName + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " >= " + str(
            minvalue) + " AND " + SortingColumnName + " <= " + str(
            maxvalue) + " ORDER BY " + SortingColumnName + " ASC;"
    else:
        temp = "INSERT INTO " + tblName + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " > " + str(
            minvalue) + " AND " + SortingColumnName + " <= " + str(
            maxvalue) + " ORDER BY " + SortingColumnName + " ASC;"
    cur.execute(temp)
    return

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    cur = openconnection.cursor()
    cur.execute("select min(" + Table1JoinColumn + ") from " + InputTable1 + ";")

    mint1 = float(cur.fetchone()[0])

    cur.execute ("select min(" + Table2JoinColumn + ") from " + InputTable2 + ";")
    mint2 = float(cur.fetchone()[0])

    cur.execute("select max(" + Table1JoinColumn + ") from " + InputTable1 + ";")

    maxt1 = float(cur.fetchone()[0])

    cur.execute("select max(" + Table2JoinColumn + ") from " + InputTable2 + ";")
    maxt2 = float(cur.fetchone()[0])
    noofthreads=5
    maxtbl = max(maxt1, maxt2)
    mintbl = min(mint1, mint2)
    inter = (maxtbl - mintbl) / noofthreads

    cur.execute("select column_name,data_type from information_schema.columns where table_name = \'" + InputTable1 + "\';")
    InputTableTemp1 = cur.fetchall()

    cur.execute("select column_name,data_type from information_schema.columns where table_name = \'" + InputTable2 + "\';")
    InputTableTemp2 = cur.fetchall()

    cur.execute("create table " + OutputTable + " (" + InputTableTemp1[0][0] + " INTEGER);")
    for i in range(1,len(InputTableTemp1)):
        cur.execute("alter table " + OutputTable + " add column " + InputTableTemp1[i][0] + " " + InputTableTemp1[i][1] + ";")


    for i in range(len(InputTableTemp2)):
        cur.execute("alter table " + OutputTable + " add column " + InputTableTemp2[i][0] + " " + InputTableTemp2[i][1] + ";")
    for i in range(noofthreads):
        tName = "inputtable1_" + str(i)
        if i == 0:
            minval = mintbl
            maxval = minval + inter
            temp = "create table " + tName + " as select * from " + InputTable1 + " where " + Table1JoinColumn + " >= " + str(
                minval) + " and " + Table1JoinColumn + " <= " + str(maxval) + ";"
        else:
            minval = maxval
            maxval = minval + inter
            temp = "CREATE TABLE " + tName + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(
                minval) + " AND " + Table1JoinColumn + " <= " + str(maxval) + ";"
        cur.execute(temp)

    for i in range(noofthreads):
        tName = "inputtable2_" + str(i)
        if i == 0:
            minval = mintbl
            maxval = minval + inter
            temp = "create table " + tName + " as select * from " + InputTable2 + " where " + Table2JoinColumn + " >= " + str(
                minval) + " AND " + Table2JoinColumn + " <= " + str(maxval) + ";"
        else:
            minval = maxval
            maxval = minval + inter
            temp = "create table " + tName + " as select * from " + InputTable2 + " where " + Table2JoinColumn + " > " + str(
                minval) + " AND " + Table2JoinColumn + " <= " + str(maxval) + ";"
        cur.execute(temp)

    for i in range(noofthreads):
        OutputTRange = "outtable_range" + str(i)
        temp = "create table " + OutputTRange + " (" + InputTableTemp1[0][0] + " INTEGER);"
        cur.execute(temp)

        for i in range(1, len(InputTableTemp1)):
            temp = "alter table " + OutputTRange + " ADD COLUMN " + InputTableTemp1[i][0] + " " + \
                   InputTableTemp1[i][1] + ";"
            cur.execute(temp)

        for i in range(len(InputTableTemp2)):
            temp = "alter table " + OutputTRange + " add column " + InputTableTemp2[i][0] + " " + \
                   InputTableTemp2[i][1] + ";"
            cur.execute(temp)

    thread = [0, 0, 0, 0, 0]
    for i in range(noofthreads):
        thread[i] = threading.Thread(target=ParaJoin, args=(Table1JoinColumn, Table2JoinColumn, openconnection, i))
        thread[i].start()

    for i in range(noofthreads):
        thread[i].join()

    for i in range(noofthreads):
        cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM outtable_range" + str(i) + ";")


    for i in range(noofthreads):
        cur.execute("DROP TABLE IF EXISTS inputtable1_" + str(i) + ";")
        cur.execute("DROP TABLE IF EXISTS inputtable2_" + str(i) + ";")
        cur.execute("DROP TABLE IF EXISTS outtable_range" + str(i) + ";")

    openconnection.commit()
     # Remove this once you are done with implementation
def ParaJoin(Table1JoinColumn,Table2JoinColumn,openconnection,i):
    cur = openconnection.cursor()
    cur.execute("""insert INTO outtable_range""" + str(i) + """ select * from inputtable1_""" + str(i) + """ inner join inputtable2_""" + str(i) +""" on inputtable1_""" + str(i) + """.""" + str(Table1JoinColumn).lower() + """ = inputtable2_""" + str(i) + """.""" + str(Table2JoinColumn).lower() + """;""")

    return

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
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
    con.commit()
    con.close()

# Donot change this function
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
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


