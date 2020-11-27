import pymongo
import mongoProject
import sys

host = ''
dbName = ''
if len(sys.argv) < 3:
    print('insufficient arguments - start with \'python3 main.py [hostname][dbName]\'')
    prompt = input('manually enter host and database name? (y/n): ')
    if prompt == 'y':
        host = input('host ip: ')
        dbName = input('dbName: ')
    else:
        exit(0)
else:
    host = sys.argv[1]
    dbName = sys.argv[2]

data = mongoProject.ourDb(host, dbName)