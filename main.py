import pymongo
import mongoProject
import sys
import pprint

#make sure on set up your database name is cs488_588_project, and your collection names are aggregated_data and metadata
#change host to the external ip of your database

host = 'externalIp'
'''
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
'''
data = mongoProject.ourDb(host)

while True:
    print('Select query option(enter the number):')
    print('1. Count low speeds and high speeds. Find the number of speeds\n'
          + 'less than 5 mph and greater than 80 mph in the data set\n'
          + '2. Find teh totla volume for the station Foster NB for\n'
          + 'September 15, 2011\n'
          + '3. Find the travel time for station Foster NB for 5-minute\n'
          + 'intervals for Sept 15, 2011. Report travel time in seconds.')
    i = input('Select: ')
    if i == '0':
        print('goodbye')
        exit(0)
    elif i == '1':
        pprint.pprint(data.countLowHigh())
    else:
        print('invalid option try again')
    exit(0)