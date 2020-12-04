import pymongo
import mongoProject
import sys
import pprint
from os import system, name

def clear():
    if name == 'nt':#Windows
        _ = system('cls')
    else:#mac and linux use same system call
        _ = system('clear')

#make sure on set up your database name is cs488_588_project, and your collection names are aggregated_data and metadata
#change host to the external ip of your database

host = '35.233.250.109'
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
          + 'intervals for Sept 15, 2011. Report travel time in seconds.\n'
          + '4. Get the the travel time on the entire 205 NB during peak\n'
          + 'times (7-9AM, 4-6PM).\n'
          + '5. Find a route from Johnson Creek to Columbia Blvd on I-205\n'
          + 'using upstream and downstream fields.\n'
          + '6. Update the milepost of Foster NB to any number you input.\n'
          + '0. Quit')
    i = input('Select: ')
    if i == '0':
        print('goodbye')
        exit(0)
    elif i == '1':
        pprint.pprint(data.countLowHigh())
    elif i == '2':
        pprint.pprint(data.fosterNBVolume())
    elif i == '3':
        travelTimes = data.singleDayTravelTimes()
        for i in range(10):
            pprint.pprint(travelTimes[i])
    elif i == '4':
        pprint.pprint(data.peakTravelTimes())
    elif i == '5':
        pprint.pprint('Not implemented yet')
    elif i == '6':
        num = input('Enter a number to update Foster NB milepost to: ')
        pprint.pprint(data.update(num))
    else:
        clear()
        print('invalid option try again')
    if i > 0 and i <= 6:
        clear()