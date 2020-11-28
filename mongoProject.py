import pymongo
import pprint

port = 27017
dbName = 'cs488_588_project'

class ourDb():
    def __init__(self, host):
        self.client = pymongo.MongoClient(host, port)
        self.db = self.client[dbName]
        self.buckets = self.db['aggregated_data']
        self.stations = self.db['metadata']

    '''
    counting low and high speeds
    pass in db['buckets'] from main, creates a pipeline to count the
    speeds less than 5 or greater than 80 and returns as a list
    '''
    def countLowHigh(self):
        pipeline = [
            {'$group': {'_id': None, 'speeds<5': {'$sum': {'$toInt': '$speed<5'}},
            'speeds>80': {'$sum': {'$toInt': '$speed>80'}}}}
        ]
        return list(self.buckets.aggregate(pipeline))

    '''
    finding total volume of foster nb for sep 15, 2011
    pass in db['stations'] and db['buckets'] from main, finds the station
    id of foster NB and creates a pipeline to return as a list.
    '''
    def fosterNBVolume(self):
        query = {'locationtext': 'Foster NB'}
        stationid = self.stations.find_one(filter = query)['stationid']
        pipeline = [
        {'stationid': stationid,
        'startdate': {'$gte': '9/15/11 00:00:00', '$lt': '9/16/11 00:00:00'},
        'enddate': {'$gte': '9/15/11 00:00:00', '$lt': '9/16/11 00:00:00'}}
        ]
        return list(self.buckets.aggregate(pipeline))

    '''
    single day station travel times - find the travel time for foster NB for 5-minute intervals
    for September 15, 2011
    pass in db['stations'] and db['buckets'] from main
    '''
    '''
    def singleDayTravelTimes(self):
        query = {'locationtext': 'Foster NB'}
        table = self.stations.find_one(filter = query)
        stationid, length = table['stationid'], table['length']
        pipeline = [
            {'stationid': stationid,
            'startdate': {'$gte': '9-15-11 00:00:00', '$lt': '9-16-11 00:00:00'},
            'enddate': {'$gte': '9-15-11 00:00:00', '$lt': '9-15-11 00:00:00'}},
            {'$group': {'$multiply': [{'travel time:': {'$divide': [length: {'$divide': ['total_speed', 'total_volume']}}}, 3600}}}
        ]
        return list(self.buckets.aggregate(pipeline))
    '''