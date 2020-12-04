import pymongo
import pprint
from datetime import datetime

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
            {'$group': {'_id': None, 'speeds<5': {'$sum': '$speed<5'},
            'speeds>80': {'$sum': '$speed>80'}}}
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
        startDate = datetime(2011, 9, 15, 0, 0, 0)
        endDate = datetime(2011, 9, 16, 0, 0, 0)
        pipeline = [
        {'$match': {'stationid': stationid}},
        {'$match': {'startdate': {'$gte': startDate, '$lt': endDate}}},
        {'$match': {'enddate': {'$gte': startDate, '$lt': endDate}}},
        {'$group': {'_id': None, 'volume': {'$sum': '$total_volume'}}}
        ]
        return list(self.buckets.aggregate(pipeline))

    '''
    single day station travel times - find the travel time for foster NB for 5-minute intervals
    for September 15, 2011
    pass in db['stations'] and db['buckets'] from main
    '''
    def singleDayTravelTimes(self):
        query = {'locationtext': 'Foster NB'}
        table = self.stations.find_one(filter = query)
        stationid, length = table['stationid'], table['length']
        startDate = datetime(2011, 9, 15, 0, 0, 0)
        endDate = datetime(2011, 9, 16, 0, 0, 0)
        pipeline = [
        {'$match': {'stationid': stationid}},
        {'$match': {'startdate': {'$gte': startDate, '$lt': endDate}}},
        {'$match': {'enddate': {'$gte': startDate, '$lt': endDate}}},
        {'$project': {'startdate': '$startdate', 'travelTime': {'$multiply': [{'$divide': [length, {'$divide': ['$total_speed', '$total_volume']}]}, 3600]}}},
        {'$sort': {'startdate': 1}}
        ]
        return list(self.buckets.aggregate(pipeline))

    def peakTravelTimes(self):
         table = self.stations.find({'direction': 'NORTH'},{'stationid': 1})
         stations = list(table)
         ids = []
         for i in range(len(stations)):
             ids.append(stations[i]['stationid'])
         pipeline = [
         {'$match': {'stationid': {'$in': ids}}},
         {'$group': {'_id': None, 'totalLength': {'$sum': '$length'}}}
         ]
         totalLength = self.stations.aggregate(pipeline)
         totalLength = list(totalLength)[0]['totalLength']
         startDateAM = datetime(2011, 9, 22, 7, 0, 0)
         endDateAM = datetime(2011, 9, 22, 9, 0, 0)
         startDatePM = datetime(2011, 9, 22, 16, 0, 0)
         endDatePM = datetime(2011, 9, 22, 18, 0, 0)
         pipeline = [
         {'$match': {'stationid': {'$in': ids}}},
         {'$match': {'startdate': {'$gte': startDateAM, '$lt': endDateAM}}},
         {'$match': {'enddate': {'$gte': startDateAM, '$lt': endDateAM}}},
         {'$group': {'_id': None, 'totalVolume': {'$sum': '$total_volume'}}}
         ]
         totalVolumeAM = list(self.buckets.aggregate(pipeline))[0]['totalVolume']
         pipeline = [
         {'$match': {'stationid': {'$in': ids}}},
         {'$match': {'startdate': {'$gte': startDateAM, '$lt': endDateAM}}},
         {'$match': {'enddate': {'$gte': startDateAM, '$lt': endDateAM}}},
         {'$group': {'_id': None, 'totalSpeed': {'$sum': '$total_speed'}}}
         ]
         totalSpeedAM = list(self.buckets.aggregate(pipeline))[0]['totalSpeed']
         pipeline = [
         {'$match': {'stationid': {'$in': ids}}},
         {'$match': {'startdate': {'$gte': startDatePM, '$lt': endDatePM}}},
         {'$match': {'enddate': {'$gte': startDatePM, '$lt': endDatePM}}},
         {'$group': {'_id': None, 'totalVolume': {'$sum': '$total_volume'}}}
         ]
         totalVolumePM = list(self.buckets.aggregate(pipeline))[0]['totalVolume']
         pipeline = [
         {'$match': {'stationid': {'$in': ids}}},
         {'$match': {'startdate': {'$gte': startDatePM, '$lt': endDatePM}}},
         {'$match': {'enddate': {'$gte': startDatePM, '$lt': endDatePM}}},
         {'$group': {'_id': None, 'totalSpeed': {'$sum': '$total_speed'}}}
         ]
         totalSpeedPM = list(self.buckets.aggregate(pipeline))[0]['totalSpeed']
         return [{'7AM-9AM':((totalLength/(totalSpeedAM/totalVolumeAM)) * 60)},{'4PM-6PM':((totalLength/(totalSpeedPM/totalVolumePM)) * 60)}]
         '''
         pipeline = [
         {'$match': {'stationid': {'$in': ids}}},
         {'$match': {'$or': [{'startdate': {'$gte': startDateAM, '$lt': endDateAM}}, {'startdate': {'$gte': startDatePM, '$lt': endDatePM}}]}},
         {'$match': {'$or': [{'enddate': {'$gte': startDateAM, '$lt': endDateAM}}, {'enddate': {'$gte': startDatePM, '$lt': endDatePM}}]}},
         {'$group': {'_id': None, 'travelTime': {'$sum': {'$multiply': [{'$cond': [ {'$eq': [{'$cond': [ {'$eq': [{'$sum': '$total_volume'}, 0]}, 
         0, {'$divide': [{'$sum': '$total_speed'}, {'$sum': '$total_volume'}]}]}, 0]},0,{'$divide': [totalLength, {'$divide': [{'$sum': '$total_speed'}, {'$sum': '$total_volume'}]}]}]},60]}}}}
         ]
         '''
         return list(self.buckets.aggregate(pipeline))

    def update(self, num):
        query = {'locationtext': 'Foster NB'}
        new_values = {'$set' : {'milepost': float(num)}}
        self.stations.update_one(query, new_values)
        return self.stations.find_one(filter = query)