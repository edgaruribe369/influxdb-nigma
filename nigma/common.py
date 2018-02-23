from influxdb import InfluxDBClient
import nigma.constants as C

class Nigma(object):
    '''Canned Queries Base Class'''

    def __init__(self, user='', pwd='', host='', port='', db=''):
        '''Prepares data source name (DSN) for connecting to database

        :host:  hostname
        :port:  tcp port
        :user:  username
        :pwd:   password
        :db:    database to attach
        '''

        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.db = db

        self.connection = None

    def __enter__(self):
        print('Establishing connection')
        return self.connect()

    def __exit__(self, exceptionType, exceptionValue, traceBack):
        print('Attempting to Disconnect')
        self.disconnect()

    def __del__(self):
        self.disconnect()

    def connect(self):
        '''Establish Connection'''
        self.connection = InfluxDBClient(self.host,
                                         self.port,
                                         self.user,
                                         self.pwd,
                                         self.db)
        return self

    def disconnect(self):
        '''Close Database Connection'''
        connection = self.connection
        if connection is not None:
            self.connection.close()
        self.connection = None


class Session(object):
    '''Session for Query Generation'''

    def __init__(self, nigma):
        self.nigma = nigma
        self.fields = []
        self.filters = []
        self.groupby = []
        self.limited = None
        self.filler = None
        self.measurement = None

    def useMeasurement(self, measurement):
        '''
        Set Query Measurement

        :measurement: String name of influxdb measurement to use.
        '''
        self.measurement = C.SELECT_MEASUREMENT.format(measurement)

    def query(self, fields):
        '''
        Set Query Fields

        :fields: List or String of InfluxDB fields to query.
        '''
        if type(fields) == list:
            self.fields.extend(fields)
        else:
            self.fields.append(fields)

    def filter(self, field, comp, value):
        '''
        Set Query Filters

        :field: String feld to compare
        :comp: String comparison symbol (i.e. =, <, >)
        :value: Int/Float value to compare above field
        '''
        relationship = C.KEY_COMP.format(field, comp, value)
        self.filters.append(relationship)

    def duration(self, time):
        '''
        Set Query Duration

        :time: String time for datapoints withing duration
        (Example: 5m, 60m, 5hr)
        '''
        delta = C.RANGE_COMP.format(time)
        self.filters.append(delta)

    def fill(self, value):
        '''
        Set Query Fill for Aggregation Based Queries

        :value: Int/Float/None Types
        '''
        self.filler = str(value)

    def group(self, tags):
        '''
        Set Query GroupBy

        :field: List or String of Tags to GroupBy
        '''
        if isinstance(tags, list):
            for item in tags:
                self.groupby.append(C.TAG_CAST.format(item))
        else:
            self.groupby.append(C.TAG_CAST.format(tags))

    def limit(self, value):
        '''
        Set Query Limit

        :value: Int for number of datapoints to return
        '''
        self.limited = value

    def view(self):
        '''
        View Query
        '''
        return self._genQuery()

    def execute(self, epoch = 'ms'):
        '''
        Execute Query
        '''
        res = self.nigma.connection.query(self._genQuery(),
                                          epoch=epoch)
        return res

    def _genQuery(self):
        '''
        Generate String Query To Execute
        '''
        query = ''
        if not self.fields:            
            query += C.SELECT_ALL
        else:
            query += C.SELECT_SPECIFIC.format(', '.join(self.fields))
        query += self.measurement
        if self.filters:
            query += C.CONDITION.format(' and '.join(self.filters))
        if self.groupby:
            query += C.GROUPBY_TAG.format(', '.join(self.groupby))
            if self.filler:
                query += C.FILL_DEFAULT.format(self.filler)
        if self.limited:
            query += C.LIMIT.format(self.limited)

        return query
