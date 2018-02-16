import influxdb
import constants as C

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
        #if exceptionType is None:
        #    pass
        #else:
        #    pass
        print('Attempting to Disconnect')
        self.disconnect()

    def __del__(self):
        self.disconnect()

    def connect(self):
        '''Establish Connection'''
        self.connection = influxdb.InfluxDBClient(self.host,
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
        self.measurement = C.SELECT_MEASUREMENT.format(measurement)

    def query(self, fields):
        self.fields.append(fields)

    def filter(self, field, comp, value):
        relationship = C.KEY_COMP.format(field, comp, value)
        self.filters.append(relationship)

    def duration(self, value, comp='>'):
        delta = C.RANGE_COMP.format(comp, value)
        self.filters.append(delta)

    def fill(self, value):
        self.filler = str(value)

    def group(self, field):
        self.groupby.append(C.TAG_CAST.format(field))

    def limit(self, value):
        self.limited = value

    def view(self):
        return self._genQuery()

    def execute(self):
        res = self.nigma.connection.query(self._genQuery())
        return res

    def _genQuery(self):
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
