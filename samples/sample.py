import nigma

if __name__ == '__main__':

    with Nigma(user='user', pwd='password', host='localhost', port='8086', db='cpu') as nigma:
        session = Session(nigma)

        #Measurements
        session.useMeasurement('cpu')

        #Fields
        session.query('speed')
        session.query('temperature')

        #GroupBy
        session.group('machines')

        #Limit Number of Points
        session.limit(3)

        #Add Additional Filter Condition
        #Only Logical AND at the Moment
        session.filter('temperature', '>', '30')

        #Fill NAN Values
        session.fill(0)

        #Display Query Current Session
        print(session.view())

        #Execute Query, Returns ResultSet
        result = session.execute()
        print(result)