'''
Query Based Constants
'''

SELECT_ALL = 'SELECT * '
SELECT_SPECIFIC = 'SELECT {}'
CONDITION = 'WHERE {} '
SELECT_MEASUREMENT = ' FROM "{}" '
KEY_COMP = " {} {} '{}' "
RANGE_COMP = " time {} now()-{} "
GROUPBY_TAG = 'GROUP BY {} '
TAG_CAST = '"{}"'
FILL_DEFAULT = 'fill({})'
LIMIT = ' LIMIT {}'