import argparse
import signal
import subprocess
import time

def create_database(database):
    '''Create a new database for testing in.'''

    subprocess.call(['createdb', database])

def drop_database(database):
    '''Drop our testing database.'''

    subprocess.call(['dropdb', database])

def initialize_schema(database):
    '''Initializes our test database.'''

    subprocess.call(['psql', '-f' 'schema_definition.postgres.sql', database])

def load_agenda(database, agenda):
    '''Loads the supplied agenda into our database.

    Requires that our database be initialized.
    '''

    database_load = subprocess.Popen(
        ['psql', database],
        stdin=subprocess.PIPE,
    )

    database_load.communicate("\copy AGENDA FROM '{}' CSV;".format(agenda).encode())

def run_query(database, query):
    '''Runs our supplied query against our test database.'''

    query_results = subprocess.Popen(
        ['psql', '-f', query, database],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )

    output, error = query_results.communicate()
    operations = list(filter(lambda line: 'OPERATION' in line, error.decode().split('\n')))
    print(operations[-1])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Test runner for DBToaster',
    )

    parser.add_argument(
        'query',
        help='The query to run, a .sql file.',
    )

    parser.add_argument(
        'agenda',
        help='The agenda file to run the query on, a .csv file.',
    )

    parser.add_argument(
        'time',
        help='Time (in minutes) to run this query for, before killing it.',
    )

    args = parser.parse_args()

    DATABASE = 'dbtoaster'

    create_database(DATABASE)
    try:
        initialize_schema(DATABASE)
        load_agenda(DATABASE, args.agenda)
        run_query(DATABASE, args.query)
    finally:
        drop_database(DATABASE)
