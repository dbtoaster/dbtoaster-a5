import argparse
import subprocess
import time
import re

progress = re.compile(r'NOTICE:\s+(OPERATION.*)$')

def create_database(database):
    '''Create a new database for testing in.'''

    subprocess.call(
        ['createdb', database],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

def drop_database(database):
    '''Drop our testing database.'''

    subprocess.call(
        ['dropdb', database],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

def initialize_schema(database):
    '''Initializes our test database.'''

    subprocess.call(
        ['psql', '-f' 'schema_definition.postgres.sql', database],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

def load_agenda(database, agenda):
    '''Loads the supplied agenda into our database.

    Requires that our database be initialized.
    '''

    database_load = subprocess.Popen(
        ['psql', database],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    database_load.communicate("\copy AGENDA FROM '{}' CSV;".format(agenda).encode())

def run_query(database, query, timeout):
    '''Runs our supplied query against our test database.'''

    query_results = subprocess.Popen(
        ['psql', '-f', query, database],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )

    time.sleep(timeout)
    query_results.kill()
    error = query_results.stderr.read()
    terminate_query = subprocess.call(
        ['psql', '-c', "select pg_cancel_backend(procpid) from pg_stat_activity where current_query LIKE 'SELECT dispatch();'", DATABASE],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )

    operations = list(filter(lambda line: 'OPERATION' in line, error.decode().split('\n')))
    print(progress.search(operations[-1]).group(1))


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
        type=int,
    )

    parser.add_argument(
        '-d', '--debug',
        help='Print debugging information from the database.',
        action='store_true',
        dest='debug',
    )

    args = parser.parse_args()

    DATABASE = 'dbtoaster'

    initialize_schema(DATABASE)
    load_agenda(DATABASE, args.agenda)
    run_query(DATABASE, args.query, args.time)
