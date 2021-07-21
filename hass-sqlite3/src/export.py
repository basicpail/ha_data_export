import sqlite3
from datetime import datetime
import argparse
from pprint import pprint
import csv

DEFAULT_HASS_DB_PATH="/home/heri/data/homeassistant/home-assistant_v2.db"
DEFAULT_BACKUP_PATH="/home/heri/backup/"
DEFAULT_CSV_PATH="/home/heri/csv"

parser = argparse.ArgumentParser(description='Exports data from DB to CSV')
parser.add_argument('--dbpath', action='store', type=str, default=DEFAULT_HASS_DB_PATH)
parser.add_argument('--colname', action='store_true')
parser.add_argument('--startdate', action='store', type=str)
parser.add_argument('--enddate', action='store', type=str)
parser.add_argument('--entitylist', action='store_true')
parser.add_argument('--components', action='store', type=str, nargs='+')

args = parser.parse_args()
dbpath=args.dbpath
colname=args.colname
startdate=args.startdate
enddate=args.enddate
entitylist=args.entitylist
components=args.components

def export(con, start_date, end_date, param_components):

    cur = con.cursor()
    if param_components == None:
        command = f"SELECT * FROM states WHERE last_updated BETWEEN '{start_date}' AND '{end_date}'"
    else:
        comps = [f"'{i}'" for i in param_components]
        comps = ",".join(comps)
        command = f"SELECT * FROM states WHERE last_updated BETWEEN '{start_date}' AND '{end_date}' AND entity_id IN ({comps})"
        
    cur.execute(command)
    data = cur.fetchall()

    export_csv(data, con)
    

def export_csv(data, con):
    now = datetime.now()
    dt_string = now.strftime("%y%m%d_%H:%M:%S")
    columns = get_columns(con)
    columns = [col[1] for col in columns]
    
    with open(f'{DEFAULT_CSV_PATH}/{dt_string}.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for d in data:
            writer.writerow(d)
    
    print("Exported Data to CSV")

def get_entity_list(con):
    cur = con.cursor()
    command = 'select distinct entity_id from states'
    cur.execute(command)
    data = cur.fetchall()

    return data

def get_columns(con):
    cur = con.cursor()
    cur.execute('PRAGMA table_info(states)')
    col = cur.fetchall()

    return col

def main():
    con = sqlite3.connect(dbpath)
    print(f"Connected to DB at {dbpath}")

    if colname:
        col = get_columns(con)
        pprint(col)

    if entitylist:
        entity_list = get_entity_list(con)
        pprint(entity_list)

    if startdate != None and enddate != None:
        export(con, startdate, enddate, components)

    con.close()

if __name__ == '__main__':
    main()
