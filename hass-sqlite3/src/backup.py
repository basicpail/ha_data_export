import sqlite3
from datetime import datetime
import os
from requests import get, post
import json
import argparse

DEFAULT_HASS_DB_PATH="/home/heri/data/homeassistant/home-assistant_v2.db"
DEFAULT_BACKUP_PATH="/home/heri/backup/"
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiI4NGViZGYwZWE5MjE0MmY4YjhjZjAzMmQ2NTY3ZjdhYiIsImlhdCI6MTYxNTg3NzI0OCwiZXhwIjoxOTMxMjM3MjQ4fQ.WOAAE1GpnzF__BOKe_dfML7pRf4Gq2Z2ZbGuKA2FBe0"

parser = argparse.ArgumentParser(description='Creates a backup for HASS Database and purges the DB')
parser.add_argument('--keepdays', action='store', type=int, default='7')

args = parser.parse_args()
keepdays=args.keepdays

def progress(status, remaining, total):
    print(f'Copied {total-remaining} of {total} pages...')

def create_header():
    headers = {
    "Authorization": f"Bearer {TOKEN}",
    "content-type": "application/json",
    }

    return headers

def create_backup_db(con):
    now = datetime.now()
    dt_string = now.strftime("%y%m%d_%H:%M:%S")
    curr_backup_path =DEFAULT_BACKUP_PATH+"backup_"+dt_string+".db"
    print("Copying to "+ curr_backup_path)
    
    bck = sqlite3.connect(curr_backup_path)
    with bck:
        con.backup(bck, pages=-1, progress=progress)
    bck.close()

def delete_database():
    # Call the hass rest api to purge the database

    endpoint = "https://heriauto.duckdns.org:8123/api/services/recorder/purge"
    headers = create_header()
    data = {"keep_days": f"{keepdays}","repack": "true"}
    print(data)
    json_data = json.dumps(data)
    response = post(endpoint, headers = headers, data = json_data)

    if response.status_code == 200:
        print("Succeeded in purging the database")
    else:
        print("Error! Network Response")

def main():
    con = sqlite3.connect(DEFAULT_HASS_DB_PATH)
    create_backup_db(con)
    con.close()
    print("Deleting previous database...")
    response = delete_database()

if __name__ == '__main__':
    main()
