"""
A script to be used with AnotherSqliteConn in test_client.py to simulate
an open connection to a sqlite3 database from a different process.
"""

import sqlite3
import sys

con = sqlite3.connect(sys.argv[1])
cursor = con.cursor()
while True:
    cmd = input()
    sys.stderr.write(cmd + "\n")
    if cmd == 'stop': break
    cursor.execute(cmd)
