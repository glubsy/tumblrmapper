#!/bin/env python
import os
import sys
import fdb

class Database():
    """handle the db file itself, creating everything"""
    
    def __init__(self, db_host, db_filepath, db_user, db_password):
        self.db_host = db_host
        self.db_filepath = db_filepath
        self.username = db_user
        self.userpassword = db_password

    def query_blog(self, queryobj):
        """query DB for blog status"""

    def populate_db_with_procedures(self):
        pass


class Connection(Database):
    """Keeps connection to databases, passes requests"""

    def __init__(self, Database):
        self.con = None #our connection
    
    def connect_to(self, object):
        """initialize connection to remote DB"""
        self.con = fdb.connect(database=str(self.db_host + self.db_filepath), user=self.username, password=self.userpassword)
        return self.con

    def close_connection(self, con):
        """close con"""
        self.con.close()


def create_blank_db_file(username, userpassword, dbpath=None):
    """creates the db at host"""
    if dbpath is None: #default file in script dir (permissions issues)
        dbpath = os.path.dirname(__file__) + "/" + "blank_db.fdb"
    # ("create database 'host:/temp/db.db' user 'sysdba' password 'pass'")
    c = r"create database " + r"'" + dbpath + r"' user '" + username + r"' password '" + userpassword + r"'"
    fdb.create_database(c)

def populate_db_with_tables(username, userpassword, dbpath=None):
    """Create our tables and procedures here in the DB"""
    con = fdb.connect(database=dbpath, user=username, password=userpassword)
    with fdb.TransactionContext(con): #auto rollback if exception is raised, and no need to close() because automatic
        # cur = con.cursor()
        con.execute_immediate("create table BLOGS ( ID varchar(255) NOT NULL PRIMARY KEY )")
        con.execute_immediate("create table OLD_1280 ( FILENAME varchar(255) )")


# test typical usage:
# create_blank_db_file("sysdba", "masterkey", "/home/nupupun/test/test.fdb")
populate_db_with_tables("sysdba", "masterkey", "/home/nupupun/test/test.fdb")
the_db = Database(db_host="localhost", db_filepath="/home/nupupun/test/test.fdb", db_user="test", db_password="test")
