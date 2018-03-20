#!/bin/env python

import os
import sys
import fdb

def connect_DB(dbpath):
    """Connect to DB through FDB, returns con object
    dbpath is absolute path to fdb file"""
    fdb.connect()


class DBHandler():
    """handle the db file itself, creating everything"""
    
    def __init__(self):
        self.db_address = ""

    def query_blog(self, queryobj):
        """query DB for blog status"""
    
    def create_blank_db_file(self, host, user, password):
        """creates the db at host"""
        dbpath = os.path.dirname(__file__) + "/" + "blank_db.fdb" #FIXME flexible initial db path (potitial permissions issue)
        c = "create database " + host + ":" + dbpath + " " + user + " '" + user + " ' password '" + password + "'"" 
        con = fdb.create_database("create database 'host:/temp/db.db' user 'sysdba' password 'pass'")
    
    def populate_db_with_tables(self)
        """Create our tables and procedures here in the DB"""
        con = connect_DB
        with TransactionContext(con): #auto rollback if exception is raised, and no need to close() because automatic
            cur = con.cursor()
            cur.execute_immediate("create table BLOGS ( stff )")
            cur.execute_immediate("create table OLD_1280 ( stff )")

    def populate_db_with_procedures(self):
        pass


class ConnectedDB(db):
    """Keeps connection to databes, passes requests"""

    def __init__(self, db):
        self.db_host_address = db.db_host_address
        self.db_filepath = db.db_filepath
        self.username = db.username
        self.userpassword = db.userpassword
        self.con = ""
    
    def connect_to(self, object):
        """initialize connection to remote DB"""
        self.con = fdb.connect(database=str(db.db_host_address + db.db_filepath), user=self.username, password=self.userpassword)
        return self.con

    def close_connection(self, con):
        """close con"""
        con.close()


    create_blank_db_file()
    populate_db_with_tables()