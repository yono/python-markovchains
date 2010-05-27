#!/usr/bin/env python
# -*- coding:utf-8 -*-
import mysql
import postgresql

class Database(object):

    @classmethod
    def create(cls, db, dbname):
        if db == 'mysql':
            return mysql.MySQL(dbname)
        elif db == 'postgresql':
            return postgresql.PostgreSQL(dbname)

