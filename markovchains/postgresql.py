#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
from ConfigParser import SafeConfigParser
import copy
import random

try:
    import psycopg2
except:
    pass

from util import *


class PostgreSQL(object):

    def __init__(self, dbname):
        if psycopg2:
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            self.inifile = os.path.join(BASE_DIR, 'settings.ini')
            user, password = self._load_ini()
            self.user = user
            self.password = password
            self.con = psycopg2.connect("user=%s password=%s" % (self.user, 
                                                                 self.password))
            self.con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            self.cur = self.con.cursor()
            self.dbname = dbname
            self.num = 3
        else:
            raise BaseException

    def __del__(self):
        self.cur.close()
        self.con.close()

    def _reconnect_db(self):
        self.cur.close()
        self.con.close()
        self.con = psycopg2.connect("user=%s dbname=%s" % (self.user, self.dbname))
        self.con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cur = self.con.cursor()

    def load_db(self):
        self.cur.execute('select datname from pg_database')
        rows = self.cur.fetchall()
        for row in rows:
            if row[0] == self.dbname:
                self._reconnect_db()
                return
        self._create_db()

    def _load_ini(self):
        parser = SafeConfigParser()
        parser.readfp(open(self.inifile))
        user = parser.get('postgresql', 'user')
        password = parser.get('postgresql', 'password')
        return (user, password)


    """
    データベース初期化 & テーブル作成
    """
    def _create_db(self):
        self.cur.execute("create database %s encoding 'utf8'" %(self.dbname))
        self._reconnect_db()
        self._init_tables()

    def _init_tables(self):
        self._init_user()
        self._init_word()
        self._init_chain()
        self._init_userchain()

    def _init_user(self):
        self.cur.execute("""
        CREATE TABLE mc_user (
            id serial PRIMARY KEY,
            name varchar NOT NULL
        )
        """)

    def _init_word(self):
        self.cur.execute('''
        CREATE TABLE word (
            id serial PRIMARY KEY,
            name varchar(100) NOT NULL default ''
        )         
        ''')

    def _init_chain(self):
        sql = []
        sql.append('''
        CREATE TABLE chain (
            id serial PRIMARY KEY,
        ''')
        for i in xrange(self.num):
            sql.append("word%d_id int NOT NULL default '0'," % i)
        sql.append('''
            isstart bool NOT NULL default false,
            count int NOT NULL default '0'
        ''')
        for i in xrange(self.num):
            sql.append(",FOREIGN KEY (word%d_id) REFERENCES word(id)" % i)
        sql.append(")")
        self.cur.execute('\n'.join(sql))

        sql = []
        sql.append('CREATE INDEX chain_idx ON chain (')
        ids = []
        for i in xrange(self.num):
            ids.append('word%d_id' % (i))
        sql.append(','.join(ids))
        sql.append(')')

    def _init_userchain(self):
        self.cur.execute(u'''
        CREATE TABLE userchain (
            id serial PRIMARY KEY,
            user_id int NOT NULL default '0',
            chain_id int NOT NULL default '0',
            count int NOT NULL default '0',
            FOREIGN KEY (user_id) REFERENCES mc_user(id),
            FOREIGN KEY (chain_id) REFERENCES word(id)
        )
        ''')


    """
    データ挿入 & 更新
    """
    def insert_words(self, sql):
        self.cur.execute(u'INSERT INTO word (name) VALUES %s' %\
                (','.join(sql)))

    def insert_chains(self, values):
        sql = []
        sql.append("INSERT INTO chain(")
        sql.append(",".join(["word%d_id" % i for i in xrange(self.num)]))
        sql.append(",isstart,count)")
        sql.append("VALUES %s" % (','.join(values)))
        self.cur.execute('\n'.join(sql))

    def insert_userchains(self, sql):
        self.cur.execute('''
        INSERT INTO userchain(user_id,chain_id,count) VALUES %s
        ''',(','.join(sql)))

    def update_chains(self, ids, count, isstart):
        sql = []
        sql.append('UPDATE chain SET count=%d' % (count))
        sql.append('WHERE')
        for i in xrange(self.num):
            sql.append('word%d_id = %d and ' % (i, ids[i]))
        sql.append('isstart = %s' % (str(isstart).upper()))
        self.cur.execute('\n'.join(sql))

    def update_userchains(self, count, userchainid):
        self.cur.execute('''
            UPDATE userchain
            SET count=%d
            WHERE id = %d
            ''',(count, userchainid))


    """
    データ取得
    """
    def get_nextwords(self, words, userid, num):
        sql = []
        sql.append('select c.word%d_id, w.name, c.count' % (num - 1))
        sql.append('from chain as c')
        sql.append('inner join word as w on c.word%d_id = w.id' % (num - 1))
        sql.append(self._cond_join_userchain(userid))
        sql.append(' where ')
        ids = []
        for i in xrange(num - 1):
            ids.append(' c.word%d_id = %d' % (i, words[i + 1].id))
        sql.append(' and'.join(ids))
        sql.append(self._cond_userid(userid))
        sql.append(' order by count desc')

        self.cur.execute('\n'.join(sql))
        rows = self.cur.fetchall()
        result = []
        for row in rows:
            result.append(Word(int(row[0]), row[1].decode(), int(row[2])))
        return result

    def get_startword(self, num, userid=-1, word=None):
        sql = []
        sql.append('select ')
        for i in xrange(num):
            sql.append('c.word%d_id, w%d.name,' % (i, i))
        sql.append(' c.count')
        sql.append(' from chain as c')
        for i in xrange(num):
            sql.append(' inner join word as w%d on c.word%d_id = w%d.id'\
                    % (i, i, i))
        sql.append(self._cond_join_userchain(userid))
        sql.append(' where ')
        sql.append(' c.isstart = TRUE')
        sql.append(self._cond_userid(userid))
        sql.append(self._cond_wordname(word))

        self.cur.execute('\n'.join(sql))

        rows = self.cur.fetchall()
        row = random.choice(rows)

        result = []
        for i in xrange(0, (num * 2) - 1, 2):
            result.append(Word(int(row[i]), row[i + 1].decode(),
                int(row[(num * 2)])))
        return tuple(result)

    def get_allwords(self):
        self.cur.execute('select name,id from word')
        rows = self.cur.fetchall()
        words = {}
        for row in rows:
            words[row[0].decode()] = int(row[1])
        return words

    def get_allchain(self, num):
        sql = []
        sql.append("select")
        sql.append(','.join(["w%d.name" % i for i in xrange(num)]))
        sql.append(',c.isstart, c.count, c.id')
        sql.append('from chain as c')
        for i in xrange(self.num):
            sql.append('inner join word as w%d on c.word%d_id = w%d.id' %\
                    (i, i, i))
        self.cur.execute('\n'.join(sql))
        rows = self.cur.fetchall()
        words = {}
        for row in rows:
            id = int(row[-1])
            count = int(row[-2])
            isstart = row[-3]
            w1 = row[0].decode()
            w2 = row[1].decode()
            w3 = row[2].decode()
            words[(w1, w2, w3)] = Chain(id, count, isstart)
        return words

    def get_userchain(self, num, userid):
        sql = []
        sql.append("select")
        sql.append(','.join(["w%d.name" % i for i in xrange(self.num)]))
        sql.append(',uc.user_id, c.count, uc.id')
        sql.append('from chain as c')
        sql.append('inner join userchain as uc on uc.chain_id = c.id')
        for i in xrange(self.num):
            sql.append('inner join word as w%d on c.word%d_id = w%d.id' %\
                    (i, i, i))
        sql.append("where uc.user_id = %d" % (userid))
        self.cur.execute('\n'.join(sql))
        rows = self.cur.fetchall()
        words = {}
        for row in rows:
            count = int(row[-2])
            id = int(row[-1])
            words[tuple(row[0: len(row) - 3])] = (count, id)
        return words

    def get_user(self, user):
        self.cur.execute('select id from user where name = %s',(user))
        row = self.cur.fetchone()
        if row is None:
            self.cur.execute("insert into user (name) values (%s)", (user))
            self.cur.execute('select id from user where name = %s', (user))
            row = self.cur.fetchone()
        return int(row[0])

    def get_userid(self, user):
        if user:
            self.cur.execute("select id from user where name = %s", (user))
            row = self.cur.fetchone()
            userid = int(row[0])
        else:
            userid = 0
        return userid

    """
    SQL 条件追加
    """
    def _cond_join_userchain(self, userid):
        if (userid > 0):
            return ' inner join userchain uc on uc.chain_id = c.id'
        else:
            return ''

    def _cond_userid(self, userid):
        if (userid > 0):
            return ' and uc.user_id = %d' % (userid)
        else:
            return ''

    def _cond_wordname(self, word):
        if word:
            return ' and w0.name = "%s"' % (word)
        else:
            return ''

    def make_sentence(self, user='', word=None):
        limit = 1

        userid = self.get_userid(user)
        words = self.get_startword(self.num, userid, word)
        sentenceid = list(copy.copy(words))

        count = 0
        punctuation_words = {u'。': 0, u'．': 0, u'？': 0, u'！': 0,
                             u'!': 0, u'?': 0,  u'w': 0, u'…': 0}
        punctuations = punctuation_words
        while True:
            end_cond = (count > limit) and (words[-1].name in punctuations)
            if end_cond:
                break

            nextwords = self.get_nextwords(words, userid, self.num)
            if len(nextwords) == 0:
                break

            nextword = Util.select_nextword(nextwords)
            sentenceid.append(nextword)
            tmp = [words[i] for i in xrange(1, self.num)]
            tmp.append(nextword)
            words = tuple(tmp)
            count += 1

        return ''.join([x.name for x in sentenceid])

