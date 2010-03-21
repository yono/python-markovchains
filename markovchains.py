#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
from optparse import OptionParser, OptionValueError
import copy
import MySQLdb
import MeCab
from ConfigParser import SafeConfigParser

class MarkovChains(object):

    def __init__(self):
        dbname = 'markov'
        parser = SafeConfigParser()
        parser.readfp(open('settings.ini'))
        user = parser.get('mysql','user')
        password = parser.get('mysql','password')
        self.con = MySQLdb.connect(user=user,passwd=password,
                charset='utf8',use_unicode=True)
        self.db = self.con.cursor()
        self.db.execute('show databases')
        rows = self.db.fetchall()
        is_exist = False
        for row in rows:
            if row[0] == dbname:
                is_exist = True

        if is_exist:
            self.db.execute('use %s' % (dbname))
        else:
            print 'create db'
            self.db.execute('create database %s default character set utf8' %\
                    (dbname))
            self.db.execute('use %s' % (dbname))
            self.init_tables()

        self.mecab = MeCab.Tagger()
        self.words = {}
        self.chains = {}

        self.newwords = {}
        self.newchains = {}
        self.newuserchains = {}

    def init_tables(self):
        self.init_user()
        self.init_word()    
        self.init_chain()
        self.init_userchain()

    def init_user(self):
        db = self.db
        db.execute('''
        CREATE TABLE user (
        id int(11) NOT NULL auto_increment,
        name varchar(100) NOT NULL default '',
        PRIMARY KEY (id)
        ) DEFAULT CHARACTER SET=utf8
        ''')

    def init_word(self):
        db = self.db
        db.execute('''
        CREATE TABLE word (
        id int(11) NOT NULL auto_increment,
        name varchar(100) NOT NULL default '',
        PRIMARY KEY (id)
        ) DEFAULT CHARACTER SET=utf8
        ''')

    def init_chain(self):
        db = self.db
        db.execute(u'''
        CREATE TABLE chain (
        id int(11) NOT NULL auto_increment,
        word1_id int(11) NOT NULL default '0',
        word2_id int(11) NOT NULL default '0',
        word3_id int(11) NOT NULL default '0',
        isstart BOOL NOT NULL default '0',
        PRIMARY KEY (id),
        FOREIGN KEY (word1_id) REFERENCES word(id),
        FOREIGN KEY (word2_id) REFERENCES word(id),
        FOREIGN KEY (word3_id) REFERENCES word(id),
        INDEX word_12 (word1_id,word2_id)
        ) DEFAULT CHARACTER SET=utf8
        ''')

    def init_userchain(self):
        db = self.db
        db.execute(u'''
        CREATE TABLE userchain (
        id int(11) NOT NULL auto_increment,
        user_id int NOT NULL default '0',
        chain_id int NOT NULL default '0',
        PRIMARY KEY (id),
        FOREIGN KEY (user_id) REFERENCES word(id),
        FOREIGN KEY (chain_id) REFERENCES word(id)
        ) DEFAULT CHARACTER SET=utf8;
        ''')
    
    def get_allwords(self):
        db = self.db
        db.execute('select name,id from word')
        rows = db.fetchall()
        words = dict(rows)
        return words
 
    def get_allchain(self):
        db = self.db
        db.execute('''
        select w1.name,w2.name,w3.name,c.isstart 
        from word w1,word w2,word w3,chain c
        where w1.id = c.word1_id and w2.id = c.word2_id and w3.id = c.word3_id
        ''')
        rows = db.fetchall()
        words = dict(zip(rows,range(len(rows))))
        return words

    def get_userchain(self,userid):
        db = self.db
        db.execute('''
        select w1.name,w2.name,w3.name,c.isstart 
        from word w1,word w2,word w3,chain c,userchain uc
        where uc.user_id = %d and uc.chain_id = c.id
        and w1.id = c.word1_id and w2.id = c.word2_id and w3.id = c.word3_id
        ''' % (userid))
        rows = db.fetchall()
        words = dict(zip(rows,range(len(rows))))
        return words
 
    def get_kutouten(self):
        db = self.db
        kutouten_word = {'。':0, '．':0,'？':0,'！':0}
        kutouten = {}
        for word in kutouten_word:
            db.execute("select id from word where name = '%s'" % (word))
            rows = db.fetchall()
            if rows:
                for row in rows:
                    kutouten[int(row[0])] = 0
        return kutouten

    def regist_newdata(self):
        cur = self.db
        print 'register words'
        sql = ['("%s")' % (MySQLdb.escape_string(x[0])) for x in \
                self.newwords if x[0] not in self.words]
     
        if sql:
            cur.execute(u'INSERT INTO word (name) VALUES %s' % (','.join(sql)))
        allwords = self.get_allwords()

        print 'register chain'
        sql = []
        for chain in self.newchains:
            id0 = allwords[chain[0]]
            id1 = allwords[chain[1]]
            id2 = allwords[chain[2]]
            isstart = chain[3]
            sql.append("(%d,%d,%d,%d)" % (id0,id1,id2,isstart))

            if (len(sql) % 1000) == 0:
                cur.execute('''
                insert into chain(word1_id,word2_id,word3_id,isstart) values %s
                ''' % (','.join(sql)))
                sql = []

        if sql:
            cur.execute('''
            insert into chain(word1_id,word2_id,word3_id,isstart) values %s
            ''' % (','.join(sql)))
        
        print 'register userchain'
        sql = []
        cur.execute('select id,word1_id,word2_id,word3_id from chain')
        rows = cur.fetchall()
        chains = {}
        for row in rows:
            chains[(int(row[1]),int(row[2]),int(row[3]))] = int(row[0])
        for chain in self.newuserchains:
            id0 = allwords[chain[0]]
            id1 = allwords[chain[1]]
            id2 = allwords[chain[2]]
            userid = chain[3]
            chainid = chains[(id0,id1,id2)]
            sql.append("(%d,%d)" % (userid,chainid))

            if (len(sql) % 1000) == 0:
                cur.execute('''
                insert into userchain(user_id,chain_id) values %s
                ''' % (','.join(sql)))
                sql = []

        if sql:
            cur.execute('''
            insert into userchain(user_id,chain_id) values %s
            ''' % (','.join(sql)))

    def regist_sentence(self,sentence,user=''):
        cur = self.db
        mecab = self.mecab
        if len(self.words) == 0:
            self.words = self.get_allwords()
        allwords = self.words
        if len(self.chains) == 0:
            self.chains = self.get_allchain()
        chains = self.chains
        u = unicode

        ## ユーザー登録
        userid = False
        if user:
            cur.execute('select id from user where name = "%s"' % (user))
            row = cur.fetchone()
            if row is None:
                cur.execute("insert into user (name) values ('%s')" % (user)) 
                cur.execute('select id from user where name = "%s"' % (user))
                row = cur.fetchone()
            userid = int(row[0]) 
            userchains = self.get_userchain(userid)
            cur.execute('select id from chain order by id desc limit 1')
            row = cur.fetchone()
            if row:
                lastid = int(row[0])
            else:
                lastid = 0

        n = mecab.parseToNode(sentence)

        ## 不要な文字を削る
        words = []
        isstart = True
        while n:
            if n.surface == '':
                n = n.next
                continue
            words.append({'name':u(n.surface),
                          'isstart':isstart})
            self.newwords[(u(n.surface),isstart)] = 0
            isstart = False
            n = n.next

        
        # マルコフ連鎖登録
        w1 = ''
        w2 = ''
        w3 = ''
        chain = {}
        for word in words:
            if w1 and w2 and w2:
                word1 = w1['name']
                word2 = w2['name']
                word3 = w3['name']
                isstart = w1['isstart']
                chain[(word1,word2,word3,isstart)]= 0
            w1,w2,w3 = w2,w3,word
        
        rchains = {}
        for wlist in chain:
            if wlist not in self.chains:
                w1,w2,w3,isstart = wlist
                self.newchains[(w1,w2,w3,isstart)] = 0
                rchains[(w1,w2,w3,isstart)] = 0
        
        # ユーザごとのデータを記録
        if user:
            sql = []
            select_sql = []
            for row in rchains:
                if row not in userchains:
                    self.newuserchains[(row[0],row[1],row[2],userid)] = 0

    def make_sentence(self,user=''):
        limit = 20
        cur = self.db

        ## 文頭の言葉を取得
        if user == '':
            cur.execute('''
            select word1_id,word2_id,word3_id from chain where isstart=True
            order by rand() limit 1
            ''')
            row = cur.fetchone()
        else:
            cur.execute("select id from user where name = '%s'" % (user))
            row = cur.fetchone()
            userid = int(row[0])
            cur.execute('''
            select c.word1_id,c.word2_id,c.word3_id 
            from chain c,userchain uc
            where c.id = uc.chain_id and uc.user_id = %d and c.isstart = True
            order by rand() limit 1
            ''' % (userid))
            row = cur.fetchone()
        wordid = map(int, row)

        sentenceid = copy.copy(wordid)

        ## テーブルを参照して文章(単語idの配列)生成
        count = 0
        ## 句読点
        #kutouten = {99:0,3:0,33:0,53:0,64:0}
        kutouten = self.get_kutouten()
        while True:
            if user == '':
                cur.execute('''
                select word3_id from chain
                where word1_id = %d and word2_id = %d
                order by rand() limit 1
                ''' % (wordid[1],wordid[2]))
                row = cur.fetchone()
            else:
                cur.execute('''
                select c.word3_id from chain c,userchain uc
                where c.word1_id = %d and c.word2_id = %d
                and uc.chain_id = c.id 
                and uc.user_id = %d
                order by rand() limit 1
                ''' % (wordid[1],wordid[2],userid))
                row = cur.fetchone()
            if row is None:
                break
            nextid = int(row[0])
            sentenceid.append(nextid)
            wordid = [wordid[1],wordid[2],nextid]
            if count > limit and nextid in kutouten:
                break
            count += 1
        
        ## idを基に実際の文章を生成 
        sentence = ''
        for i in xrange(len(sentenceid)):
            cur.execute('select name from word where id = %d'\
                     % (sentenceid[i],))
            row = cur.fetchone()
            sentence = '%s%s' % (sentence,row[0])

        return sentence

if __name__=='__main__':
    obj = MarkovChains()
