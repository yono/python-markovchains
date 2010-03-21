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
        else: # db作成
            print 'db作成'
            self.db.execute('create database %s default character set utf8' %\
                    (dbname))
            self.db.execute('use %s' % (dbname))
            self.init_tables()

        self.mecab = MeCab.Tagger()
        self.words = {}
        self.chains = {}

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
        select word1_id,word2_id,word3_id from chain
        ''')
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

        n = mecab.parseToNode(sentence)

        ## 不要な文字を削る
        words = []
        isstart = True
        while n:
            if n.surface == '':
                n = n.next
                continue
            words.append({'name':u(n.surface),
                          'feature':u(n.feature),
                          'isstart':isstart})
            isstart = False
            n = n.next

        ## ユーザー登録
        userid = False
        if user:
            cur.execute('select id from user where name = "%s"',(user))
            if r.fetchone() is None:
                cur.execute('insert into user (name) values ("%s")',(user))
                r = cur.execute('select id from user where name = "%s"',(user))
            userid = int(r[0]) 
        
        ## 単語登録
        sql = ['("%s")' % (MySQLdb.escape_string(x['name'])) for x in words if x['name'] not in allwords]
        rwords = [x['name'] for x in words if x['name'] not in allwords]
        
        if sql:
            cur.execute(u'INSERT INTO word (name) VALUES %s' % (','.join(sql)))
            for word in rwords:
                cur.execute('select id from word where name = "%s"' %\
                        (MySQLdb.escape_string(word)))
                result = cur.fetchone()
                allwords[word] = result[0]

        ## マルコフ連鎖登録
        w1 = ''
        w2 = ''
        chain = {}
        for word in words:
            #name = word['name']
            #isstart = word['isstart']
            current = word
            if w1 and w2:
                if (w1['name'],w2['name']) not in chain:
                    chain[(w1['name'],w2['name'],w1['isstart'])] = {}
                chain[(w1['name'],w2['name'],w1['isstart'])][current['name']]=\
                        chain[(w1['name'],w2['name'],w1['isstart'])]\
                        .get(current['name'], 0) + 1
            w1,w2 = w2,word
        
        sql = []
        for wlist in chain:
            for word in chain[wlist]:
                id0 = allwords[wlist[0]]
                id1 = allwords[wlist[1]]
                isstart = wlist[2]
                id2 = allwords[word]
                if (id0,id1,id2) not in chains:
                    sql.append("(%d,%d,%d,%d)" % (id0,id1,id2,isstart))
                    self.chains[(id0,id1,id2)] = 0
        
        if sql:
            cur.execute('''
            insert into chain(word1_id,word2_id,word3_id,isstart) values %s
            ''' % (','.join(sql)))

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
            row = cur.execute("select id from user where name = ?",(user))
            row = row.fetchone()
            userid = int(row[0])
            row = cur.execute('''
            select c.word1_id,c.word2_id,c.word3_id 
            from chain c,user_chain uc,word w
            where c.id = uc.chain_id and uc.user_id = %d and w.isstart = TRUE
            and c.word1_id = w.id
            order by rand() limit 1
            ''' % (userid))
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
                row = cur.execute('''
                select c.word3_id,w.isend from chain c,user_chain uc,word w
                where c.word1_id = ? and c.word2_id = ?
                and uc.chain_id = c.id 
                and uc.user_id = ? and c.word3_id = w.id
                order by rand() limit 1
                ''',(wordid[1],wordid[2],userid))
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
