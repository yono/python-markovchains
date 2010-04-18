#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
from optparse import OptionParser, OptionValueError
import copy
from ConfigParser import SafeConfigParser
import random
import MySQLdb
import MeCab

class MarkovChains(object):

    def __init__(self):
        dbname = 'markov'
        user, password = self._load_ini()
        self.con = MySQLdb.connect(user=user,passwd=password,
                charset='utf8',use_unicode=True)
        self.db = self.con.cursor()

        self._load_db(dbname)

        self.mecab = MeCab.Tagger()
        self.words = {}
        self.chains = {}

        self.newwords = {}
        self.newchains = {}
        self.newuserchains = {}
    
    def __del__(self):
        self.db.close()
        self.con.close()
    
    def _load_ini(self):
        parser = SafeConfigParser()
        parser.readfp(open('settings.ini'))
        user = parser.get('mysql','user')
        password = parser.get('mysql','password')
        return (user,password)
    
    def _load_db(self,dbname):
        self.db.execute('show databases')
        rows = self.db.fetchall()
        for row in rows:
            if row[0] == dbname:
                self.db.execute('use %s' % (dbname))
                return 
        self._create_db()
    
    def _create_db(self):
        self.db.execute('create database %s default character set utf8' %\
                (dbname))
        self.db.execute('use %s' % (dbname))
        self._init_tables()

    def _init_tables(self):
        self._init_user()
        self._init_word()    
        self._init_chain()
        self._init_userchain()

    def _init_user(self):
        self.db.execute('''
        CREATE TABLE user (
        id int(11) NOT NULL auto_increment,
        name varchar(100) NOT NULL default '',
        PRIMARY KEY (id)
        ) DEFAULT CHARACTER SET=utf8
        ''')

    def _init_word(self):
        self.db.execute('''
        CREATE TABLE word (
        id int(11) NOT NULL auto_increment,
        name varchar(100) NOT NULL default '',
        PRIMARY KEY (id)
        ) DEFAULT CHARACTER SET=utf8
        ''')

    def _init_chain(self):
        self.db.execute(u'''
        CREATE TABLE chain (
        id int(11) NOT NULL auto_increment,
        word1_id int(11) NOT NULL default '0',
        word2_id int(11) NOT NULL default '0',
        word3_id int(11) NOT NULL default '0',
        isstart BOOL NOT NULL default '0',
        count int(11) NOT NULL default '0',
        PRIMARY KEY (id),
        FOREIGN KEY (word1_id) REFERENCES word(id),
        FOREIGN KEY (word2_id) REFERENCES word(id),
        FOREIGN KEY (word3_id) REFERENCES word(id),
        INDEX word_12 (word1_id,word2_id)
        ) DEFAULT CHARACTER SET=utf8
        ''')

    def _init_userchain(self):
        self.db.execute(u'''
        CREATE TABLE userchain (
        id int(11) NOT NULL auto_increment,
        user_id int(11) NOT NULL default '0',
        chain_id int(11) NOT NULL default '0',
        count int(11) NOT NULL default '0',
        PRIMARY KEY (id),
        FOREIGN KEY (user_id) REFERENCES word(id),
        FOREIGN KEY (chain_id) REFERENCES word(id)
        ) DEFAULT CHARACTER SET=utf8;
        ''')
    
    def get_allwords(self):
        self.db.execute('select name,id from word')
        rows = self.db.fetchall()
        words = dict(rows)
        return words
 
    def get_allchain(self):
        self.db.execute('''
        select w1.name,w2.name,w3.name,c.isstart,c.count
        from word w1,word w2,word w3,chain c
        where w1.id = c.word1_id and w2.id = c.word2_id and w3.id = c.word3_id
        ''')
        rows = self.db.fetchall()
        words = {}
        for row in rows:
            count = int(row[4])
            words[(row[0],row[1],row[2],row[3])] = count
        return words

    def get_userchain(self,userid):
        db = self.db
        db.execute('''
        select w1.name, w2.name, w3.name, c.count, uc.id, uc.user_id
        from word w1,word w2,word w3,chain c,userchain uc
        where uc.user_id = %d and uc.chain_id = c.id
        and w1.id = c.word1_id and w2.id = c.word2_id and w3.id = c.word3_id
        ''' % (userid))
        rows = db.fetchall()
        words = {}
        for row in rows:
            count = int(row[3])
            id = int(row[4])
            words[(row[0],row[1],row[2],row[5])] = (count,id)
        return words
 
    def _get_punctuation(self):
        db = self.db
        punctuations_word = {'。':0, '．':0,'？':0,'！':0}
        punctuations = {}
        for word in punctuations_word:
            db.execute("select id from word where name = '%s'" % (word))
            rows = db.fetchall()
            if rows:
                for row in rows:
                    punctuations[int(row[0])] = 0
        return punctuations

    """
    連想配列に保存された解析結果をDBに保存
    """
    def register_data(self):
        cur = self.db
        sql = ['("%s")' % (MySQLdb.escape_string(x[0])) for x in \
                self.newwords if x[0] not in self.words]
     
        if sql:
            cur.execute(u'INSERT INTO word (name) VALUES %s' % (','.join(sql)))
        allwords = self.get_allwords()

        sql = []
        for chain in self.newchains:
            id0 = allwords[chain[0]]
            id1 = allwords[chain[1]]
            id2 = allwords[chain[2]]
            isstart = chain[3]
            count = self.newchains[chain]
            if chain in self.chains:
                cur.execute('''
                    update chain set count=%d
                    where word1_id = %d and word2_id = %d and word3_id = %d
                    and isstart = %d
                    ''' % (self.newchains[chain],id0,id1,id2,isstart))
            else:
                sql.append("(%d,%d,%d,%d,%d)" % (id0,id1,id2,isstart,count))

            if (len(sql) % 1000) == 0 and len(sql) > 0:
                cur.execute('''
                insert into chain(word1_id,word2_id,word3_id,isstart,count) 
                values %s
                ''' % (','.join(sql)))
                sql = []

        if sql:
            cur.execute('''
            insert into chain(word1_id,word2_id,word3_id,isstart,count) 
            values %s
            ''' % (','.join(sql)))
        
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
            count = self.newuserchains[chain]
            chainid = chains[(id0,id1,id2)]

            if chain in self.userchains:
                cur.execute('''
                    update userchain set count=%d
                    where id = %d
                    ''' % (self.newuserchains[chain],
                        self.userchains[chain][1]))
        
            else: 
                sql.append("(%d,%d,%d)" % (userid,chainid,count))

            if (len(sql) % 1000) == 0 and len(sql) > 0:
                cur.execute('''
                insert into userchain(user_id,chain_id,count) values %s
                ''' % (','.join(sql)))
                sql = []

        if sql:
            cur.execute('''
            insert into userchain(user_id,chain_id,count) values %s
            ''' % (','.join(sql)))

    """
    文章を解析し、連想配列に保存
    """
    def analyze_sentence(self,sentence,user=''):
        mecab = self.mecab
        if len(self.words) == 0:
            self.words = self.get_allwords()
        allwords = self.words
        if len(self.chains) == 0:
            self.chains = self.get_allchain()
        chains = self.chains
        u = unicode

        userid = False
        if user:
            userid = self._get_user(user)
            self.userchains = self.get_userchain(userid)

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
            if w1 and w2:
                key = (w1['name'],w2['name'],w3['name'],w1['isstart'])
                chain[key] = chain.get(key,0) + 1
            w1,w2 = w2,word
        
        rchains = {}
        for wlist in chain:
            self.newchains[wlist] = \
                self.newchains.get(wlist,0) + chain[wlist] +\
                self.chains.get(wlist,0)
            rchains[wlist] = chain[wlist]
        
        # ユーザごとのデータを記録
        if userid:
            sql = []
            select_sql = []
            for row in rchains:
                key = (row[0],row[1],row[2],userid)
                self.newuserchains[key] = \
                    self.newuserchains.get(key,0) + rchains[row] +\
                    self.userchains.get(key,(0,0))[0]

    def make_sentence(self,user=''):
        limit = 20

        userid = self._get_userid(user)
        wordid = self._get_startword(userid)
        sentenceid = copy.copy(wordid)

        ## テーブルを参照して文章(単語idの配列)生成
        count = 0
        punctuations = self._get_punctuation()
        while True:
            rows = self._get_nextword(wordid,userid)
            if len(rows) == 0:
                break
            nextid = self._select_nextid(rows)
            sentenceid.append(nextid)
            wordid = (wordid[1],wordid[2],nextid)
            if count > limit and nextid in punctuations:
                break
            count += 1
        
        ## idを基に実際の文章を生成 
        return self._make_sentence_from_ids(sentenceid)

    def _get_user(self,user)
        self.db.execute('select id from user where name = "%s"' % (user))
        row = self.db.fetchone()
        if row is None:
            self.db.execute("insert into user (name) values ('%s')" % (user)) 
            self.db.execute('select id from user where name = "%s"' % (user))
            row = self.db.fetchone()
        return int(row[0]) 

    def _get_userid(self,user):
        if user:
            self.db.execute("select id from user where name = '%s'" % (user))
            row = self.db.fetchone()
            userid = int(row[0])
        else:
            userid = 0
        return userid

    def _get_startword(self,userid):
        if userid > 0:
            self.db.execute('''
            select c.word1_id,c.word2_id,c.word3_id 
            from chain c,userchain uc
            where c.id = uc.chain_id and uc.user_id = %d and c.isstart = True
            order by rand() limit 1
            ''' % (userid))
            row = self.db.fetchone()
        else:
            self.db.execute('''
            select word1_id,word2_id,word3_id from chain where isstart=True
            order by rand() limit 1
            ''')
            row = self.db.fetchone()
        return map(int, row)
    
    def _get_nextword(self,wordid,userid):
        if userid > 0:
            self.db.execute('''
            select c.word3_id,c.count from chain c,userchain uc
            where c.word1_id = %d and c.word2_id = %d
            and uc.chain_id = c.id 
            and uc.user_id = %d
            order by count desc
            ''' % (wordid[1],wordid[2],userid))
        else:
            self.db.execute('''
            select word3_id,count from chain
            where word1_id = %d and word2_id = %d
            order by count desc
            ''' % (wordid[1],wordid[2]))
        return self.db.fetchall()

    def _select_nextid(self,rows):
        allnum = sum([int(x[1]) for x in rows])
        data = []
        for row in rows:
            data.append((int(row[0]),int(row[1])/float(allnum)))
        randnum = random.random() 
        sum_prob = 0
        for d in data:
            sum_prob += d[1]
            if randnum <= sum_prob:
                nextid = d[0]
        return nextid
    
    def _make_sentence_from_ids(self,wordsid):
        words = []
        for i in xrange(len(wordsid)):
            self.db.execute('select name from word where id = %d'\
                     % (wordsid[i],))
            row = self.db.fetchone()
            words.append(row[0])
        sentence = ''.join(words)
        return sentence

if __name__=='__main__':
    obj = MarkovChains()
