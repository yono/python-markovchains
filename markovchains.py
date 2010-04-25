#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
from optparse import OptionParser, OptionValueError
import copy
from ConfigParser import SafeConfigParser
import random
import MySQLdb
import MeCab
from extractword import Sentence

class Word(object):

    def __init__(self,id,name,count):
        self.id = id
        self.name = name
        self.count = count

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
        self._create_db(dbname)
    
    def _create_db(self,dbname):
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
        select 
            w1.name,
            w2.name,
            w3.name,
            c.isstart,
            c.count
        from chain c
            inner join word w1
            on c.word1_id = w1.id

            inner join word w2
            on c.word2_id = w2.id

            inner join word w3
            on c.word3_id = w3.id
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
        select 
            w1.name,
            w2.name,
            w3.name,
            c.count,
            uc.id,
            uc.user_id
        from userchain uc
            inner join chain c
            on uc.chain_id = c.id

            inner join word w1
            on c.word1_id = w1.id

            inner join word w2
            on c.word2_id = w2.id

            inner join word w3
            on c.word3_id = w3.id
        where
            uc.user_id = %d
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
        punctuations_word = {u'。':0, u'．':0,u'？':0,u'！':0}
        punctuations = {}
        for word in punctuations_word:
            db.execute("select id from word where name = '%s'" % (word))
            rows = db.fetchall()
            if rows:
                for row in rows:
                    punctuations[int(row[0])] = 0
        return punctuations

    def _insert_words(self,sql):
        self.db.execute(u'INSERT INTO word (name) VALUES %s' % (','.join(sql)))

    def _insert_chains(self,sql):
        self.db.execute('''
        insert into chain(word1_id,word2_id,word3_id,isstart,count) 
        values %s
        ''' % (','.join(sql)))

    def _update_chains(self,count,id0,id1,id2,isstart):
        self.db.execute('''
            UPDATE 
                chain 
            SET 
                count=%d
            WHERE 
                word1_id = %d and 
                word2_id = %d and 
                word3_id = %d and 
                isstart  = %d
        ''' % (count,id0,id1,id2,isstart))

    def _insert_userchains(self,sql):
        self.db.execute('''
        insert into userchain(user_id,chain_id,count) values %s
        ''' % (','.join(sql)))

    def _update_userchains(self,count,userchainid):
        self.db.execute('''
            update 
                userchain 
            set 
                count=%d
            where 
                id = %d
            ''' % (count,userchainid))

    def register_words(self):
        sql = ['("%s")' % (MySQLdb.escape_string(x)) for x in \
                self.newwords if x not in self.words]
        if sql:
            self._insert_words(sql)

    def register_chains(self):
        insert_range = 1000
        allwords = self.get_allwords()
        sql = []
        for chain in self.newchains:
            id0 = allwords[chain[0]]
            id1 = allwords[chain[1]]
            id2 = allwords[chain[2]]
            isstart = chain[3]
            count = self.newchains[chain]
            if chain in self.chains:
                self._update_chains(count,id0,id1,id2,isstart)
            else:
                sql.append("(%d,%d,%d,%d,%d)" % (id0,id1,id2,isstart,count))

            if (len(sql) % insert_range) == 0 and len(sql) > 0:
                self._insert_chains(sql)
                sql = []

        if sql:
            self._insert_chains(sql)

    def register_userchains(self):
        insert_range = 1000
        allwords = self.get_allwords()
        sql = []
        self.db.execute('select id,word1_id,word2_id,word3_id from chain')
        rows = self.db.fetchall()
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
                self._update_userchains(count, self.userchains[chain][1])
        
            else: 
                sql.append("(%d,%d,%d)" % (userid,chainid,count))

            if (len(sql) % insert_range) == 0 and len(sql) > 0:
                self._insert_userchains(sql)
                sql = []
        if sql:
            self._insert_userchains(sql)

    """
    連想配列に保存された解析結果をDBに保存
    """
    def register_data(self):
        self.register_words()
        self.register_chains()
        self.register_userchains()

    """
    文章を解析し、連想配列に保存
    """
    def analyze_sentence(self,text,user=''):
        if len(self.words) == 0:
            self.words = self.get_allwords()
        if len(self.chains) == 0:
            self.chains = self.get_allchain()

        words = self._get_words(text)
        self._update_newwords_dic(words)
        chain = self._update_newchains_dic(words)
        
        if user:
            userid = self._get_user(user)
            self.userchains = self.get_userchain(userid)
            self._update_newuserchains_dic(chain,userid)

    def _update_newwords_dic(self,words):
        for w in words:
            if w['name'] not in self.words:
                self.newwords[w['name']] = self.newwords.get(w['name'], 0) + 1

    def _update_newchains_dic(self,words):
        w1 = ''
        w2 = ''
        w3 = ''
        chain = {}
        for word in words:
            if w1 and w2 and w3:
                key = (w1['name'],w2['name'],w3['name'],w1['isstart'])
                chain[key] = chain.get(key,0) + 1
            w1,w2,w3 = w2,w3,word
        
        rchains = {}
        for wlist in chain:
            self.newchains[wlist] = \
                self.newchains.get(wlist,0) + chain[wlist] +\
                self.chains.get(wlist,0)
            rchains[wlist] = chain[wlist]
        return rchains

    def _update_newuserchains_dic(self,chains,userid):
        for row in chains:
            key = (row[0],row[1],row[2],userid)
            self.newuserchains[key] = \
                self.newuserchains.get(key,0) + chains[row] +\
                self.userchains.get(key,(0,0))[0]

    def _get_words(self,text):
        sentence = Sentence()
        sentence.analysis_text(text)
        words = sentence.get_words()
        result = []
        first = True
        for word in words:
            result.append({'name':word, 'isstart':first})
            first = False
        return result

    def make_sentence(self,user=''):
        limit = 20

        userid = self._get_userid(user)
        words = self._get_startword(userid)
        sentenceid = list(copy.copy(words))

        ## テーブルを参照して文章(単語idの配列)生成
        count = 0
        punctuations = self._get_punctuation()
        while True:
            nextwords = self._get_nextwords(words,userid)
            if len(nextwords) == 0:
                break
            nextword = self._select_nextword(nextwords)
            sentenceid.append(nextword)
            words = (words[1],words[2],nextword)
            if count > limit or nextword.name in punctuations:
                break
            count += 1
        
        ## idを基に実際の文章を生成 
        return ''.join([x.name for x in sentenceid])

    def _get_user(self,user):
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

    def _get_startword(self,userid=-1):
        sql_list = []
        sql_list.append('select c.word1_id,w1.name,c.word2_id,w2.name,')
        sql_list.append(' c.word3_id,w3.name,c.count')
        sql_list.append(' from chain c')
        sql_list.append(' inner join word w1 on c.word1_id = w1.id')
        sql_list.append(' inner join word w2 on c.word2_id = w2.id')
        sql_list.append(' inner join word w3 on c.word3_id = w3.id')
        sql_list.append(self._cond_join_userchain(userid))
        sql_list.append(' where ')
        sql_list.append(' c.isstart = True')
        sql_list.append(self._cond_userid(userid))
        sql_list.append(' order by rand()')
        sql_list.append(' limit 1')

        self.db.execute(''.join(sql_list))

        row = self.db.fetchone()

        result = []
        for i in xrange(0,5,2):
            result.append(Word(int(row[i]),row[i+1],int(row[6])))
        return tuple(result)
    
    def _get_nextwords(self,words,userid):
        sql_list = []
        sql_list.append('select c.word3_id, w.name, c.count')
        sql_list.append(' from chain c')
        sql_list.append(' inner join word w on c.word3_id = w.id')
        sql_list.append(self._cond_join_userchain(userid))
        sql_list.append(' where ')
        sql_list.append(' c.word1_id = %d and' % (words[1].id))
        sql_list.append(' c.word2_id = %d' % (words[2].id))
        sql_list.append(self._cond_userid(userid))
        sql_list.append(' order by count desc')

        self.db.execute(''.join(sql_list))
        rows = self.db.fetchall()
        result = []
        for row in rows:
            result.append(Word(int(row[0]),row[1],int(row[2])))
        return result
 
    def _cond_join_userchain(self,userid):
        if (userid > 0):
            return ' inner join userchain uc on uc.chain_id = c.id'
        else:
            return ''

    def _cond_userid(self,userid):
        if (userid > 0):
            return ' and uc.user_id = %d' % (userid)
        else:
            return ''

    def _select_nextword(self,words):
        sum_count = sum([x.count for x in words])
        probs = []
        for word in words:
            probs.append(float(word.count)/sum_count)
        randnum = random.random() 
        sum_prob = 0
        nextid_idx = 0
        for i in xrange(len(probs)):
            sum_prob += probs[i]
            if randnum <= sum_prob:
                nextid_idx = i
        return words[i]
    
if __name__=='__main__':
    obj = MarkovChains()
