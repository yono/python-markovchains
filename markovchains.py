#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
from optparse import OptionParser, OptionValueError
from ConfigParser import SafeConfigParser
import copy
import random
import MySQLdb
from extractword import Sentence

class Word(object):

    def __init__(self,id,name,count):
        self.id = id
        self.name = name
        self.count = count

class MarkovChains(object):

    def __init__(self,dbname='markov',order_num=3):
        user, password = self._load_ini()
        self.con = MySQLdb.connect(user=user,passwd=password,
                charset='utf8',use_unicode=True)
        self.db = self.con.cursor()

        self.num = order_num

        self._load_db(dbname)

        self.words = {}
        self.chains = {}
        self.userchains = {}

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
        sql = []
        sql.append('''
        CREATE TABLE chain (
            id int(11) NOT NULL auto_increment,
        ''')

        for i in xrange(self.num):
            sql.append("word%d_id int(11) NOT NULL default '0'," % i)

        sql.append('''
            isstart BOOL NOT NULL default '0',
            count int(11) NOT NULL default '0',
            PRIMARY KEY (id),
        ''')

        for i in xrange(self.num):
            sql.append("FOREIGN KEY (word%d_id) REFERENCES word(id)," % i)

        sql.append("INDEX ")
        ids = []
        for i in xrange(self.num-1):
            ids.append("word%d_id" % i)
        sql.append("(%s)" % (','.join(ids)))
        sql.append(") DEFAULT CHARACTER SET=utf8")
        self.db.execute('\n'.join(sql))

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
        sql = []
        sql.append("select")
        ids = []
        sql.append(','.join(["w%d.name" % i for i in xrange(self.num)]))
        sql.append(',c.isstart, c.count')
        sql.append('from chain c')
        for i in xrange(self.num):
            sql.append('inner join word w%d on c.word%d_id = w%d.id' % (i,i,i))
        self.db.execute('\n'.join(sql))
        rows = self.db.fetchall()
        words = {}
        for row in rows:
            count = int(row[-1])
            words[tuple(row[0:len(row)-1])] = count
        return words

    def get_userchain(self,userid):
        sql = []
        sql.append("select")
        sql.append(','.join(["w%d.name" % i for i in xrange(self.num)]))
        sql.append(',uc.user_id, c.count, uc.id')
        sql.append('from chain c')
        sql.append('inner join userchain uc on uc.chain_id = c.id')
        for i in xrange(self.num):
            sql.append('inner join word w%d on c.word%d_id = w%d.id' % (i,i,i))
        sql.append("where uc.user_id = %d" % (userid))
        self.db.execute('\n'.join(sql))
        rows = self.db.fetchall()
        words = {}
        for row in rows:
            count = int(row[-2])
            id = int(row[-1])
            words[tuple(row[0:len(row)-2])] = (count,id)
        return words
 
    def _get_punctuation(self):
        punctuation_words = {u'。':0, u'．':0,u'？':0,u'！':0}
        return punctuation_words

    def _insert_words(self,sql):
        self.db.execute(u'INSERT INTO word (name) VALUES %s' % (','.join(sql)))

    def _insert_chains(self,values):
        sql = []
        sql.append("insert into chain(")
        sql.append(",".join(["word%d_id" % i for i in xrange(self.num)]))
        sql.append(",isstart,count)")
        sql.append("values %s" % (','.join(values)))
        self.db.execute('\n'.join(sql))

    def _update_chains(self,ids,count,isstart):
        sql = []
        sql.append('UPDATE chain SET count=%d' % (count))
        sql.append('WHERE')
        for i in xrange(self.num):
            sql.append('word%d_id = %d and ' % (i, ids[i]))
        sql.append('isstart = %d' % (isstart))
        self.db.execute('\n'.join(sql))

    def _insert_userchains(self,sql):
        self.db.execute('''
        insert into userchain(user_id,chain_id,count) values %s
        ''' % (','.join(sql)))

    def _update_userchains(self,count,userchainid):
        self.db.execute('''
            update userchain 
            set count=%d
            where id = %d
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
            ids = [allwords[chain[i]] for i in xrange(self.num)]
            isstart = chain[self.num]
            count = self.newchains[chain]
            if chain in self.chains:
                self._update_chains(ids,count,isstart)
            else:
                values = []
                for i in xrange(self.num):
                    values.append("%d" % (ids[i]))
                sql.append("(%s,%d,%d)" % (','.join(values),isstart,count))

            if (len(sql) % insert_range) == 0 and len(sql) > 0:
                self._insert_chains(sql)
                sql = []

        if sql:
            self._insert_chains(sql)

    def register_userchains(self):
        insert_range = 1000
        allwords = self.get_allwords()
        ids = []
        for i in xrange(self.num):
            ids.append("word%d_id" % i)
        self.db.execute('select id,%s from chain' % (','.join(ids)))
        rows = self.db.fetchall()
        chains = {}
        for row in rows:
            values = []
            for i in xrange(self.num):
                values.append(int(row[i+1]))
            chains[tuple(values)] = int(row[0])

        sql = []
        for chain in self.newuserchains:
            ids = [allwords[chain[i]] for i in xrange(self.num)]
            userid = chain[self.num]
            count = self.newuserchains[chain]
            chainid = chains[tuple(ids)]

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
            if len(self.userchains) == 0:
                self.userchains = self.get_userchain(userid)
            self._update_newuserchains_dic(chain,userid)

    def _update_newwords_dic(self,words):
        for w in words:
            if w['name'] not in self.words:
                self.newwords[w['name']] = self.newwords.get(w['name'], 0) + 1

    def _update_newchains_dic(self,words):
        c_chain = []
        chain = {}
        for word in words:
            if len(c_chain) == self.num:
                values = [x['name'] for x in c_chain]
                values.append(c_chain[0]['isstart'])
                key = tuple(values)
                chain[key] = chain.get(key,0) + 1
                c_chain.pop(0)
            c_chain.append(word)

        rchains = {}
        for wlist in chain:
            self.newchains[wlist] = \
                self.newchains.get(wlist,0) + chain[wlist] +\
                self.chains.get(wlist,0)
            rchains[wlist] = chain[wlist]
        return rchains

    def _update_newuserchains_dic(self,chains,userid):
        for row in chains:
            values = list(row[0:len(row)-1])
            values.append(userid)
            key = tuple(values)
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

        count = 0
        punctuations = self._get_punctuation()
        while True:
            end_cond = count > limit and words[-1].name in punctuations
            if end_cond:
                break

            nextwords = self._get_nextwords(words,userid)
            if len(nextwords) == 0:
                break

            nextword = self._select_nextword(nextwords)
            sentenceid.append(nextword)
            tmp = [words[i] for i in xrange(1,self.num)]
            tmp.append(nextword)
            words = tuple(tmp)
            count += 1
        
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
        sql_list.append('select ')
        for i in xrange(self.num):
            sql_list.append('c.word%d_id, w%d.name,' % (i,i))
        sql_list.append(' c.count')
        sql_list.append(' from chain c')
        for i in xrange(self.num):
            sql_list.append(' inner join word w%d on c.word%d_id = w%d.id' %\
                    (i,i,i))
        sql_list.append(self._cond_join_userchain(userid))
        sql_list.append(' where ')
        sql_list.append(' c.isstart = True')
        sql_list.append(self._cond_userid(userid))

        self.db.execute('\n'.join(sql_list))

        rows = self.db.fetchall()
        row = random.choice(rows)

        result = []
        for i in xrange(0,(self.num*2)-1,2):
            result.append(Word(int(row[i]),row[i+1],int(row[(self.num*2)])))
        return tuple(result)
    
    def _get_nextwords(self,words,userid):
        sql_list = []
        sql_list.append('select c.word%d_id, w.name, c.count' % (self.num-1))
        sql_list.append(' from chain c')
        sql_list.append(' inner join word w on c.word%d_id = w.id' %\
                (self.num-1))
        sql_list.append(self._cond_join_userchain(userid))
        sql_list.append(' where ')
        ids = []
        for i in xrange(self.num-1):
            ids.append(' c.word%d_id = %d' % (i, words[i+1].id))
        sql_list.append(' and'.join(ids))
        sql_list.append(self._cond_userid(userid))
        sql_list.append(' order by count desc')

        self.db.execute('\n'.join(sql_list))
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
            if randnum < sum_prob:
                nextid_idx = i
                break
        return words[nextid_idx]
    
if __name__=='__main__':
    obj = MarkovChains()
