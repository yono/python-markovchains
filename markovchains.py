#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
from optparse import OptionParser, OptionValueError
import copy
import sqlite3
import MeCab

class MarkovChains(object):

    def __init__(self):
        self.dbname = '.markov.db'
        BASE_DIR = os.environ['HOME']
        dbfile = os.path.join(BASE_DIR,self.dbname)
        if os.path.exists(dbfile):
            self.db = sqlite3.connect(os.path.join(BASE_DIR,self.dbname))
        else:
            self.db = sqlite3.connect(os.path.join(BASE_DIR,self.dbname))
            self.init_tables()
        self.mecab = MeCab.Tagger()
        #self.words = self.get_allwords()
        #self.chains = self.get_allchain()
        self.words = {}
        self.chains = {}
    
    def init_tables(self):
        self.init_user()
        self.init_word()    
        self.init_chain()
        self.init_userchain()

    def init_user(self):
        db = self.db
        db.execute(u'''
        CREATE TABLE user (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100) NOT NULL
        )
        ''')

    def init_word(self):
        db = self.db
        db.execute(u'''
        CREATE TABLE word (
        id INTEGER PRIMARY KEY,
        name varchar(100) NOT NULL default ''
        )
        ''')

    def init_chain(self):
        db = self.db
        db.execute(u'''
        CREATE TABLE chain (
        id integer primary key,
        word1_id int NOT NULL default '0',
        word2_id int NOT NULL default '0',
        word3_id int NOT NULL default '0',
        isstart TINYINT NOT NULL default '0',
        FOREIGN KEY (word1_id) REFERENCES word(id),
        FOREIGN KEY (word2_id) REFERENCES word(id),
        FOREIGN KEY (word3_id) REFERENCES word(id)
        )
        ''')

    def init_userchain(self):
        db = self.db
        db.execute(u'''
        CREATE TABLE userchain (
        id int primary key,
        user_id int NOT NULL default '0',
        chain_id int NOT NULL default '0',
        FOREIGN KEY (user_id) REFERENCES word(id),
        FOREIGN KEY (chain_id) REFERENCES word(id)
        )
        ''')
    
    def get_allwords(self):
        db = self.db
        rows = db.execute(u'select name,id from word')
        rows = rows.fetchall()
        words = dict(rows)
        return words
 
    def get_allchain(self):
        db = self.db
        rows = db.execute('''
        select word1_id,word2_id,word3_id from chain
        ''')
        rows = rows.fetchall()
        words = dict(zip(rows,range(len(rows))))
        return words

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
        while n:
            if n.surface == '':
                n = n.next
                continue
            words.append({'name':u(n.surface),
                          'feature':u(n.feature)})
            n = n.next

        ## ユーザー登録
        userid = False
        if user:
            r = cur.execute('select id from user where name = "?"',(user))
            if r.fetchone() is None:
                cur.execute('insert into user (name) values ("?")',(user))
                r = cur.execute('select id from user where name = "?"',(user))
            userid = int(r[0]) 
        
        ## 単語登録
        sql = [(x['name'],) for x in words if x['name'] not in allwords]
        
        cur.executemany('insert into word (name) values (?)',(sql))
        for word in sql:
            row = cur.execute('select id from word where name = ?',(word))
            result = row.fetchone()
            allwords[word[0]] = result[0]

        ## マルコフ連鎖登録
        w1 = ''
        w2 = ''
        chain = {}
        for word in words:
            name = word['name']
            if w1 and w2:
                if (w1,w2) not in chain:
                    chain[(w1,w2)] = {}
                chain[(w1,w2)][name] = chain[(w1,w2)].get(name, 0) + 1
            w1,w2 = w2,name
        
        sql = []
        for wlist in chain:
            for word in chain[wlist]:
                id0 = allwords[wlist[0]]
                id1 = allwords[wlist[1]]
                id2 = allwords[word]
                if (id0,id1,id2) not in chains:
                    sql.append((id0,id1,id2))
                    self.chains[(id0,id1,id2)] = 0
        
        cur.executemany('''
        insert into chain (word1_id,word2_id,word3_id) values (?,?,?)
        ''', (sql))

        ## ユーザーchain登録
        if userid:
            ids = []
            for wlist in chain:
                for word in markov[wlist]:
                    id0 = words[wlist[0]]
                    id1 = words[wlist[1]]
                    id2 = words[word]
                    row = cur.execute('''
                    select id from chain where word1_id = ? and 
                    word2_id = ? and word3_id = ?
                    ''' % (id0,id1,id2))
                    ids.append((userid,int(row[0])))
            cur.executemany('''
            insert into chain (user_id,chain_id) values (?,?)
            ''', (ids))
        
        cur.commit()

    def make_sentence(self,user=''):
        limit = 20
        cur = self.db

        ## 文頭の言葉を取得
        if user == '':
            row = cur.execute('''
            select word1_id,word2_id,word3_id from chain 
            order by random() limit 1
            ''')
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
        row = row.fetchone()
        wordid = map(int, row)

        sentenceid = copy.copy(wordid)

        ## テーブルを参照して文章(単語idの配列)生成
        count = 0
        ## 句読点
        kutouten = {4894:0,33256:0,34781:0}
        while True:
            if user == '':
                row = cur.execute('''
                select word3_id from chain
                where word1_id = ? and word2_id = ?
                order by random() limit 1
                ''',(wordid[1],wordid[2]))
            else:
                row = cur.execute('''
                select c.word3_id,w.isend from chain c,user_chain uc,word w
                where c.word1_id = ? and c.word2_id = ?
                and uc.chain_id = c.id 
                and uc.user_id = ? and c.word3_id = w.id
                order by rand() limit 1
                ''',(wordid[1],wordid[2],userid))
            row = row.fetchone()
            if row is None:
                break
            nextid = int(row[0])
            #isend = int(row[1])
            sentenceid.append(nextid)
            wordid = [wordid[1],wordid[2],nextid]
            #if nextid in stopid:
            #    break
            if count > limit and nextid in kutouten:
                break
            count += 1
        
        ## idを基に実際の文章を生成 
        sentence = ''
        print len(sentenceid)
        for i in xrange(len(sentenceid)):
            row = cur.execute('select name from word where id = ?'\
                    ,(sentenceid[i],))
            row = row.fetchone()
            sentence = '%s%s' % (sentence,row[0])

        return sentence

