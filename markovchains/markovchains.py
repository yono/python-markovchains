#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import re
from optparse import OptionParser, OptionValueError
from ConfigParser import SafeConfigParser
import copy
import random
import MySQLdb
from extractword import Sentence

class Util(object):

    @classmethod
    def select_nextword(cls, words):
        sum_count = sum([x.count for x in words])
        probs = []
        for word in words:
            probs.append(word)
            probs[-1].count = float(probs[-1].count) / sum_count
        probs.sort(lambda x, y: cmp(x.count, y.count), reverse=True)
        randnum = random.random()
        sum_prob = 0
        nextword = ''
        for word in probs:
            sum_prob += word.count
            if randnum < sum_prob:
                nextword = word
                break
        return nextword

class Database(object):

    def __init__(self, dbname):

        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.inifile = os.path.join(BASE_DIR, 'settings.ini')
        user, password = self._load_ini()
        self.con = MySQLdb.connect(user=user, passwd=password,
                charset='utf8', use_unicode=True)
        self.cur = self.con.cursor()
        self.dbname = dbname
        self.num = 3

    def __del__(self):
        self.cur.close()
        self.con.close()

    def load_db(self):
        self.cur.execute('show databases')
        rows = self.cur.fetchall()
        for row in rows:
            if row[0] == self.dbname:
                self.cur.execute('use %s' % (self.dbname))
                return
        self._create_db()

    def _load_ini(self):
        parser = SafeConfigParser()
        parser.readfp(open(self.inifile))
        user = parser.get('mysql', 'user')
        password = parser.get('mysql', 'password')
        return (user, password)


    """
    データベース初期化 & テーブル作成
    """
    def _create_db(self):
        self.cur.execute('create database %s default character set utf8' %\
                (self.dbname))
        self.cur.execute('use %s' % (self.dbname))
        self._init_tables()

    def _init_tables(self):
        self._init_user()
        self._init_word()
        self._init_chain()
        self._init_userchain()

    def _init_user(self):
        self.cur.execute('''
        CREATE TABLE user (
            id int(11) NOT NULL auto_increment,
            name varchar(100) NOT NULL default '',
            PRIMARY KEY (id)
        ) DEFAULT CHARACTER SET=utf8
        ''')

    def _init_word(self):
        self.cur.execute('''
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
        for i in xrange(self.num - 1):
            ids.append("word%d_id" % i)
        sql.append("(%s)" % (','.join(ids)))
        sql.append(") DEFAULT CHARACTER SET=utf8")
        self.cur.execute('\n'.join(sql))

    def _init_userchain(self):
        self.cur.execute(u'''
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


    """
    データ挿入 & 更新
    """
    def insert_words(self, sql):
        self.cur.execute(u'INSERT INTO word (name) VALUES %s' %\
                (','.join(sql)))

    def insert_chains(self, values):
        sql = []
        sql.append("insert into chain(")
        sql.append(",".join(["word%d_id" % i for i in xrange(self.num)]))
        sql.append(",isstart,count)")
        sql.append("values %s" % (','.join(values)))
        self.cur.execute('\n'.join(sql))

    def insert_userchains(self, sql):
        self.cur.execute('''
        INSERT INTO userchain(user_id,chain_id,count) VALUES %s
        ''' % (','.join(sql)))

    def update_chains(self, ids, count, isstart):
        sql = []
        sql.append('UPDATE chain SET count=%d' % (count))
        sql.append('WHERE')
        for i in xrange(self.num):
            sql.append('word%d_id = %d and ' % (i, ids[i]))
        sql.append('isstart = %d' % (isstart))
        self.cur.execute('\n'.join(sql))

    def update_userchains(self, count, userchainid):
        self.cur.execute('''
            UPDATE userchain
            SET count=%d
            WHERE id = %d
            ''' % (count, userchainid))


    """
    データ取得
    """
    def get_nextwords(self, words, userid, num):
        sql = []
        sql.append('select c.word%d_id, w.name, c.count' % (num - 1))
        sql.append('from chain c')
        sql.append('inner join word w on c.word%d_id = w.id' % (num - 1))
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
            result.append(Word(int(row[0]), row[1], int(row[2])))
        return result

    def get_startword(self, num, userid=-1, word=None):
        sql = []
        sql.append('select ')
        for i in xrange(num):
            sql.append('c.word%d_id, w%d.name,' % (i, i))
        sql.append(' c.count')
        sql.append(' from chain c')
        for i in xrange(num):
            sql.append(' inner join word w%d on c.word%d_id = w%d.id'\
                    % (i, i, i))
        sql.append(self._cond_join_userchain(userid))
        sql.append(' where ')
        sql.append(' c.isstart = True')
        sql.append(self._cond_userid(userid))
        sql.append(self._cond_wordname(word))

        self.cur.execute('\n'.join(sql))

        rows = self.cur.fetchall()
        row = random.choice(rows)

        result = []
        for i in xrange(0, (num * 2) - 1, 2):
            result.append(Word(int(row[i]), row[i + 1],
                int(row[(num * 2)])))
        return tuple(result)

    def get_allwords(self):
        self.cur.execute('select name,id from word')
        rows = self.cur.fetchall()
        words = dict(rows)
        return words

    def get_allchain(self, num):
        sql = []
        sql.append("select")
        sql.append(','.join(["w%d.name" % i for i in xrange(num)]))
        sql.append(',c.isstart, c.count, c.id')
        sql.append('from chain c')
        for i in xrange(self.num):
            sql.append('inner join word w%d on c.word%d_id = w%d.id' %\
                    (i, i, i))
        self.cur.execute('\n'.join(sql))
        rows = self.cur.fetchall()
        words = {}
        for row in rows:
            id = int(row[-1])
            count = int(row[-2])
            isstart = row[-3]
            words[tuple(row[0: len(row) - 3])] = Chain(id, count, isstart)
        return words

    def get_userchain(self, num, userid):
        sql = []
        sql.append("select")
        sql.append(','.join(["w%d.name" % i for i in xrange(self.num)]))
        sql.append(',uc.user_id, c.count, uc.id')
        sql.append('from chain c')
        sql.append('inner join userchain uc on uc.chain_id = c.id')
        for i in xrange(self.num):
            sql.append('inner join word w%d on c.word%d_id = w%d.id' %\
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
        self.cur.execute('select id from user where name = "%s"' % (user))
        row = self.cur.fetchone()
        if row is None:
            self.cur.execute("insert into user (name) values ('%s')" % \
                            (user))
            self.cur.execute('select id from user where name = "%s"' % \
                            (user))
            row = self.cur.fetchone()
        return int(row[0])

    def get_userid(self, user):
        if user:
            self.cur.execute("select id from user where name = '%s'" % \
                            (user))
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


class Word(object):

    def __init__(self, id, name, count):
        self.id = id
        self.name = name
        self.count = count


class Chain(object):
    
    def __init__(self, id, count, isstart):
        self.id = id
        self.count = count
        self.isstart = isstart


class MarkovChains(object):

    def __init__(self, dbname='markov', order_num=3):

        self.num = order_num

        self.db = Database(dbname)

        self.words = {}
        self.chains = {}
        self.userchains = {}

        self.newwords = {}
        self.newchains = {}
        self.newuserchains = {}

        self.chaindic = {}
        self.userchaindic = {}


    def _get_punctuation(self):
        punctuation_words = {u'。': 0, u'．': 0, u'？': 0, u'！': 0,
                             u'!': 0, u'?': 0,  u'w': 0, u'…': 0}
        return punctuation_words

    """
    文章を解析し、連想配列に保存
    """
    def analyze_sentence(self, text, user=''):
        sentences = self._split_sentences(text)
        for sentence in sentences:
            words = self._get_words(sentence + u'。')
            self._update_newchains_ins(words)
            if user:
                self._update_newchains_ins(words, user)

    def _split_sentences(self, text):
        ps = self._get_punctuation()
        ps = re.compile(u'[%s]' % ('|'.join(ps.keys())))
        return ps.split(text)

    def _get_chains(self, words):
        chain = []
        chains = []
        for word in words:
            if len(chain) == self.num:
                values = [x['name'] for x in chain]
                chains.append(values)
                chain.pop(0)
            chain.append(word)
        return chains

    def _get_chaindic(self, chains, user=''):
        is_start = True
        if user:
            if user not in self.userchaindic:
                self.userchaindic[user] = {}
            chaindic = self.userchaindic[user]
        else:
            chaindic = self.chaindic

        for chain in chains:
            prewords = tuple(chain[0:len(chain)-1])
            postword = chain[-1]
            if prewords not in chaindic:
                chaindic[prewords] = {}
            if postword not in chaindic[prewords]:
                chaindic[prewords][postword] = Chain(0, 0, is_start)
            chaindic[prewords][postword].count += 1
            is_start = False

    def _update_newchains_ins(self, words, user=''):
        chainlist = self._get_chains(words)
        self._get_chaindic(chainlist, user) 

    def _get_words(self, text):
        sentence = Sentence()
        sentence.analysis_text(text)
        words = sentence.get_words()
        result = []
        first = True
        for word in words:
            result.append({'name': word, 'isstart': first})
            first = False
        return result

    """
    連想配列を DB に保存
    """
    def register_data(self):
        self.register_words()
        self.register_chains()
        self.register_userchains()

    def register_words(self):
        existwords = self.db.get_allwords()

        words = set([])
        for prewords in self.chaindic:
            for preword in prewords:
                words.update(preword)
            for postword in self.chaindic:
                words.update(postword)
        for user in self.userchaindic:
            for prewords in self.chaindic:
                for preword in prewords:
                    words.update(preword)
                for postword in self.chaindic:
                    words.update(postword)

        newwords = words.difference(set(existwords.keys()))

        sql = ['("%s")' % (MySQLdb.escape_string(x)) for x in newwords]

        if sql:
            self.db.insert_words(sql)

    def register_chains(self):
        
        # 現在持ってる chain を全て持ってくる
        exists = self.db.get_allchain(self.num)
        words = self.db.get_allwords()

        # 連想配列から同じ形の chain を作成
        chains = {}
        for prewords in self.chaindic:
            for postword in self.chaindic[prewords]:
                chain = []
                chain.extend(list(prewords))
                chain.append(postword)
                chains[tuple(chain)] = self.chaindic[prewords][postword]
            
        # ない場合は新たに作成、ある場合は更新
        insert_step = 1000
        sql = []
        for chain in chains:
            if chain in exists:
                ids = [words[chain[i]] for i in xrange(len(chain))]
                count = chains[chain].count
                isstart = chains[chain].isstart or exists[chain].isstart
                self.db.update_chains(ids, count, isstart)
            else:
                values = []
                try:
                    for i in xrange(self.num):
                        values.append("%d" % (words[chain[i]]))
                    sql.append("(%s,%d,%d)" % (','.join(values),
                                                chains[chain].isstart,
                                                chains[chain].count))
                except:
                    pass
            if (len(sql) % insert_step) == 0 and len(sql) > 0:
                self.db.insert_chains(sql)
                sql = []
        
        if sql:
            self.db.insert_chains(sql)

    def register_userchains(self):
        words = self.db.get_allwords()
        allchains = self.db.get_allchain(self.num)

        for user in self.userchaindic:
            userid = self.db.get_user(user)
            exists = self.db.get_userchain(self.num, userid)
            chains = {}
            for prewords in self.userchaindic[user]:
                for postword in self.userchaindic[user][prewords]:
                    chain = []
                    chain.extend(list(prewords))
                    chain.append(postword)
                    chains[tuple(chain)] = self.chaindic[prewords][postword]
        
            insert_step = 1000
            sql = []
            for chain in chains:
                try:
                    node = allchains[chain]
                except:
                    continue

                if chain in exists:
                    id = exists[chain][1]
                    count = chains[chain].count + node.count
                    self.db.update_userchains(count, id)
                else:
                    id = chains[chain].id
                    count = chains[chain].count
                    sql.append("(%d,%d,%d)" % (userid, id, count))
                    
                if (len(sql) % insert_step) == 0 and len(sql) > 0:
                    self.db.insert_userchains(sql)
                    sql = []
        
            if sql:
                self.db.insert_userchains(sql)

    """
    文章生成
    """
    def make_sentence(self, user=''):
        limit = 1

        if user == '' or user not in self.userchaindic:
            chaindic = self.chaindic
        else:
            chaindic = self.userchaindic[user]
        
        while True:
            prewords = random.choice(chaindic.keys())
            postword = random.choice(chaindic[prewords].keys())
            if chaindic[prewords][postword].is_start:
                break

        words = []
        words.extend(prewords)
        words.append(postword)

        while True:
            if postword in self._get_punctuation() and limit < len(words):
                return ''.join(words)
            next_prewords = list(prewords[1:len(prewords)])
            next_prewords.append(postword)
            if tuple(next_prewords) not in chaindic:
                return ''.join(words)

            postword = self._select_nextword_from_dic(chaindic, prewords)

            postword = random.choice(chaindic[tuple(next_prewords)].keys())
            prewords = next_prewords
            words.append(postword)

    def _select_nextword_from_dic(self, chaindic, _prewords):
        sum_count = 0
        prewords = tuple(_prewords)
        for postword in chaindic[prewords]:
            sum_count += chaindic[prewords][postword].count

        postwords = []

        for postword in chaindic[prewords]:
            info = Word(id=0, name=postword,
                count=chaindic[prewords][postword].count/float(sum_count))
            postwords.append(info)

        return Util.select_nextword(postwords)

if __name__ == '__main__':
    obj = MarkovChains()
    obj.db.load_db()
    print obj.make_sentence_from_db(word=u'大学')
