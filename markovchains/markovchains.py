#!/usr/bin/env python
# -*- coding:utf-8 -*-
import re
import random
import MySQLdb

from util import *
from database import Database

from extractword import Sentence


class MarkovChains(object):

    def __init__(self, dbname='markov', order_num=3):

        self.num = order_num
        self.dbname = dbname

        self.words = {}
        self.chains = {}
        self.userchains = {}

        self.newwords = {}
        self.newchains = {}
        self.newuserchains = {}

        self.chaindic = {}
        self.userchaindic = {}

    def load_db(self, database):
        self.db = Database.create(database, self.dbname)
        self.db.load_db()

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

        words = {}
        for prewords in self.chaindic:
            for preword in prewords:
                words[preword] = 0
            for postword in self.chaindic[prewords]:
                words[postword] = 0
        for user in self.userchaindic:
            for prewords in self.chaindic:
                for preword in prewords:
                    words[preword] = 0
                for postword in self.chaindic[prewords]:
                    words[postword] = 0

        sql = ["('%s')" % (MySQLdb.escape_string(x)) for x in words \
                if x not in existwords]

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
                count = chains[chain].count + exists[chain].count
                isstart = chains[chain].isstart or exists[chain].isstart
                self.db.update_chains(ids, count, isstart)
            else:
                values = []
                for i in xrange(self.num):
                    values.append("%d" % (words[chain[i]]))
                sql.append("(%s,%s,%d)" % (','.join(values),
                                str(chains[chain].isstart).upper(),
                                    chains[chain].count))
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
                    id = allchains[chain].id
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
            info = Word(id=1, name=postword,
                count=chaindic[prewords][postword].count/float(sum_count))
            postwords.append(info)

        return Util.select_nextword(postwords)

if __name__ == '__main__':
    obj = MarkovChains(dbname='markov3',order_num=3)
    obj.load_db('mysql')
    print obj.db.make_sentence()
