#!/usr/bin/env python
# -*- coding:utf-8 -*-
import random

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
