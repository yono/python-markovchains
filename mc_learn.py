#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import sys
from optparse import OptionParser, OptionValueError
import markovchains

if __name__ == '__main__':
    usage = "usage: %prog filename/dirname [options]"
    parser = OptionParser(usage)
    parser.add_option('-u','--user',action='store',help=u'発言したユーザ')
    (options, args) = parser.parse_args()
    user = ''
    if options.user != None:
        user = options.user
    fileordir = os.path.join(os.environ['PWD'],sys.argv[1])
    files = []
    if os.path.isdir(fileordir):
        files = os.listdir(fileordir)
        for i in xrange(len(files)):
            files[i] = os.path.join(fileordir,files[i])
    elif os.path.isfile(fileordir):
        files.append(fileordir)
    else:
        quit()
    m = markovchains.MarkovChains()
    print fileordir
    for file in files:
        f = open(file).read().splitlines()
        print file
        for line in f:
            last = m.regist_sentence(line,user)
            sys.stdout.write('.')
            sys.stdout.flush()
        print ''

