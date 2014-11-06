#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2009-2011 Alexander Kojevnikov <alexander@kojevnikov.com>
#
# hilite.me is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# hilite.me is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with hilite.me.  If not, see <http://www.gnu.org/licenses/>.

import datetime
from urllib import quote, unquote

from flask import Flask, make_response, render_template, request

from pygments.lexers import get_all_lexers
from pygments.styles import get_all_styles

from tools import *

import os

target_folder = '/Users/dashti/Dropbox/workspaces/DDBToaster/target/'
input_sql_queries_folder = '/Users/dashti/Documents/MyWorkspaces/DATASRC/dbtoaster/compiler/alpha5/test/queries/'
input_cppscala_folder = target_folder+'tmp/'
output_cppscala_html_folder = target_folder+'html/'
# output_sql_queries_folder = target_folder+'sql/'
output_sql_queries_html_folder = target_folder+'sqlhtml/'
print 'Generating HTML files'
if not os.path.exists(output_cppscala_html_folder):
    os.mkdir(output_cppscala_html_folder)
for fn in os.listdir(input_cppscala_folder):
    with open(input_cppscala_folder+fn, 'r') as content_file:
        code = content_file.read()
        if fn.endswith('.scala'):
            lexer = 'scala'
        else:
            lexer = 'cpp'
        text_file = open(output_cppscala_html_folder+fn+".html", "w")
        text_file.write(hilite_me(code, lexer, dict(), 'colorful', 1, 'border:solid gray;border-width:.1em .1em .1em .8em;padding:.2em .6em;'))
        text_file.close()


print 'Generating SQL files'

sql_sbdirs = ["employee","finance","mddb","simple","tpch","zeus"]

# if not os.path.exists(output_sql_queries_folder):
#     os.mkdir(output_sql_queries_folder)
if not os.path.exists(output_sql_queries_html_folder):
    os.mkdir(output_sql_queries_html_folder)
for sbdirs in sql_sbdirs:
    # if not os.path.exists(output_sql_queries_folder+sbdirs):
    #     os.mkdir(output_sql_queries_folder+sbdirs)
    if not os.path.exists(output_sql_queries_html_folder+sbdirs):
        os.mkdir(output_sql_queries_html_folder+sbdirs)
    for fn in os.listdir(input_sql_queries_folder+sbdirs):
        if fn.endswith('.sql'):
            print fn
            with open(input_sql_queries_folder+sbdirs+'/'+fn, 'r') as content_file:
                code = content_file.read()
                lexer = 'sql'
                code = code.replace('../../experiments', 'examples').replace('../alpha5/test','examples')
                text_file = open(output_sql_queries_html_folder+sbdirs+'/'+fn+".html", "w")
                text_file.write(hilite_me(code, lexer, dict(), 'colorful', 1, 'border:solid gray;border-width:.1em .1em .1em .8em;padding:.2em .6em;'))
                text_file.close()        