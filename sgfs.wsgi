#!/usr/bin/env python 
#coding: utf-8 
# sgfs.wsgi 
import os 
import cherrypy 
import sys 
sys.stdout = sys.stderr 
THISDIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.insert(0, THISDIR) 
import sgfs 
application = sgfs.get_wsgi_app()
