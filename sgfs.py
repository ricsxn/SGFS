#!/usr/bin/env python 
#coding: utf-8 
'''
Science Gateway File System

REST based service to manage Science Gateway files stored on LFC file catalogs

Copyright (c) 2011:
Istituto Nazionale di Fisica Nucleare (INFN), Italy
Consorzio COMETA (COMETA), Italy

See http://www.infn.it and and http://www.consorzio-cometa.it for details on
the copyright holders.

Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 
Author: Riccardo Bruno (riccardo.bruno@ct.infn.it) 
'''

'''
SUMMARY
'''
__author__     = ["Riccardo Bruno"]
__copyright__  = "Copyright 2012, INFN Sez. Catania"
__credits__    = ["Roberto Barbera"]
__license__    = "Apache 2.0"
__version__    = "1.0.0"
__maintainer__ = "Riccardo Bruno"
__email__      = "riccardo.bruno@ct.infn.it"
__status__     = "Development"


import os
import sys
import signal
import cherrypy
import MySQLdb
import tempfile
import simplejson as json
import gfalthr
from subprocess import Popen, PIPE, STDOUT
from cherrypy.lib.static import serve_file
from xml.dom.minidom import Document

##
## Some global stuff... (maybe config in the future)
##
SGFS_Host=os.getenv('HOSTNAME')
SGFS_Port=8088

##
## Class that manages the SGFS outputs in XML or JSON formats
##
class SGFSOutput:
	@staticmethod
	def jsonMode(Mode=None):
		jsonMode = False
		if Mode is not None and Mode.lower() == 'true':
			jsonMode = True
		return jsonMode
		
	def __init__(self,JSon=False):
		self.JSon = JSon
		if self.JSon is True:
			self.doc = {}
			self.index = 0
		else:
			self.doc = Document()
	
	def Answer(self,Mode=True):
		if Mode is True:
			answer_value="OK"
		else:
			answer_value="KO"
		if self.JSon is True:
			#answer=[{ 'status' : answer_value }]
			#self.doc['answer'] = answer
			answer = []
			self.doc = { 'status' : answer_value , 'answer' : answer }
		else:
			answer = self.doc.createElement('answer')  
			self.doc.appendChild(answer)  
			status = self.doc.createElement('status')  
			answer.appendChild(status)  
			status_node = self.doc.createTextNode(answer_value)  
			status.appendChild(status_node)
		return answer
	
	def addBlockValue(self,block,key_name,key_value,key_attributes=None):
		if self.JSon is True:
			if key_attributes is None:
				block.append({ str(key_name) : str(key_value) })
			else:
				attributes={}
				for a in key_attributes:
					attributes[str(a[0])]=str(a[1])
				block.append({ str(key_name) : { str(key_value) : attributes }})
		else:
			key_element = self.doc.createElement(str(key_name))
			if key_attributes is not None:
				for a in key_attributes:
					key_element.setAttribute(str(a[0]),str(a[1]))
			block.appendChild(key_element)  
			key_node = self.doc.createTextNode(str(key_value))  
			key_element.appendChild(key_node)
	
	def addValue(self,block,key_name,key_attributes=None):
		if self.JSon is True:
			attributes = {}
			if key_attributes is not None:
				for a in key_attributes:
					attributes[str(a[0])]=str(a[1])
			block.append({ str(key_name) : attributes })
		else:
			key_element = self.doc.createElement(str(key_name))
			if key_attributes is not None:
				for a in key_attributes:
					key_element.setAttribute(str(a[0]),str(a[1]))
			block.appendChild(key_element)
	
	def newBlock(self,parent_block,block_name):
		if self.JSon is True:
			block_element = []
			parent_block.append({ str(block_name) : block_element })
		else:
			block_element = self.doc.createElement(str(block_name))  
			parent_block.appendChild(block_element) 
		return block_element
	
	def render(self,indent='\t',encoding="UTF-8"):
		if self.JSon is True:
			cherrypy.response.headers['Access-Control-Allow-Origin']='*'
			cherrypy.response.headers['Content-Type']= 'application/json'
			return json.dumps(self.doc)
		else:
			cherrypy.response.headers['Content-Type']= 'text/xml'
			return str(self.doc.toprettyxml(indent,encoding="UTF-8"))

##
## Class that executes commands
##
class ExecCmd:
	p       = None
	command = None
	output  = None

	def __init__(self):
		self.p=None
		
	def __del__(self):
		if self.p is not None:
			print "[i] Waiting for PID: %s" % self.p.pid
			self.p.wait()
	
	def cmd(self,command):
		self.command=command
		print "[!] %s" % self.command
		self.p = Popen(self.command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
		self.output = self.p.stdout.read()
		return self.output
	
	def bgCmd(self,command):
		self.command=command
		print "[&] %s" % self.command
		self.p = Popen(self.command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
		return self.p
	
	def returnCode(self):
		self.p.wait()
		return self.p.returncode
		
	def kill(self):
		if self.p is not None:
			os.kill(self.p.pid,signal.SIGKILL)
			
	def killAll(self):
		if self.p is not None:
			cmd = """ps -o pid= --ppid %s | xargs kill""" % self.p.pid
			print "[!] %s" % cmd 
			p=Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
			p.wait()
			self.kill()
##
## Class that interacts with the SGFS database
##
class SGFSDB:
	def __init__(self):
		self.database_host='localhost'
		self.database_port='3036'
		self.database_name='sgfs'
		self.database_username='sgfs_user'
		self.database_password='sgfs_password'
		self.dbConn=None

	def connect(self):
		self.dbConn = MySQLdb.connect(\
			self.database_host,\
			self.database_username,\
			self.database_password,\
			self.database_name)

	def close(self):
		self.dbConn.close()

	def commit(self):
		self.dbConn.commit()

	def execute(self,sql_query):
		cursor=self.dbConn.cursor()
		print sql_query
		cursor.execute(sql_query)
		return cursor

	def getUserId(self,user_name=None):
		self.connect()
		cursor = self.execute("""select user_id from sgfs_users where user_name = '%s';""" % user_name)
		user_id=cursor.fetchone()[0]
		self.close()
		return user_id

	def getApplicationId(self,application_name=None):
		self.connect()
		cursor = self.execute("""select app_id from sgfs_applications where app_name = '%s';""" % application_name)
		application_id=cursor.fetchone()[0]
		self.close()
		return application_id

	def getAppInfraId(self,app_id=None):
		self.connect()
		cursor = self.execute("""select infra_id from sgfs_applications where app_id=%s""" % app_id)
		infra_id=cursor.fetchone()[0]
		self.close()
		return infra_id

	def registerTransaction(self,user_name,app_name):
		user_id=self.getUserId(user_name)
		app_id=self.getApplicationId(app_name)
		infra_id=self.getAppInfraId(app_id)
		self.connect()
		self.execute("""insert into sgfs_transactions (user_id,app_id,infra_id,transaction_from,transaction_ip) values (%s,%s,%s,now(),'%s');""" %(user_id,app_id,infra_id,cherrypy.request.remote.ip))
		cursor=self.execute("""select max(transaction_id) from sgfs_transactions where user_id=%s and app_id=%s;""" %(user_id,app_id))
		transaction_id= cursor.fetchone()[0]
		self.commit()
		self.close()
		return transaction_id

	def getInfrastructureId(self,transaction_id):
		self.connect()
		cursor=self.execute("""select infra_id from sgfs_transactions t where transaction_id=%s""" % transaction_id)
		app_vo= cursor.fetchone()[0]
		self.close()
		return app_vo

	def storeTransactionProxy(self,transaction_id,tmpfile):
		self.connect()
		self.execute("""update sgfs_transactions set transaction_proxy = '%s' where transaction_id = %s;""" % (tmpfile,transaction_id))
		self.commit()
		self.close()

	def getInfrastrucutureById(self,infra_id):
		self.connect()
		cursor=self.execute("""select infra_id,infra_name,infra_desc,infra_pxhost,infra_pxport,infra_pxid,infra_pxvo,infra_pxrole,infra_pxrenewal,infra_bdii,infra_lfc from sgfs_infrastructures where infra_id=%s;""" % infra_id)
		row=cursor.fetchone()
		t =  (row[0], \
		      row[1], \
		      row[2], \
		      row[3], \
		      row[4], \
		      row[5], \
		      row[6], \
		      row[7], \
		      row[8], \
		      row[9], \
		      row[10])
		self.close()
		return t

	def closeTransaction(self,transaction_id):
		self.connect()
		self.execute("""update sgfs_transactions set transaction_to = now() where transaction_id=%s;""" % transaction_id)
		self.commit()
		cursor=self.execute("""select transaction_proxy from sgfs_transactions where transaction_id=%s;""" % transaction_id)
		tmp_file=cursor.fetchone()[0]
		self.close()
		return tmp_file

	def getTransactionLFCData(self,transaction_id):
		self.connect()
		cursor=self.execute("""select i.infra_bdii, i.infra_lfc, a.app_lfcdir, u.user_name, transaction_proxy, i.infra_pxvo from sgfs_transactions t, sgfs_infrastructures i, sgfs_users u, sgfs_applications a where t.infra_id=i.infra_id and t.app_id = a.app_id and t.user_id=u.user_id and t.transaction_id=%s;""" % transaction_id)
		row=cursor.fetchone()
		t = (row[0], \
		     row[1], \
		     row[2], \
		     row[3], \
		     row[4], \
		     row[5])
		self.close()
		return t

	def registerAction(self,transaction_id,action,lfc_file_name,file_name):
		self.connect()
		self.execute("""insert into sgfs_actions (transaction_id,action_ts,action,lfc_file_name,file_name,action_ip) values (%s,now(),%s,'%s','%s','%s');""" %(transaction_id,action,lfc_file_name,file_name,cherrypy.request.remote.ip))
		cursor = self.execute("""select max(action_id) from sgfs_actions;""")
		action_id=cursor.fetchone()[0]
		self.commit()
		self.close()
		return action_id

	def getActionFiles(self,transaction_id):
		self.connect()
		cursor=self.execute("""select file_name from sgfs_actions where transaction_id=%s and action=0;""" % transaction_id)
		file_list=()
		for row in cursor.fetchall():
			file_list+=(row[0],)
		self.close()
		return file_list

	def registerBooking(self,action_id,transaction_id,file_size,p):
		self.connect()
		self.execute("""insert into sgfs_bookings (action_id,transaction_id,file_size,download_file_size,download_pid,booking_ip) values (%s,%s,%s,0,%s,'%s');""" % (action_id,transaction_id,file_size,p.pid,cherrypy.request.remote.ip))
		cursor = self.execute("""select max(booking_id) from sgfs_bookings;""")
		booking_id=cursor.fetchone()[0]
		self.commit()
		self.close()
		return booking_id

	def getTransactionKeys(self,transaction_id):
		self.connect()
		cursor = self.execute("""select user_id, app_id from sgfs_transactions where transaction_id = %s""" % transaction_id)
		row=cursor.fetchone()
		user_id=row[0]
		app_id =row[1]
		self.close()
		return (user_id, app_id)

	def getBookings(self,transaction_id):
		user_id, app_id = self.getTransactionKeys(transaction_id)
		self.connect()
		cursor = self.execute("""select b.booking_id, b.file_size, b.download_file_size, b.download_pid, b.download_url, a.action_id, a.file_name, t.transaction_id from sgfs_bookings b, sgfs_actions  a, sgfs_transactions t where a.transaction_id = t.transaction_id and b.transaction_id = t.transaction_id and a.action_id = b.action_id and a.action=2 and t.user_id = %s and t.app_id = %s;""" % (user_id,app_id))
		self.close()
		return cursor.fetchall()

	def updateBookingFileSize(self,booking_id,new_download_file_size):
		self.connect()
		self.execute("""update sgfs_bookings set download_file_size = %s where booking_id = %s;""" % (new_download_file_size,booking_id))
		self.commit()
		self.close()

	def updateBookingUrl(self,booking_id,transaction_id):
		self.connect()
		download_url="http://%s:%s/async_download?transaction_id=%s&booking_id=%s" % (SGFS_Host,SGFS_Port,transaction_id,booking_id)
		self.execute("""update sgfs_bookings set download_url = '%s' where booking_id = %s;""" % (download_url,booking_id))
		self.commit()
		self.close()

	def getBookedFile(self,booking_id):
		self.connect()
		cursor = self.execute("""select a.file_name from sgfs_actions a, sgfs_bookings b where a.action_id = b.action_id and b.booking_id=%s;""" % booking_id)
		file_name=cursor.fetchone()[0]
		self.close()
		return file_name

	def closeBookings(self,transaction_id,booking_ids):
		booking_files=()
		booking_pids=()
		if booking_ids is None:
			booking_ids=()
			user_id, app_id = self.getTransactionKeys(transaction_id)
			self.connect()
			cursor = self.execute("""select b.booking_id, b.download_pid ,a.file_name from sgfs_bookings b, sgfs_actions a, sgfs_transactions t where a.transaction_id = t.transaction_id and b.transaction_id = t.transaction_id and a.action_id = b.action_id and a.action=2 and t.user_id = %s and t.app_id = %s;""" % (user_id,app_id))
			for row in cursor.fetchall():
				booking_ids=booking_ids+(row[0],)
				booking_pids=booking_pids+(row[1],)
				booking_files=booking_files+(row[2],)
			self.close()
		else:
			self.connect()
			for booking_id in booking_ids:
				cursor = self.execute("""select a.file_name, b.download_pid from sgfs_actions a, sgfs_bookings b  where a.action_id = b.action_id and b.download_pid is not NULL and b.booking_id = %s;""" % booking_id)
				for row in cursor.fetchall():
					booking_files=booking_files+(row[0],)
					booking_pids=booking_pids+(row[1],)
			self.close()
		self.connect()
		for booking_id in booking_ids:
			self.execute("""update sgfs_actions set action=3 where action_id = (select action_id from sgfs_bookings where booking_id = %s);""" % booking_id)
		self.commit()
		self.close()
		return (booking_files,booking_pids)
		
	def orphanBooking(self, booking_id, download_pid):
		self.connect()
		#self.execute("""update sgfs_bookings set download_pid=NULL where booking_id = %s and download_pid = %s;""" % (booking_id,download_pid))
		self.execute("""update sgfs_actions set action=5 where action_id=(select action_id from sgfs_bookings where booking_id=%s);""" % booking_id)
		self.commit()
		cursor=self.execute("""select file_name from sgfs_actions where action_id = (select b.action_id from sgfs_bookings b where booking_id = %s);""" % booking_id);
		file_name=cursor.fetchone()[0]
		self.close()
		return file_name
		
	def downloadInfo(self,file_guid):
		self.connect()
		cursor = self.execute("""select u.user_name,a.app_name,d.file_name,d.abs_path,d.date_from,d.date_to,d.down_count from sgfs_users u, sgfs_applications a, sgfs_downloads d where d.app_id=a.app_id and d.user_id=u.user_id and d.guid = '%s'""" % file_guid)
		row = cursor.fetchone()
		if row is not None:
			user_name         = row[0]
			application_name  = row[1]
			lfc_file_name     = row[2]
			lfc_absolute_path = row[3]
			date_from         = row[4]
			date_to           = row[5]
			down_count        = row[6]
		else:
			user_name         = ''
			application_name  = ''
			lfc_file_name     = ''
			lfc_absolute_path = False
			date_from         = None
			date_to           = None
			down_count        = None

		self.close()
		return user_name,application_name,lfc_file_name,lfc_absolute_path,date_from,date_to,down_count

##
## Class that manages Infrastructure settings
##
class Infrastructure:
	
	def __init__(self, infra_id=None):
		self.infra_id=0
		self.infra_name=''
		self.infra_desc=''
		self.px_host=''
		self.px_port=0
		self.px_id=0
		self.px_vo=''
		self.px_role=''
		self.px_renewal=False
		self.infra_bdii=''
		self.infra_lfc=''
		
		if infra_id is not None:
			sgfsDB=SGFSDB()
			self.infra_id,   \
			self.infra_name, \
			self.infra_desc, \
			self.px_host,    \
			self.px_port,    \
			self.px_id,      \
			self.px_vo,      \
			self.px_role,    \
			self.px_renewal, \
			self.infra_bdii, \
			self.infra_lfc   = sgfsDB.getInfrastrucutureById(infra_id)
		self.dump()

	def dump(self):
		print "Infrastructure"
		print "--------------"
		print "infra_Id   : '%s'" % self.infra_id
		print "infra_Name : '%s'" % self.infra_name
		print "infra_Desc : '%s'" % self.infra_desc
		print "px_Host    : '%s'" % self.px_host
		print "px_Port    : '%s'" % self.px_port
		print "px_Id      : '%s'" % self.px_id
		print "px_VO      : '%s'" % self.px_vo
		print "px_role    : '%s'" % self.px_role
		print "px_renewal : '%s'" % self.px_renewal
		print "infra_bdii : '%s'" % self.infra_bdii
		print "infra_lfc  : '%s'" % self.infra_lfc
	
	def getProxy(self,temp_file=None):
		execCmd=ExecCmd()
		if temp_file is None:
			temp_file = "/tmp/x509up_u$(id -u)"
		execCmd.cmd("""wget \"http://%s:%s/eTokenServer/eToken/%s?voms=%s:%s&proxy-renewal=%s\" -O %s && chmod 600 %s""" % (self.px_host,self.px_port,self.px_id,self.px_vo,self.px_role,self.px_renewal,temp_file,temp_file))
		

##
## LFC Class - Manages the LFC file catalog
##
class LFC:
	def __init__(self,infra_bdii=None,infra_lfc=None,app_lfcdir=None,user_name=None,transaction_proxy=None,infra_pxvo=None):
		self.infra_bdii=infra_bdii
		self.infra_lfc=infra_lfc
		self.app_lfcdir=app_lfcdir
		self.user_name=user_name
		self.transaction_proxy=transaction_proxy
		self.infra_pxvo=infra_pxvo
		self.dump()
		
	def dump(self):
		print "LFC"
		print "---"
		print "BDII  : '%s'" % self.infra_bdii
		print "LFC   : '%s'" % self.infra_lfc
		print "AppDir: '%s'" % self.app_lfcdir
		print "User  : '%s'" % self.user_name
		print "Proxy : '%s'" % self.transaction_proxy
		print "VO    : '%s'" % self.infra_pxvo

	def list(self,lfc_file_name=None):
		execCmd=ExecCmd()
		if lfc_file_name is not None:
			cmd_extension = '| grep %s' % lfc_file_name
		else:
			cmd_extension = ''
		files_list=execCmd.cmd("""export LCG_GFAL_INFOSYS=%s && export LFC_HOST=%s && export X509_USER_PROXY=%s && lfc-ls -l --comment /grid/%s/sgfs/%s/%s %s""" % (self.infra_bdii,self.infra_lfc,self.transaction_proxy,self.infra_pxvo,self.app_lfcdir,self.user_name,cmd_extension))
		return (execCmd.returnCode(),files_list)
		
	def file(self,lcgCpCmd,lfc_file_name,lfc_absolute_path=False):
		execCmd   = ExecCmd()
		lfc_file_name=lfc_file_name.replace('(','\(')\
		                           .replace(')','\)')
		tmpdir  = tempfile.mkdtemp()
		if lfc_absolute_path == True:
			tmpfile = "%s/%s" %(tmpdir,os.path.basename(lfc_file_name))
			cmd="""export LCG_GFAL_INFOSYS=%s && export LFC_HOST=%s && export X509_USER_PROXY=%s && lfc-ls -l %s""" % (self.infra_bdii,self.infra_lfc,self.transaction_proxy,lfc_file_name)
		else:
			tmpfile = "%s/%s" %(tmpdir,lfc_file_name)
			cmd="""export LCG_GFAL_INFOSYS=%s && export LFC_HOST=%s && export X509_USER_PROXY=%s && lfc-ls -l /grid/%s/sgfs/%s/%s/%s""" % (self.infra_bdii,self.infra_lfc,self.transaction_proxy,self.infra_pxvo,self.app_lfcdir,self.user_name,lfc_file_name)
		file_entry=execCmd.cmd(cmd)
		returnCode=execCmd.returnCode()
		if returnCode == 0:
			cmd="""echo '%s' | awk '{ print $5 }'""" % file_entry
			file_size=execCmd.cmd(cmd).strip()
			if lfc_absolute_path == True:
				cmd="""export LCG_GFAL_INFOSYS=%s && export LFC_HOST=%s && export X509_USER_PROXY=%s && lcg-cp --vo %s -n 3 lfn:%s file:%s"""  % (self.infra_bdii,self.infra_lfc,self.transaction_proxy,self.infra_pxvo,lfc_file_name,tmpfile)
			else:
				cmd="""export LCG_GFAL_INFOSYS=%s && export LFC_HOST=%s && export X509_USER_PROXY=%s && lcg-cp --vo %s -n 3 lfn:/grid/%s/sgfs/%s/%s/%s file:%s""" % (self.infra_bdii,self.infra_lfc,self.transaction_proxy,self.infra_pxvo,self.infra_pxvo,self.app_lfcdir,self.user_name,lfc_file_name,tmpfile)
			p=lcgCpCmd.bgCmd(cmd)
			count=execCmd.cmd('for ((i=0; i<30; i++)); do if [ -s %s ]; then i=0; break; fi; sleep 1; done; echo $i' % tmpfile)
			i_count=int(count)
			if(i_count != 0):
				tmpfile="Timeout downloading file: '%s'" % lfc_file_name
				returnCode=1
				os.kill(p.pid, signal.SIGKILL)
		else:
			tmpfile=file_entry
			file_size=0
		return returnCode,cmd,file_size,tmpfile

	def rm(self,lfc_file_name):
		execCmd=ExecCmd()
		execCmd.cmd("""export LCG_GFAL_INFOSYS=%s ; export LFC_HOST=%s ; export X509_USER_PROXY=%s ;lcg-del -a lfn:/grid/%s/sgfs/%s/%s/%s""" % (self.infra_bdii,self.infra_lfc,self.transaction_proxy,self.infra_pxvo,self.app_lfcdir,self.user_name,lfc_file_name))
		return "lfn:/grid/%s/sgfs/%s/%s/%s" % (self.infra_pxvo,self.app_lfcdir,self.user_name,lfc_file_name)

	def book(self, lfc_file_name):
		tmpdir  = tempfile.mkdtemp()
		tmpfile = "%s/%s" %(tmpdir,lfc_file_name)
		result, file_info = self.list(lfc_file_name)
		file_size = file_info.split('\n')[0].split()[4]
		execCmd=ExecCmd()
		p=execCmd.bgCmd("""export LCG_GFAL_INFOSYS=%s ; export LFC_HOST=%s ; export X509_USER_PROXY=%s ; lcg-cp --vo %s -n 3 lfn:/grid/%s/sgfs/%s/%s/%s file:%s""" % (self.infra_bdii,self.infra_lfc,self.transaction_proxy,self.infra_pxvo,self.infra_pxvo,self.app_lfcdir,self.user_name,lfc_file_name,tmpfile))
		return tmpfile, file_size, p

	def getSurls(self,lfc_file_name):
		execCmd=ExecCmd()
		cmd="""export LCG_GFAL_INFOSYS=%s && export LFC_HOST=%s && export X509_USER_PROXY=%s && lcg-lr lfn:/grid/%s/sgfs/%s/%s/%s""" % (self.infra_bdii,self.infra_lfc,self.transaction_proxy,self.infra_pxvo,self.app_lfcdir,self.user_name,lfc_file_name)
		surls=execCmd.cmd(cmd)
		returnCode=execCmd.returnCode()
		return returnCode,cmd,surls.split('\n')

	def regSurl(self, surl, lfc_file_name, lfc_path=None):
		execCmd=ExecCmd()
		if(lfc_path != None):
		    lfc_path="lfn:/grid/%s/%s/%s" % (self.infra_pxvo,lfc_path,lfc_file_name) 
		else:
			lfc_path="lfn:/grid/%s/sgfs/%s/%s/%s" % (self.infra_pxvo,self.app_lfcdir,self.user_name,lfc_file_name)
		cmd="""export LCG_GFAL_INFOSYS=%s && export LFC_HOST=%s && export X509_USER_PROXY=%s && lcg-rf -v --vo %s -l %s %s""" % (self.infra_bdii,self.infra_lfc,self.transaction_proxy,self.infra_pxvo,lfc_path,surl)
		cmd_output=execCmd.cmd(cmd)
		returnCode=execCmd.returnCode()
		if (0 == returnCode):
			cmd="""echo %s | awk  -F':' '{ print $2 }'""" % cmd_output
			cmd_output=execCmd.cmd(cmd)
		return returnCode,cmd,cmd_output,lfc_path
		
	def file_data(self,lfc_file_name,lfc_absolute_path=False):
		file_size=0
		lfc_file_name=lfc_file_name.replace('(','\(')\
		                           .replace(')','\)')
		if lfc_absolute_path == True:
			lfc_file_path=lfc_file_name
		else:
			lfc_file_path="""/grid/%s/sgfs/%s/%s/%s""" % (self.infra_pxvo,self.app_lfcdir,self.user_name,lfc_file_name)
		cmd="""export LCG_GFAL_INFOSYS=%s && export LFC_HOST=%s && export X509_USER_PROXY=%s && lfc-ls -l %s""" % (self.infra_bdii,self.infra_lfc,self.transaction_proxy,lfc_file_path)
		execCmd=ExecCmd()
		file_entry=execCmd.cmd(cmd)
		returnCode=execCmd.returnCode()
		if returnCode == 0:
			cmd="""echo '%s' | awk '{ print $5 }'""" % file_entry
			file_size=execCmd.cmd(cmd).strip()
		return returnCode,cmd,file_size,lfc_file_path

##
## CherryPy REST handler classes
##
class Root(object):
	@cherrypy.expose
	def index(self,json=None):
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		answer_block=sgfsOutput.Answer(True)
		info_block=sgfsOutput.newBlock(answer_block,'product_info')
		sgfsOutput.addBlockValue(info_block,"name","SGFS - Science Gateway File System") 
		sgfsOutput.addBlockValue(info_block,"description","A REST based service to manage Science Gateway files stored on LFC file catalogs")
		sgfsOutput.addBlockValue(info_block,"version","v1.0")
		auth_block=sgfsOutput.newBlock(answer_block,'author')
		sgfsOutput.addBlockValue(auth_block,"name","Riccardo Bruno")
		sgfsOutput.addBlockValue(auth_block,"company","Consorzio COMETA (2012)")
		sgfsOutput.addBlockValue(auth_block,"email","riccardo.bruno@ct.infn.it")
		licence='''Copyright (c) 2011:
Istituto Nazionale di Fisica Nucleare (INFN), Italy
Consorzio COMETA (COMETA), Italy

See http://www.infn.it and and http://www.consorzio-cometa.it for details on
the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.'''
		sgfsOutput.addBlockValue(answer_block,"Licence",licence)
		service_block=sgfsOutput.newBlock(answer_block,'services')
		sgfsOutput.addBlockValue(service_block,"service","Shows this information",(('address','/'),))
		sgfsOutput.addBlockValue(service_block,"service","Begin a new transaction with a given user name and application name",(('address','/begin/<username>/<appname>'),))
		sgfsOutput.addBlockValue(service_block,"service","List user' files stored on the LFC file catalog",(('address','/list/<transaction_id>'),))
		sgfsOutput.addBlockValue(service_block,"service","Synchronous download a given file from the LFC file catalog",(('address','/download/<transaction_id>/<file_name>'),))
		sgfsOutput.addBlockValue(service_block,"service","Delete a given file from the LFC file catalog",(('address','/delete/<transaction_id>/<file_name>'),))
		sgfsOutput.addBlockValue(service_block,"service","Book a file to be downloaded from LFC to the server",(('address','/book/<transaction_id>/<file_name>'),))
		sgfsOutput.addBlockValue(service_block,"service","Show the list of booked files containing their statuses",(('address','/bookings/<transaction_id>'),))
		sgfsOutput.addBlockValue(service_block,"service","Closes all booking associated to the given transaction",(('address','/close_bookings/<transaction_id>'),))
		sgfsOutput.addBlockValue(service_block,"service","Download a booked file",(('address','/sync_download/<transaction_id>/<booking_id>'),))
		sgfsOutput.addBlockValue(service_block,"service","Close the given transaction",(('address','/close/<transaction_id>'),))
		sgfsOutput.addBlockValue(service_block,"service","Get the SURL address of a given LFC file",(('address','/surl/<transaction_id>/<file_name>'),))
		sgfsOutput.addBlockValue(service_block,"service","Registers a given SURL into the LFC file catalog (Works only in POST mode!)",(('address','/register_surl/<transaction_id>/<surl>/<lfc_file_name>[/<lfc_file_path>]'),))
		return sgfsOutput.render('\t')

class JSONTester:
	@cherrypy.expose
	def index(self,json=None,**params):
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		answer_block=sgfsOutput.Answer(True)
		sgfsOutput.addBlockValue(answer_block,"JSon","This is a service test")
		return sgfsOutput.render('\t')
		# JQuery needs the following headers:
		#  - Access-Control-Allow-Origin: Normally cross domain is not allowed
		#  - Content-Type: Many are availavle, the most used is application/json
		#cherrypy.response.headers['Access-Control-Allow-Origin']='*'
		#cherrypy.response.headers['Content-Type']='application/json'
		#  - In case user wants to call JSONP, the asnwer shoud be prefixed
		#    by a xxxxx() statement where xxxxx string could come from parameters
		#    a default xxxxx statement could be: '_jqjsp'
		#    return "_jqjsp(<json_answer>)"

class beginTransaction:
	@cherrypy.expose
	def index(self,user_name=None,application_name=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(user_name,application_name,json)
		
	def GET(self,user_name=None,application_name=None,json=None):
		# Register the new transaction on the database
		sgfsDB=SGFSDB()
		transaction_id=sgfsDB.registerTransaction(user_name,application_name)
		# Associate a proxy to the new transaction
		tmpfile = tempfile.mktemp()
		infra_id=sgfsDB.getInfrastructureId(transaction_id)
		infrastructure=Infrastructure(infra_id)
		infrastructure.getProxy(tmpfile)
		sgfsDB.storeTransactionProxy(transaction_id,tmpfile)
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		answer_block=sgfsOutput.Answer(True)
		sgfsOutput.addBlockValue(answer_block,"transaction_id",transaction_id)
		return sgfsOutput.render('\t')

class endTransaction:
	@cherrypy.expose
	def index(self,transaction_id=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(transaction_id,json)
		
	def GET(self,transaction_id=None,json=None):
		# End the given transaction on the database
		sgfsDB=SGFSDB()
		# Before close the transactio; remove all files in action table
		for action_file in sgfsDB.getActionFiles(transaction_id):
			action_dir=os.path.dirname(action_file)
			print "Removing %s - %s" % (action_file,action_dir)
			try:
				os.unlink(action_file)
			except OSError:
				print "EXCEPTION: unlink %s" % action_file
			try:
				os.rmdir(action_dir)
			except OSError:
				print "EXCEPTION: rmdir  %s" % action_dir
		tmpfile=sgfsDB.closeTransaction(transaction_id)
		print "Removing proxy file - %s" % tmpfile
		try:
			os.unlink(tmpfile)
		except OSError:
			print "EXCEPTION: unlink %s" % tmpfile
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		answer_block=sgfsOutput.Answer(True)
		sgfsOutput.addBlockValue(answer_block,"transaction_id",transaction_id)
		return sgfsOutput.render('\t')

class listTransactionFiles:
	@cherrypy.expose
	def index(self,transaction_id=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(transaction_id,json)
		
	def GET(self,transaction_id=None,json=None):
		# Register the new transaction on the database
		sgfsDB=SGFSDB()
		infra_bdii,        \
		infra_lfc,         \
		app_lfcdir,        \
		user_name,         \
		transaction_proxy, \
		infra_pxvo = sgfsDB.getTransactionLFCData(transaction_id)
		# Create the LFC object
		lfc = LFC(infra_bdii,infra_lfc,app_lfcdir,user_name,transaction_proxy,infra_pxvo)
		# Get file list
		result, file_list=lfc.list()
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		if result == 0:
			answer_block=sgfsOutput.Answer(True)
			for file_rec in file_list.split('\n'):
				file_remark=''
				if len(file_rec) > 0:
					try:
						rec_items   = file_rec.split()
						file_name   = rec_items[8]
						file_size   = rec_items[4]
						file_flags  = rec_items[0]
						file_date   = '%s %s %s' % (rec_items[5],rec_items[6],rec_items[7])
						file_remark = ''
						if rec_items[9:] != []:
							for w in rec_items[9:]:
								file_remark ="%s %s" %(file_remark,w)
						file_remark = file_remark.strip()
					except:
						pass
					sgfsOutput.addValue(answer_block,'file',(('name',file_name),('size',file_size),('flags',file_flags),('date',file_date),('remark',file_remark),))
		else:
			answer_block=sgfsOutput.Answer(False)
			sgfsOutput.addBlockValue(answer_block,'error',file_list)
		return sgfsOutput.render('\t')

class delFile:
	@cherrypy.expose
	def index(self,transaction_id=None,lfc_file_name=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(transaction_id,lfc_file_name,json)
		
	def GET(self,transaction_id=None,lfc_file_name=None,json=None):
		# Register the new transaction on the database
		sgfsDB=SGFSDB()
		infra_bdii,        \
		infra_lfc,         \
		app_lfcdir,        \
		user_name,         \
		transaction_proxy, \
		infra_pxvo = sgfsDB.getTransactionLFCData(transaction_id)
		# Create the LFC object
		lfc = LFC(infra_bdii,infra_lfc,app_lfcdir,user_name,transaction_proxy,infra_pxvo)
		# Get file from storage
		lfn_name=lfc.rm(lfc_file_name)
		# Register the DELETE action on file
		sgfsDB.registerAction(transaction_id,1,lfn_name,'')
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		answer_block=sgfsOutput.Answer(True)
		sgfsOutput.addBlockValue(answer_block,"lfn_name",lfn_name)
		return sgfsOutput.render('\t')

class bookFile:
	@cherrypy.expose
	def index(self,transaction_id=None,lfc_file_name=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(transaction_id,lfc_file_name,json)
		
	def GET(self,transaction_id=None,lfc_file_name=None,json=None):
		# Retrieve LFC data from transaction
		sgfsDB=SGFSDB()
		infra_bdii,        \
		infra_lfc,         \
		app_lfcdir,        \
		user_name,         \
		transaction_proxy, \
		infra_pxvo = sgfsDB.getTransactionLFCData(transaction_id)
		# Create the LFC object
		lfc = LFC(infra_bdii,infra_lfc,app_lfcdir,user_name,transaction_proxy,infra_pxvo)
		# Get file from storage in background
		file_name, file_size, p=lfc.book(lfc_file_name)
		# Register the BOOKING action on file
		action_id = sgfsDB.registerAction(transaction_id,2,lfc_file_name,file_name)
		# Register the BOOKING
		booking_id = sgfsDB.registerBooking(action_id,transaction_id,file_size,p)
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		answer_block=sgfsOutput.Answer(True)
		sgfsOutput.addBlockValue(answer_block,"booking_id",booking_id)
		return sgfsOutput.render('\t')

class bookingsCheck:
	@cherrypy.expose
	def index(self,transaction_id=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(transaction_id,json)
		
	def GET(self,transaction_id=None,json=None):
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		# Retrieve LFC data from transaction
		sgfsDB=SGFSDB()
		bookings = sgfsDB.getBookings(transaction_id)
		answer_block=sgfsOutput.Answer(True)
		for book in bookings:
			booking_id         = book[0]
			file_size          = book[1]
			download_file_size = book[2]
			download_pid       = book[3]
			download_url       = book[4]
			action_id          = book[5]
			file_name          = os.path.basename(book[6])
			transaction_id     = book[7]
			try:
				new_download_file_size = os.path.getsize(book[6])
			except OSError:
				new_download_file_size = 0
			# Determine the download status if it does not exist
			# sets: download_file_size = -1
			# informing the client about the no more unexisting pid
			# (download failed).
			if new_download_file_size < file_size:
				cmd="ps -ef | grep sgfs | grep %s | grep lcg-cp | grep -v grep | awk '{ print $2 }'" % download_pid
				execCmd=ExecCmd()
				pid = execCmd.cmd(cmd)
 				if len(pid) == 0:
					print "Orphan pid: %s detected" % pid
					download_file_size=-1
					sgfsDB.closeBookings(transaction_id,(booking_id,))
					booking_file = sgfsDB.orphanBooking(booking_id,download_pid)
					try:
						os.unlink(booking_file)
					except OSError:
						print "EXCEPTION: unlink %s" % booking_file
			# Prepare the output
			sgfsOutput.addValue(answer_block,"booking", \
								(('booking_id',booking_id),\
								 ('file_size' ,file_size), \
								 ('download_file_size',new_download_file_size), \
								 ('action_id',action_id), \
								 ('file_name',file_name), \
								 ('transaction_id',transaction_id),
								 ('download_url',download_url),))
			if download_file_size < new_download_file_size:
				sgfsDB.updateBookingFileSize(booking_id,new_download_file_size)
			if download_file_size > 0 and download_url is None and new_download_file_size == file_size:
				sgfsDB.updateBookingUrl(booking_id,transaction_id)
		return sgfsOutput.render('\t')

class bookedDownload:
	@cherrypy.expose
	def index(self,transaction_id=None,booking_id=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(transaction_id,booking_id,json)
		
	def GET(self,transaction_id=None,booking_id=None,json=None):
		sgfsDB=SGFSDB()
		# Get file from action_id
		file_name=sgfsDB.getBookedFile(booking_id)
		# Register the DOWNLOAD_BOOKING action
		sgfsDB.registerAction(transaction_id,4,"%s"%booking_id,file_name)
		# Serve it to the client for download
		return serve_file(file_name, "application/x-download", "attachment")

class closeBookings:
	@cherrypy.expose
	def index(self,transaction_id=None,booking_ids=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(transaction_id,booking_ids,json)
		
	def GET(self,transaction_id=None,booking_ids=None,json=None):
		sgfsDB=SGFSDB()
		if booking_ids is not None:
			booking_info = sgfsDB.closeBookings(transaction_id,booking_ids.split(','))
		else:
			booking_info = sgfsDB.closeBookings(transaction_id,None)
		# Check and remove active downloads
		for booking_pid in booking_info[1]:
			cmd="ps -ef | grep sgfs | grep %s | grep lcg-cp | grep -v grep | awk '{ print $2 }'" % booking_pid
			execCmd=ExecCmd()
			pids = execCmd.cmd(cmd).split('\n')
			for pid in pids:
				if len(pid) > 0:
					print "Killing pid: %s" % pid
					os.kill(int(pid),signal.SIGKILL)
		# Update the database
		for booking_file in booking_info[0]:
			booking_file_dir=os.path.dirname(booking_file)
			print "Removing %s - %s" % (booking_file,booking_file_dir)
			try:
				os.unlink(booking_file)
			except OSError:
				print "EXCEPTION: unlink %s" % booking_file
			try:
				os.rmdir(booking_file_dir)
			except OSError:
				print "EXCEPTION: rmdir  %s" % booking_file_dir
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		answer_block=sgfsOutput.Answer(True)
		return sgfsOutput.render('\t')

class getSurl:
	@cherrypy.expose
	def index(self,transaction_id=None,lfc_file_name=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(transaction_id,lfc_file_name,json)
		
	def GET(self,transaction_id=None,lfc_file_name=None,json=None):
		sgfsDB=SGFSDB()
		infra_bdii,        \
		infra_lfc,         \
		app_lfcdir,        \
		user_name,         \
		transaction_proxy, \
		infra_pxvo = sgfsDB.getTransactionLFCData(transaction_id)
		# Create the LFC object
		lfc = LFC(infra_bdii,infra_lfc,app_lfcdir,user_name,transaction_proxy,infra_pxvo)
		#retrieve the SURL
		returnCode,cmd,surls = lfc.getSurls(lfc_file_name)
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		if (returnCode != 0):
			answer_block=sgfsOutput.Answer(False)
			sgfsOutput.addBlockValue(answer_block,'error',surls[0])
			sgfsOutput.addBlockValue(answer_block,'command',cmd)
		else:
			answer_block=sgfsOutput.Answer(True)
			surl_list=()
			for surl in surls:
				if surl != '':
					surl_list=surl_list+(('address', surl),)
					sgfsOutput.addValue(answer_block,'surl',surl_list)
		return sgfsOutput.render('\t')

class regSurl:
	@cherrypy.expose
	def index(self,transaction_id=None,surl=None,lfc_file_name=None,lfc_path=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(transaction_id,surl,lfc_file_name,lfc_path,json)
		
	def GET(self,transaction_id=None,surl=None,lfc_file_name=None,lfc_path=None,json=None):
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		answer_block=sgfsOutput.Answer(False)
		sgfsOutput.addBlockValue(answer_block,"message","This service works only in POST mode")
		return sgfsOutput.render('\t')	
	
	def POST(self,transaction_id=None,surl=None,lfc_file_name=None,lfc_path=None,json=None):
		sgfsDB=SGFSDB()
		infra_bdii,        \
		infra_lfc,         \
		app_lfcdir,        \
		user_name,         \
		transaction_proxy, \
		infra_pxvo = sgfsDB.getTransactionLFCData(transaction_id)
		# Create the LFC object
		lfc = LFC(infra_bdii,infra_lfc,app_lfcdir,user_name,transaction_proxy,infra_pxvo)
		#register SURL
		returnCode,cmd,guid,lfc_path = lfc.regSurl(surl,lfc_file_name,lfc_path)
		# register Action
		#sgfsDB.regSurl(surl,lfc_file_name,lfc_path)
		# SGFS Answer
		sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
		if (returnCode != 0):
			answer_block=sgfsOutput.Answer(False)
			sgfsOutput.addBlockValue(answer_block,"error",guid)
			sgfsOutput.addBlockValue(answer_block,"command",cmd)
		else:
			answer_block=sgfsOutput.Answer(True)
			sgfsOutput.addBlockValue(answer_block,"guid",guid)
			sgfsOutput.addBlockValue(answer_block,"lfc_path",lfc_path)
		return sgfsOutput.render('\t')

class getFile:
	gfal_f    = None
	action_id = None
	
	def __del__(self):
		if self.gfal_f is not None:
			print "[%s] Closing interrupted transfer gfal file descriptor" % self.action_id
			gfalthr.gfal_close(self.gfal_f)
	
	@cherrypy.expose
	def index(self,transaction_id=None,lfc_file_name=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(transaction_id,lfc_file_name,json)
	index._cp_config = {'response.stream': True}

	def GET(self,transaction_id=None,lfc_file_name=None,json=None):
		# Register the new transaction on the database
		sgfsDB=SGFSDB()
		infra_bdii,        \
		infra_lfc,         \
		app_lfcdir,        \
		user_name,         \
		transaction_proxy, \
		infra_pxvo = sgfsDB.getTransactionLFCData(transaction_id)
		# Create the LFC object
		lfc = LFC(infra_bdii,infra_lfc,app_lfcdir,user_name,transaction_proxy,infra_pxvo)
		# Get file from storage
		returnCode,cmd,file_size,file_name=lfc.file_data(lfc_file_name)
		# Register the DOWNLOAD  action on file
		if returnCode == 0:
			self.action_id = sgfsDB.registerAction(transaction_id,0,lfc_file_name,file_name)
			# Serve it to the client for download
			cherrypy.response.headers['Content-Type'       ] = 'application/x-download'
			cherrypy.response.headers['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(file_name)
			cherrypy.response.headers['Content-Length'     ] = '%s'                        % file_size
			cherrypy.response.headers['Cache-Control'      ] = 'no-cache, must-revalidate'
			cherrypy.response.headers['Pragma'             ] = 'no-cache'
			return self.content(transaction_proxy,infra_bdii,infra_lfc,file_name,int(file_size))
		else:
			# SGFS Answer
			sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
			answer_block=sgfsOutput.Answer(False)
			sgfsOutput.addBlockValue(answer_block,"error",file_name)
			sgfsOutput.addBlockValue(answer_block,"command",cmd)
			return sgfsOutput.render('\t')
	
	def content(self,px_file,bdii_host,lfc_host,lfc_file_name,file_size):
		print """
--------------------------------------\n
[%s] GFAL transfer \n
	proxy: '%s'
	bdii : '%s'
	lfc  : '%s'
	file : 'lfn:%s'
	size : %s bytes\n
--------------------------------------""" % (self.action_id,px_file,bdii_host,lfc_host,lfc_file_name,file_size)
		os.environ["X509_USER_PROXY"] = px_file
		os.environ["LCG_GFAL_INFOSYS"] = bdii_host
		os.environ["LFC_HOST"]=lfc_host
		transfer_size=0
		buffer_size=8*1024
		self.gfal_f=gfalthr.gfal_open("lfn:%s" % lfc_file_name,os.O_RDONLY,0755)
		while transfer_size < file_size:
			buffer_data = gfalthr.gfal_read(self.gfal_f,buffer_size)
			if buffer_data[1] is not None:
				block_size=buffer_data[0]
				transfer_size+=block_size
				if transfer_size == file_size:
					#print "transfer: %s/%s (%s) " % (transfer_size,file_size,block_size)
					gfalthr.gfal_close(self.gfal_f)
					self.gfal_f = None
					print "[%s] transfer: (done)" % self.action_id
				#else:
					#print "transfer: %s/%s (%s) " % (transfer_size,file_size,block_size)
				yield buffer_data[1][:buffer_size]
			else:
				print "[%s] Unable to download ..." % self.action_id
				return

#
# FileTransfer class (used to handle file streams)
# 
# Two variants: 1st File System based
#               2nd GFAL based
#
class SGFS_FileTransfer:
	sgfsDB         = None
	gfal_f         = None
	fs_f           = None
	action_id      = None
	transaction_id = None
	action_id      = None
	px_file        = None
	bdii_host      = None
	lfc_host       = None
	lfc_file_name  = None
	file_size      = None
	file_name      = None
	transferCmd    = None
	transfer_size  = None

	#def __init__(self):
	#	

	def __del__(self):
		if self.gfal_f is not None:
			print "[%s-%s] Closing interrupted transfer gfal file descriptor at (%s/%s)" % (self.transaction_id,self.action_id,self.transfer_size,self.file_size)
			gfalthr.gfal_close(self.gfal_f)
		if self.fs_f is not None:
			print "[%s-%s] Closing interrupted transfer fs file descriptor at (%s/%s)" % (self.transaction_id,self.action_id,self.transfer_size,self.file_size)
			if self.transferCmd is not None:
				self.transferCmd.killAll()
			self.fs_f.close()
			self.closeTransaction()
			self.deleteFile()

	def gfalTransfer(self,transaction_id,action_id,px_file,bdii_host,lfc_host,lfc_file_name,file_size):
		self.transaction_id = transaction_id
		self.action_id      = action_id
		self.px_file        = px_file
		self.bdii_host      = bdii_host
		self.lfc_host       = lfc_host
		self.lfc_file_name  = lfc_file_name
		self.file_size      = file_size
		print """
--------------------------------------\n
[%s] GFAL transfer \n
	proxy: '%s'
	bdii : '%s'
	lfc  : '%s'
	file : 'lfn:%s'
	size : %s bytes\n
--------------------------------------""" % (self.action_id,self.px_file,self.bdii_host,self.lfc_host,self.lfc_file_name,self.file_size)
		os.environ["X509_USER_PROXY"] = px_file
		os.environ["LCG_GFAL_INFOSYS"] = bdii_host
		os.environ["LFC_HOST"]=lfc_host
		self.transfer_size=0
		buffer_size=8*1024
		self.gfal_f=gfalthr.gfal_open("lfn:%s" % self.lfc_file_name,os.O_RDONLY,0755)
		if self.gfal_f > 0:
			while self.transfer_size < self.file_size:
				buffer_data = gfalthr.gfal_read(self.gfal_f,buffer_size)
				if buffer_data[1] is not None:
					block_size=buffer_data[0]
					self.transfer_size+=block_size
					if self.transfer_size == self.file_size:
						#print "[%s] transfer: %s/%s (%s) " % (self.action_id,self.transfer_size,file_size,block_size)
						print "[%s] transfer: (done)" % self.action_id
						gfalthr.gfal_close(self.gfal_f)
						self.gfal_f = None
				#	else:
						#print "[%s] transfer: %s/%s (%s) " % (self.action_id,self.transfer_size,file_size,block_size)
				yield buffer_data[1][:buffer_size]
			else:
				print "[%s] Unable to download file '%s'" % (self.action_id,lfc_file_name)
				return
	
	def fsTransfer(self,transaction_id,action_id,file_name,file_size):
		self.action_id      = action_id
		self.transaction_id = transaction_id
		self.file_name      = file_name
		self.file_size      = file_size
		print """
--------------------------------------------\n
[%s-%s] Transfer file: '%s' (%s) bytes\n
--------------------------------------------""" % (self.transaction_id,self.action_id,self.file_name,self.file_size)
		self.transfer_size=0
		buffer_size=16*1024
		try:
			self.fs_f=open(self.file_name,'r')
			while self.transfer_size < self.file_size:
				buffer_data=self.fs_f.read(buffer_size)
				block_size=len(buffer_data)
				self.transfer_size+=block_size
				if block_size > 0:
					#print "[%s] transfer: %s/%s (%s) " % (self.action_id,self.transfer_size,file_size,block_size)
					if self.transfer_size == self.file_size:
						print "[%s-%s] transfer: (done)" % (self.transaction_id,self.action_id)
						self.fs_f.close()
						self.fs_f=None
						self.deleteFile()
						self.closeTransaction()
					yield buffer_data[:block_size]
		except IOError, (errno, strerror):
			# SGFS Answer
			sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
			answer_block=sgfsOutput.Answer(False)
			sgfsOutput.addBlockValue(answer_block,"error","I/O-Error (%s): %s" % (errno, strerror))
			yield sgfsOutput.render('\t')

	def closeTransaction(self):
		sgfs_DB=SGFSDB()
		tmpfile=sgfs_DB.closeTransaction(self.transaction_id)
		print "[%s-%s] Removing proxy file - %s" % (self.transaction_id,self.action_id,tmpfile)
		try:
			os.unlink(tmpfile)
		except OSError:
			print "[%s-%s] EXCEPTION: unlink %s" % (self.transaction_id.self.action_id,self.action_id,tmpfile)

	def deleteFile(self):
		file_dir=os.path.dirname(self.file_name)
		try:
			os.unlink(self.file_name)
		except OSError:
			print "[%s-%s] EXCEPTION: unlink %s" % (self.transaction_id,self.action_id,self.file_name)
		try:
			os.rmdir(file_dir)
		except OSError:
			print "[%s-%s] EXCEPTION: rmdir  %s" % (self.transaction_id,self.action_id,action_dir)

	def getTransferCmd(self):
		self.transferCmd = ExecCmd()
		return self.transferCmd
#
# SGFS_FileTransfer (end) 
#


class fixedDownload:
	@cherrypy.expose
	def index(self,file_guid=None,json=None):
		http_method = getattr(self,cherrypy.request.method)
		return (http_method)(file_guid,json)
	index._cp_config = {'response.stream': True}

	def GET(self,file_guid=None,json=None):
		# Register the new transaction on the database
		sgfsDB=SGFSDB()
		# 
		# Retrieve from the db the transaction keys (user_name,application_name)
		# used to identify the infrastructure and the other download parameters
		#
		user_name,         \
		application_name,  \
		lfc_file_name,     \
		lfc_absolute_path, \
		date_from,         \
		date_to,           \
		down_count = sgfsDB.downloadInfo(file_guid)
		if lfc_file_name == '':
			# File not found!
			sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
			answer_block=sgfsOutput.Answer(False)
			sgfsOutput.addBlockValue(answer_block,"error","The requested file does not exist")
			return sgfsOutput.render('\t')
		print """
-[Transfer]------------\n\
 user_name        : %s \n\
 application_name : %s \n\
 lfc_file_name    : %s \n\
 lfc_absolute_path: %s \n\
 date_from        : %s \n\
 date_to          : %s \n\
 down_count       : %s \n\
--------------------------\n""" % (user_name,application_name,lfc_file_name,lfc_absolute_path,date_from,date_to,down_count)
		# Begin transaction
		transaction_id=sgfsDB.registerTransaction(user_name,application_name)
		# Associate a proxy to the new transaction
		tmpfile = tempfile.mktemp()
		infra_id=sgfsDB.getInfrastructureId(transaction_id)
		infrastructure=Infrastructure(infra_id)
		infrastructure.getProxy(tmpfile)
		sgfsDB.storeTransactionProxy(transaction_id,tmpfile)
		# Now I can download the requested file still form db
		infra_bdii,        \
		infra_lfc,         \
		app_lfcdir,        \
		user_name,         \
		transaction_proxy, \
		infra_pxvo = sgfsDB.getTransactionLFCData(transaction_id)
		# Create the LFC object
		lfc = LFC(infra_bdii,infra_lfc,app_lfcdir,user_name,transaction_proxy,infra_pxvo)
		# Instanciate the lcg-cp execute cmd
		fileTransfer=SGFS_FileTransfer()
		lcgCpCmd=fileTransfer.getTransferCmd()
		# Get file from storage
		returnCode,cmd,file_size,file_name=lfc.file(lcgCpCmd,lfc_file_name,lfc_absolute_path=True)
		if returnCode == 0:
			action_id = sgfsDB.registerAction(transaction_id,6,lfc_file_name,file_name)
			# Serve it to the client for download
			cherrypy.response.headers['Content-Type'       ] = 'application/x-download'
			cherrypy.response.headers['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(lfc_file_name)
			cherrypy.response.headers['Content-Length'     ] = '%s'                        % file_size
			cherrypy.response.headers['Cache-Control'      ] = 'no-cache, must-revalidate'
			cherrypy.response.headers['Pragma'             ] = 'no-cache'
			
			return fileTransfer.fsTransfer(transaction_id,action_id,file_name,int(file_size))
		else:
			# SGFS Answer
			sgfsOutput=SGFSOutput(SGFSOutput.jsonMode(json))
			answer_block=sgfsOutput.Answer(False)
			sgfsOutput.addBlockValue(answer_block,"error",file_name)
			sgfsOutput.addBlockValue(answer_block,"command",cmd)
			return sgfsOutput.render('\t')
		# What about the opened transaction?

##
## The application services will be defined here ...
##
def get_app():
	root=Root()
	root.begin=beginTransaction()
	root.end=endTransaction()
	root.list=listTransactionFiles()
	root.download=getFile()
	root.delete=delFile()
	root.json=JSONTester()
	root.book=bookFile() 
	root.bookings=bookingsCheck()
	root.async_download=bookedDownload()
	root.close_bookings=closeBookings()
	root.surl=getSurl()
	root.register_surl=regSurl()
	root.fixed_download=fixedDownload()
	return cherrypy.tree.mount(root) 

##
## SGFS WSGI startup
##
def get_wsgi_app(): 
	app = get_app() 
	config = { 
		"environment"         : "embedded", 
		"log.screen"          :      False, 
		"show_tracebacks"     :      False, 
		"engine.autoreload_on":       True, 
	} 
	cherrypy.config.update(config) 
	return app 

##
## SGFS Standalone startup
##
def main():
	app = get_app() 
	config = { 
		'server.socket_host': '0.0.0.0',
		'server.socket_port':      8088,
		'response.timeout'  :   1000000,
		'response.stream'   :      True
	} 
	cherrypy.config.update(config) 
	cherrypy.quickstart(app) 

if __name__ == "__main__":
	main() 

