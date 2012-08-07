--
-- mkdatabase.sql
--
-- Script that creates the SGFS server database
--
-- execute as 'root' user
--  mysql -u root -p < mkdatabase.sql
--
-- (!) Pay attention the current script removes the existing database
--     in case you need keep old data, please save your data first
--     and then migrate your old data to the new database
--
-- Copyright (c) 2011:
-- Istituto Nazionale di Fisica Nucleare (INFN), Italy
-- Consorzio COMETA (COMETA), Italy
-- 
-- See http://www.infn.it and and http://www.consorzio-cometa.it for details on
-- the copyright holders.
--
-- Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- 
-- Author: Riccardo Bruno (riccardo.bruno@ct.infn.it)
--
drop database if exists sgfs;

create database sgfs;
grant all on sgfs.* TO 'sgfs_user'@'%' IDENTIFIED BY "sgfs_password";
grant all on sgfs.* TO 'sgfs_user'@'localhost' IDENTIFIED BY "sgfs_password";
flush privileges;
use sgfs;

--
-- Infrastructure table - Science Gateway enabled infrastructure
--
create table sgfs_infrastructures (
	 infra_id        int unsigned not null auto_increment
	,infra_name      varchar(128)
	,infra_desc      varchar(256)
	,infra_pxhost    varchar(128)
	,infra_pxport    varchar(128)
	,infra_pxid      int unsigned not null
	,infra_pxvo      varchar(128)
	,infra_pxrole    varchar(128)
	,infra_pxrenewal bool default true
	,infra_bdii      varchar(128)
	,infra_lfc       varchar(128)
	
	,primary key (infra_id)
);

--
-- Infrastructures must be defined before
--
insert into sgfs_infrastructures (infra_id,infra_name,infra_desc,infra_pxhost,infra_pxport,infra_pxid,infra_pxvo,infra_pxrole,infra_pxrenewal,infra_bdii,infra_lfc)  values (1,'eumed','Mediterranean Grid Infrastructure','myproxy.ct.infn.it',8082,24272,'eumed','eumed',1,'bdii.eumedgrid.eu:2170','lfc.ulakbim.gov.tr');
insert into sgfs_infrastructures (infra_id,infra_name,infra_desc,infra_pxhost,infra_pxport,infra_pxid,infra_pxvo,infra_pxrole,infra_pxrenewal,infra_bdii,infra_lfc)  values (2,'gisela','Latin American Grid Infrastructure','myproxy.ct.infn.it',8082,21873,'prod.vo.eu-eela.eu','prod.vo.eu-eela.eu',1,'prod.vo.eu-eela.eu','<unknown>');
insert into sgfs_infrastructures (infra_id,infra_name,infra_desc,infra_pxhost,infra_pxport,infra_pxid,infra_pxvo,infra_pxrole,infra_pxrenewal,infra_bdii,infra_lfc)  values (3,'earthserver','EarthServer Grid Infrastructure','myproxy.ct.infn.it',8082,22353,'vo.earthserver.eu','vo.earthserver.eu',1,'infn-bdii-01.ct.pi2s2.it:2170','lfc-01.ct.trigrid.it');

--
-- Users table - Science Gateway allowed users
--
create table sgfs_users (
	 user_id         int unsigned not null auto_increment
	,user_name       varchar(128) not null

	,primary key (user_id)
);

--
-- Users must be defined before
--
insert into sgfs_users (user_id,user_name) values (1,'brunor');
insert into sgfs_users (user_id,user_name) values (2,'alashhab');

--
-- Application table - Science Gateway allowed applications
--
create table sgfs_applications (
	 app_id         int unsigned not null auto_increment
	,app_name       varchar(128) not null
	,app_desc       varchar(256) not null
	,app_lfcdir     varchar(128) not null
	,infra_id       int unsigned not null

	,primary key (app_id)
	,foreign key (infra_id) references sgfs_infrastructures(infra_id)
);

--
-- Applicatins must be defined before
--
insert into sgfs_applications (app_id,app_name,app_lfcdir,app_desc,infra_id) values (1,'Tester application','test','Just a tester application',1);
insert into sgfs_applications (app_id,app_name,app_lfcdir,app_desc,infra_id) values (2,'CMSquares','cmsquares','Counting Magic Squares of 6th order',1);
insert into sgfs_applications (app_id,app_name,app_lfcdir,app_desc,infra_id) values (3,'Mars data output viewer','mars_data/output','Mars data output viewver',3);
insert into sgfs_applications (app_id,app_name,app_lfcdir,app_desc,infra_id) values (4,'GILDA Liferay VM (WN)','gildavm','Gilda Liferay Virtual Machine (WN)',1);

--
-- Transatcions table
--
create table sgfs_transactions (
	 transaction_id    int unsigned not null auto_increment
--	,token             varchar(32) not null
	,user_id           int unsigned not null
	,app_id            int unsigned not null
	,infra_id          int unsigned not null
	,transaction_proxy varchar(256)
	,transaction_from  datetime  not null
	,transaction_to    datetime
	,transaction_ip    varchar(32)

	,primary key (transaction_id)
	,foreign key (user_id) references sgfs_users(user_id)
	,foreign key (app_id)  references sgjp_applications(app_id)
	,foreign key (infra_id) references sgfs_infrastructures(infra_id)
);


--
-- Actions table - Each action done on the files during a transaction must to be logged
--
-- Possible actions are:
--    0 - 'DOWNLOAD'
--    1 - 'DELETE'
--    2 - 'OPEN_BOOKING'
--    3 - 'CLOSED_BOOKING'
--    4 - 'DOWNLOAD_BOOKING' (Uses lfc_file_name to store booking_id)
--    5 - 'ORPHAN_BOOKING'   (download pid was no more detected before download completion)
--    6 - 'FIXED_DOWNLOAD'   (
--
create table sgfs_actions (
	 action_id        int unsigned not null auto_increment
	,transaction_id   int unsigned not null
	,action_ts        datetime  not null
--	,token            varchar(32) not null
	,action           int unsigned not null
	,lfc_file_name    varchar(256)
	,file_name        varchar(256)
	,action_ip        varchar(32)
	
	,primary key (action_id)
	,foreign key (transaction_id) references sgfs_transactions(transaction_id)
);

--
-- Booking table - Files can be downloaded by the server for a future user access
--
create table sgfs_bookings (
	 booking_id         int unsigned not null auto_increment
	,action_id          int unsigned not null
	,transaction_id     int unsigned not null
	,file_size          int unsigned
	,download_file_size int unsigned
	,download_pid       int unsigned
	,download_url       varchar(1024)
	,booking_ip         varchar(32)
	
	,primary key (booking_id)
	,foreign key (action_id) references sgfs_actions(action_id)
	,foreign key (transaction_id) references sgfs_transactions(transaction_id)
);

--
-- Downloads - LFC catalogued files can be downloaded via fixed URLs
--   guid       points to the LFC' GUID field; used to identify the download
--   key        (user_id,app_id) identifies the infrastructure
--   file_name  can be referenced relatively or via absolute path
--   abs_path   tells to use a relative o absolute file path
--   date_from  validity date (from)
--   date_to    validity to   (NULL = forever)
--   down_count a limited number of download (NULL = not used)
--
create table sgfs_downloads (
	 guid           varchar( 64) not null
	,user_id        varchar(128) not null
	,app_id         varchar(128) not null
	,file_name      varchar(512) not null
	,abs_path       bool default false
	,date_from      datetime
	,date_to        datetime
	,down_count     int unsigned
	
	,primary key (guid)
	,foreign key (user_id) references sgfs_users(user_id)
	,foreign key (app_id)  references sgfs_applications(app_id)
);

insert into sgfs_downloads (guid,user_id,app_id,file_name,abs_path,date_from,date_to,down_count) values ('d2182266-03c2-4bcd-9201-d57a59d3564e',1,4,'/grid/eumed/sgfs/gildavm/GILDA_VM_Liferay_SL5.5_x86_64_v1.4p5_WN.tar.gz',TRUE,now(),NULL,NULL);