{\rtf1\ansi\ansicpg1252\cocoartf2513
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww10800\viewh8400\viewkind0
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs24 \cf0 Arquivo: cash_ins.json\
	  \
CREATE TABLE cash_ins (\
transaction_id varchar(50) primary key,\
account_id varchar(50),\
counterparty_account_id varchar(50),\
amount numeric,\
inserted_at timestamp);\
\
Arquivo: cash_outs_created.json\
\
CREATE TABLE cash_outs_created (\
transaction_id varchar(50) primary key,\
account_id varchar(50),\
counterparty_account_id varchar(50),\
amount numeric,\
inserted_at timestamp)\
\
Arquivo: cash_outs_processed.json\
\
CREATE TABLE cash_outs_processed (\
transaction_id varchar(50) primary key,\
inserted_at timestamp)\
\
Arquivo: cash_outs_refunded.json]\
\
CREATE TABLE cash_outs_refunded (\
transaction_id varchar(50) primary key,\
inserted_at timestamp)}