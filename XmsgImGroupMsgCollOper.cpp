/*
  Copyright 2019 www.dev5.cn, Inc. dev5@qq.com
 
  This file is part of X-MSG-IM.
 
  X-MSG-IM is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  X-MSG-IM is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
 
  You should have received a copy of the GNU Affero General Public License
  along with X-MSG-IM.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <libmisc-mysql-c.h>
#include "XmsgImGroupMsgCollOper.h"
#include "XmsgImGroupDb.h"

XmsgImGroupMsgCollOper* XmsgImGroupMsgCollOper::inst = new XmsgImGroupMsgCollOper();

XmsgImGroupMsgCollOper::XmsgImGroupMsgCollOper()
{

}

XmsgImGroupMsgCollOper* XmsgImGroupMsgCollOper::instance()
{
	return XmsgImGroupMsgCollOper::inst;
}

bool XmsgImGroupMsgCollOper::loadLatestMsg(SptrCgt cgt, int latest, list<shared_ptr<XmsgImGroupMsgNotice>>& latestMsg)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, cgt: %s", cgt->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s where gcgt = '%s' order by msgId desc limit %d", XmsgImGroupDb::xmsgImGroupMsgColl.c_str(), cgt->toString().c_str(), latest)
	bool ret = MysqlMisc::query(conn, sql, [&latestMsg](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImGroupDb::xmsgImGroupMsgColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			return true;
		}
		auto coll = XmsgImGroupMsgCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s",XmsgImGroupDb::xmsgImGroupMsgColl.c_str(), row->toString().c_str())
			return false; 
		}
		shared_ptr<XmsgImGroupMsgNotice> notice(new XmsgImGroupMsgNotice());
		notice->set_gcgt(coll->gcgt->toString());
		notice->set_scgt(coll->scgt->toString());
		notice->set_msgid(coll->msgId);
		notice->mutable_msg()->CopyFrom(*coll->msg);
		notice->set_gts(coll->gts);
		latestMsg.push_back(notice);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImGroupMsgCollOper::queryMsgByPage(SptrCgt cgt, ullong msgId , bool before , int pageSize , list<shared_ptr<XmsgImGroupMsgColl>>& msg)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, cgt: %s", cgt->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s where gcgt = '%s' and msgId %s %llu order by msgId %s limit %d", 
			XmsgImGroupDb::xmsgImGroupMsgColl.c_str(),
			cgt->toString().c_str(),
			before ? "<" : ">",
			msgId,
			before ? "desc" : "asc",
			pageSize)
	bool ret = MysqlMisc::query(conn, sql, [&msg](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImGroupDb::xmsgImGroupMsgColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImGroupDb::xmsgImGroupMsgColl.c_str())
			return true;
		}
		auto coll = XmsgImGroupMsgCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s",XmsgImGroupDb::xmsgImGroupMsgColl.c_str(), row->toString().c_str())
			return false; 
		}
		msg.push_back(coll);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

void XmsgImGroupMsgCollOper::saveMsg(SptrGroupMsg msg, function<void(int ret, const string& desc)> cb)
{
	shared_ptr<x_msg_group_msg> xmgm(new x_msg_group_msg());
	xmgm->msg = msg;
	xmgm->cb = cb;
	unique_lock<mutex> lock(this->lock4msgQueue);
	bool nll = this->msgQueue.empty();
	this->msgQueue.push(xmgm);
	if (nll)
		this->cond4msgQueue.notify_one(); 
}

void XmsgImGroupMsgCollOper::loop()
{
	list<shared_ptr<x_msg_group_msg>> lis;
	unique_lock<mutex> lock(this->lock4msgQueue);
	while (!this->msgQueue.empty())
	{
		auto xmgm = this->msgQueue.front();
		this->msgQueue.pop();
		lis.push_back(xmgm);
		if (lis.size() < XmsgImGroupCfg::instance()->cfgPb->misc().groupmsgsavebatchsize())
			continue;
		break;
	}
	if (lis.empty())
	{
		this->cond4msgQueue.wait(lock);
		return;
	}
	lock.unlock();
	ullong sts = DateMisc::dida();
	int ret = 0;
	string desc;
	this->insertBatch(lis, ret, desc);
	for (auto& it : lis)
		it->cb(ret, desc);
	LOG_DEBUG("insert batch size: %zu, elap: %dms, ret: %d, desc: %s", lis.size(), DateMisc::elapDida(sts), ret, desc.c_str())
}

void XmsgImGroupMsgCollOper::insertBatch(const list<shared_ptr<x_msg_group_msg>>& lis, int& ret, string& desc)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		ret = RET_EXCEPTION;
		desc = "can not get connection from pool";
		LOG_ERROR("can not get connection from pool, size: %zu", lis.size())
		return;
	}
	if (!MysqlMisc::start(conn))
	{
		ret = RET_EXCEPTION;
		desc = "start transaction failed";
		LOG_ERROR("start transaction failed, err: %s, size: %zu", ::mysql_error(conn), lis.size())
		MysqlConnPool::instance()->relConn(conn, false);
		return;
	}
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?)", XmsgImGroupDb::xmsgImGroupMsgColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	for (auto& it : lis)
	{
		req->addRow() 
		->addVarchar(it->msg->gcgt->toString()) 
		->addVarchar(it->msg->scgt->toString()) 
		->addLong(it->msg->msgId) 
		->addLong(it->msg->localMsgId) 
		->addBlob(it->msg->msg->SerializeAsString()) 
		->addDateTime(it->msg->gts);
	}
	int affected = 0;
	if (!MysqlMisc::sql(conn, req, ret, desc, &affected))
	{
		ret = RET_EXCEPTION;
		desc = "insert batch failed";
		LOG_ERROR("insert many into %s failed, err: %s, size: %zu", XmsgImGroupDb::xmsgImGroupMsgColl.c_str(), desc.c_str(), lis.size())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return;
	}
	if (affected != (int) lis.size())
	{
		LOG_FAULT("it`s a bug, affected: %d, size: %zu", affected, lis.size())
	}
	if (!MysqlMisc::commit(conn))
	{
		ret = RET_EXCEPTION;
		desc = "commit transaction failed";
		LOG_ERROR("commit transaction failed, err: %s, size: %zu", ::mysql_error(conn), lis.size())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return;
	}
	MysqlConnPool::instance()->relConn(conn);
}

shared_ptr<XmsgImGroupMsgColl> XmsgImGroupMsgCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	string str;
	if (!row->getStr("gcgt", str))
	{
		LOG_ERROR("can not found field: gcgt")
		return nullptr;
	}
	auto gcgt = ChannelGlobalTitle::parse(str);
	if (gcgt == nullptr)
	{
		LOG_ERROR("gcgt format error: %s", str.c_str())
		return nullptr;
	}
	if (!row->getStr("scgt", str))
	{
		LOG_ERROR("can not found field: scgt")
		return nullptr;
	}
	auto scgt = ChannelGlobalTitle::parse(str);
	if (scgt == nullptr)
	{
		LOG_ERROR("scgt format error: %s", str.c_str())
		return nullptr;
	}
	ullong msgId;
	if (!row->getLong("msgId", msgId))
	{
		LOG_ERROR("can not found field: msgId, gcgt: %s, scgt: %s", gcgt->toString().c_str(), scgt->toString().c_str())
		return nullptr;
	}
	ullong localMsgId;
	if (!row->getLong("localMsgId", localMsgId))
	{
		LOG_ERROR("can not found field: localMsgId, gcgt: %s, scgt: %s", gcgt->toString().c_str(), scgt->toString().c_str())
		return nullptr;
	}
	if (!row->getBin("msg", str))
	{
		LOG_ERROR("can not found field: info, gcgt: %s, scgt: %s", gcgt->toString().c_str(), scgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgImMsg> msg(new XmsgImMsg());
	if (!msg->ParseFromString(str))
	{
		LOG_ERROR("info format error, gcgt: %s, scgt: %s", gcgt->toString().c_str(), scgt->toString().c_str())
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts, gcgt: %s, scgt: %s", gcgt->toString().c_str(), scgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgImGroupMsgColl> coll(new XmsgImGroupMsgColl());
	coll->gcgt = gcgt;
	coll->scgt = scgt;
	coll->msgId = msgId;
	coll->localMsgId = localMsgId;
	coll->msg = msg;
	coll->gts = gts;
	return coll;
}

XmsgImGroupMsgCollOper::~XmsgImGroupMsgCollOper()
{

}

