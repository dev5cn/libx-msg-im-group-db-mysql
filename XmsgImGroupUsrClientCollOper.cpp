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
#include "XmsgImGroupUsrClientCollOper.h"
#include "XmsgImGroupDb.h"

XmsgImGroupUsrClientCollOper* XmsgImGroupUsrClientCollOper::inst = new XmsgImGroupUsrClientCollOper();

XmsgImGroupUsrClientCollOper::XmsgImGroupUsrClientCollOper()
{

}

XmsgImGroupUsrClientCollOper* XmsgImGroupUsrClientCollOper::instance()
{
	return XmsgImGroupUsrClientCollOper::inst;
}

void XmsgImGroupUsrClientCollOper::init()
{
	XmsgImClientLocalMgr::instance()->setAddUsrClientCb([](SptrClientLocal client, function<void(int ret, const string& desc)> cb)
	{
		XmsgImGroupDb::instance()->future([client, cb]
				{
					shared_ptr<XmsgImGroupUsrClientColl> coll(new XmsgImGroupUsrClientColl());
					coll->cgt = client->cgt;
					coll->plat = client->plat;
					coll->did = client->did;
					coll->enable = true;
					coll->gts = DateMisc::nowGmt0();
					coll->uts = coll->gts;
					int ret = XmsgImGroupUsrClientCollOper::instance()->insert(coll) ? RET_SUCCESS : RET_EXCEPTION;
					string desc = ret == RET_SUCCESS ? "" : "may be database exception";
					cb(ret, desc);
				}, client->cgt);
	});
}

bool XmsgImGroupUsrClientCollOper::load(SptrCgt cgt, list<shared_ptr<XmsgImGroupUsrClientColl>>& client)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, cgt: %s", cgt->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s where cgt = '%s'", XmsgImGroupDb::xmsgImGroupUsrClientColl.c_str(), cgt->toString().c_str())
	bool ret = MysqlMisc::query(conn, sql, [&client](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImGroupDb::xmsgImGroupUsrClientColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			return true;
		}
		auto coll = XmsgImGroupUsrClientCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s",XmsgImGroupDb::xmsgImGroupUsrClientColl.c_str(), row->toString().c_str())
			return false; 
		}
		client.push_back(coll);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImGroupUsrClientCollOper::insert(shared_ptr<XmsgImGroupUsrClientColl> coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, coll: %s", coll->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?)", XmsgImGroupDb::xmsgImGroupUsrClientColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->cgt->toString()) 
	->addVarchar(coll->plat) 
	->addVarchar(coll->did) 
	->addBool(coll->enable) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->uts);
	bool ret = MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupUsrClientColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupUsrClientColl.c_str(), coll->toString().c_str())
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

shared_ptr<XmsgImGroupUsrClientColl> XmsgImGroupUsrClientCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	string str;
	if (!row->getStr("cgt", str))
	{
		LOG_ERROR("can not found field: cgt")
		return nullptr;
	}
	SptrCgt cgt = ChannelGlobalTitle::parse(str);
	if (cgt == nullptr)
	{
		LOG_ERROR("cgt format error: %s", str.c_str())
		return nullptr;
	}
	string plat;
	if (!row->getStr("plat", plat))
	{
		LOG_ERROR("can not found field: plat")
		return nullptr;
	}
	string did;
	if (!row->getStr("did", did))
	{
		LOG_ERROR("can not found field: did")
		return nullptr;
	}
	bool enable;
	if (!row->getBool("enable", enable))
	{
		LOG_ERROR("can not found field: enable")
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts")
		return nullptr;
	}
	ullong uts;
	if (!row->getLong("uts", uts))
	{
		LOG_ERROR("can not found field: uts")
		return nullptr;
	}
	shared_ptr<XmsgImGroupUsrClientColl> coll(new XmsgImGroupUsrClientColl());
	coll->cgt = cgt;
	coll->plat = plat;
	coll->did = did;
	coll->enable = enable;
	coll->gts = gts;
	coll->uts = uts;
	return coll;
}

XmsgImGroupUsrClientCollOper::~XmsgImGroupUsrClientCollOper()
{

}

