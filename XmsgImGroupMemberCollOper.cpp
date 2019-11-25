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
#include "XmsgImGroupDb.h"
#include "XmsgImGroupMemberCollOper.h"
#include "XmsgImGroupUsrClientCollOper.h"

XmsgImGroupMemberCollOper* XmsgImGroupMemberCollOper::inst = new XmsgImGroupMemberCollOper();

XmsgImGroupMemberCollOper::XmsgImGroupMemberCollOper()
{

}

XmsgImGroupMemberCollOper* XmsgImGroupMemberCollOper::instance()
{
	return XmsgImGroupMemberCollOper::inst;
}

bool XmsgImGroupMemberCollOper::insert(shared_ptr<XmsgImGroupMemberColl> coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	bool ret = this->insert(conn, coll);
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImGroupMemberCollOper::insert(void* conn, shared_ptr<XmsgImGroupMemberColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?, ?)", XmsgImGroupDb::xmsgImGroupMemberColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->mcgt->toString()) 
	->addVarchar(coll->gcgt->toString()) 
	->addBool(coll->enable) 
	->addLong(coll->latestReadMsgId) 
	->addLong(coll->ver) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->uts);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupMemberColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupMemberColl.c_str(), coll->toString().c_str())
	});
}

bool XmsgImGroupMemberCollOper::updateLatestReadMsgId(SptrCgt gcgt, SptrCgt mcgt, ullong latestReadMsgId, ullong uts)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, mcgt: %s, gcgt: %s, latestReadMsgId: %llu", mcgt->toString().c_str(), gcgt->toString().c_str(), latestReadMsgId)
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "update %s set latestReadMsgId = ?, uts = ? where mcgt = ? and gcgt = ?", XmsgImGroupDb::xmsgImGroupMemberColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addLong(latestReadMsgId) 
	->addDateTime(uts) 
	->addVarchar(mcgt->toString()) 
	->addVarchar(gcgt->toString());
	int ret = 0;
	string desc;
	int affected;
	if (!MysqlMisc::sql(conn, req, ret, desc, &affected))
	{
		LOG_ERROR("update %s failed, mcgt: %s, gcgt: %s, latestReadMsgId: %llu, err: %s", XmsgImGroupDb::xmsgImGroupMemberColl.c_str(), mcgt->toString().c_str(), gcgt->toString().c_str(), latestReadMsgId, desc.c_str())
		MysqlConnPool::instance()->relConn(conn, false);
		return false;
	}
	MysqlConnPool::instance()->relConn(conn);
	return true;
}

bool XmsgImGroupMemberCollOper::load(bool (*loadCb)(shared_ptr<XmsgImGroupMemberColl> coll, list<shared_ptr<XmsgImGroupUsrClientColl>>& client))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s", XmsgImGroupDb::xmsgImGroupMemberColl.c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImGroupDb::xmsgImGroupMemberColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImGroupDb::xmsgImGroupMemberColl.c_str())
			return true;
		}
		auto coll = XmsgImGroupMemberCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s",XmsgImGroupDb::xmsgImGroupMemberColl.c_str(), row->toString().c_str())
			return false; 
		}
		list<shared_ptr<XmsgImGroupUsrClientColl>> client;
		if (XmsgImGroupMgr::instance()->findUsr(coll->mcgt) == nullptr) 
		{
			if (!XmsgImGroupUsrClientCollOper::instance()->load(coll->mcgt, client))
			{
				LOG_ERROR("load group member`s client failed, mcgt: %s", coll->mcgt->toString().c_str())
				return false;
			}
		}
		if (!loadCb(coll, client))
		{
			return false;
		}
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

shared_ptr<XmsgImGroupMemberColl> XmsgImGroupMemberCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	string str;
	if (!row->getStr("mcgt", str))
	{
		LOG_ERROR("can not found field: mcgt")
		return nullptr;
	}
	SptrCgt mcgt = ChannelGlobalTitle::parse(str);
	if (mcgt == nullptr)
	{
		LOG_ERROR("mcgt format error: %s", str.c_str())
		return nullptr;
	}
	if (!row->getStr("gcgt", str))
	{
		LOG_ERROR("can not found field: gcgt")
		return nullptr;
	}
	SptrCgt gcgt = ChannelGlobalTitle::parse(str);
	if (gcgt == nullptr)
	{
		LOG_ERROR("gcgt format error: %s", str.c_str())
		return nullptr;
	}
	bool enable;
	if (!row->getBool("enable", enable))
	{
		LOG_ERROR("can not found field: enable, mcgt: %s", mcgt->toString().c_str())
		return nullptr;
	}
	ullong latestReadMsgId;
	if (!row->getLong("latestReadMsgId", latestReadMsgId))
	{
		LOG_ERROR("can not found field: latestReadMsgId, mcgt: %s", mcgt->toString().c_str())
		return nullptr;
	}
	ullong ver;
	if (!row->getLong("ver", ver))
	{
		LOG_ERROR("can not found field: ver, mcgt: %s", mcgt->toString().c_str())
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts, mcgt: %s", mcgt->toString().c_str())
		return nullptr;
	}
	ullong uts;
	if (!row->getLong("uts", uts))
	{
		LOG_ERROR("can not found field: uts, mcgt: %s", mcgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgImGroupMemberColl> coll(new XmsgImGroupMemberColl());
	coll->mcgt = mcgt;
	coll->gcgt = gcgt;
	coll->enable = enable;
	coll->latestReadMsgId = latestReadMsgId;
	coll->ver = ver;
	coll->gts = gts;
	coll->uts = uts;
	return coll;
}

XmsgImGroupMemberCollOper::~XmsgImGroupMemberCollOper()
{

}

