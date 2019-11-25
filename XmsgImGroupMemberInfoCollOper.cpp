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
#include "XmsgImGroupMemberInfoCollOper.h"

XmsgImGroupMemberInfoCollOper* XmsgImGroupMemberInfoCollOper::inst = new XmsgImGroupMemberInfoCollOper();

XmsgImGroupMemberInfoCollOper::XmsgImGroupMemberInfoCollOper()
{

}

XmsgImGroupMemberInfoCollOper* XmsgImGroupMemberInfoCollOper::instance()
{
	return XmsgImGroupMemberInfoCollOper::inst;
}

bool XmsgImGroupMemberInfoCollOper::load(const string& gcgt, list<shared_ptr<XmsgImGroupMemberInfoColl>>& member)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, gcgt: %s", gcgt.c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s where gcgt = '%s'", XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str(), gcgt.c_str())
	bool ret = MysqlMisc::query(conn, sql, [&member](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str())
			return true;
		}
		auto coll = XmsgImGroupMemberInfoCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s",XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str(), row->toString().c_str())
			return false; 
		}
		member.push_back(coll);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImGroupMemberInfoCollOper::insert(shared_ptr<XmsgImGroupMemberInfoColl> coll)
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

bool XmsgImGroupMemberInfoCollOper::insert(void* conn, shared_ptr<XmsgImGroupMemberInfoColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?, ?)", XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->gcgt->toString()) 
	->addVarchar(coll->mcgt->toString()) 
	->addBool(coll->enable) 
	->addBlob(coll->info->SerializeAsString()) 
	->addLong(coll->ver) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->uts);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str(), coll->toString().c_str())
	});
}

shared_ptr<XmsgImGroupMemberInfoColl> XmsgImGroupMemberInfoCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	string str;
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
	bool enable;
	if (!row->getBool("enable", enable))
	{
		LOG_ERROR("can not found field: enable, mcgt: %s", mcgt->toString().c_str())
		return nullptr;
	}
	if (!row->getBin("info", str))
	{
		LOG_ERROR("can not found field: info, mcgt: %s", mcgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgKv> info(new XmsgKv());
	if (!info->ParseFromString(str))
	{
		LOG_ERROR("info format error, mcgt: %s", mcgt->toString().c_str())
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
	shared_ptr<XmsgImGroupMemberInfoColl> coll(new XmsgImGroupMemberInfoColl());
	coll->gcgt = gcgt;
	coll->mcgt = mcgt;
	coll->enable = enable;
	coll->info = info;
	coll->ver = ver;
	coll->gts = gts;
	coll->uts = uts;
	return coll;
}

XmsgImGroupMemberInfoCollOper::~XmsgImGroupMemberInfoCollOper()
{

}

