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
#include "XmsgImGroupObjInfoCollOper.h"
#include "XmsgImGroupDb.h"

XmsgImGroupObjInfoCollOper* XmsgImGroupObjInfoCollOper::inst = new XmsgImGroupObjInfoCollOper();

XmsgImGroupObjInfoCollOper::XmsgImGroupObjInfoCollOper()
{

}

XmsgImGroupObjInfoCollOper* XmsgImGroupObjInfoCollOper::instance()
{
	return XmsgImGroupObjInfoCollOper::inst;
}

bool XmsgImGroupObjInfoCollOper::load(bool (*loadCb)(shared_ptr<XmsgImGroupObjInfoColl> coll))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	ullong now = Xsc::clock;
	now -= XmsgImGroupCfg::instance()->cfgPb->misc().groupobjinfocached() * DateMisc::day;
	string sql;
	SPRINTF_STRING(&sql, "select * from %s where gts > '%s'", XmsgImGroupDb::xmsgImGroupObjInfoColl.c_str(), DateMisc::to_yyyy_mm_dd_hh_mi_ss_ms(now).c_str())
	int size = 0;
	bool ret = MysqlMisc::query(conn, sql, [loadCb, &size](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImGroupDb::xmsgImGroupObjInfoColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImGroupDb::xmsgImGroupObjInfoColl.c_str())
			return true;
		}
		auto coll = XmsgImGroupObjInfoCollOper::instance()->loadOneFromIter(row.get());
		if (coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgImGroupDb::xmsgImGroupObjInfoColl.c_str(), row->toString().c_str())
			return false; 
		}
		loadCb(coll);
		++size;
		return true;
	});
	LOG_DEBUG("load %s.%s successful, size: %d", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupObjInfoColl.c_str(), size)
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImGroupObjInfoCollOper::insert(shared_ptr<XmsgImGroupObjInfoColl> coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, coll: %s", coll->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?)", XmsgImGroupDb::xmsgImGroupObjInfoColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->gcgt->toString()) 
	->addVarchar(coll->ucgt->toString()) 
	->addVarchar(coll->oid) 
	->addDateTime(coll->gts);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupObjInfoColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupObjInfoColl.c_str(), coll->toString().c_str())
	});
}

shared_ptr<XmsgImGroupObjInfoColl> XmsgImGroupObjInfoCollOper::loadOneFromIter(void* it)
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
	if (!row->getStr("ucgt", str))
	{
		LOG_ERROR("can not found field: ucgt")
		return nullptr;
	}
	SptrCgt ucgt = ChannelGlobalTitle::parse(str);
	if (ucgt == nullptr)
	{
		LOG_ERROR("ucgt format error: %s", str.c_str())
		return nullptr;
	}
	string oid;
	if (!row->getStr("oid", oid))
	{
		LOG_ERROR("can not found field: oid, gcgt: %s", gcgt->toString().c_str())
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts, gcgt: %s", gcgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgImGroupObjInfoColl> coll(new XmsgImGroupObjInfoColl());
	coll->gcgt = gcgt;
	coll->ucgt = ucgt;
	coll->oid = oid;
	coll->gts = gts;
	return coll;
}

XmsgImGroupObjInfoCollOper::~XmsgImGroupObjInfoCollOper()
{

}

