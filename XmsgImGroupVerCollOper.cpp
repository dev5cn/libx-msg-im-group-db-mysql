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
#include "XmsgImGroupVerCollOper.h"
#include "XmsgImGroupDb.h"

XmsgImGroupVerCollOper* XmsgImGroupVerCollOper::inst = new XmsgImGroupVerCollOper();

XmsgImGroupVerCollOper::XmsgImGroupVerCollOper()
{

}

XmsgImGroupVerCollOper* XmsgImGroupVerCollOper::instance()
{
	return XmsgImGroupVerCollOper::inst;
}

bool XmsgImGroupVerCollOper::insert(shared_ptr<XmsgImGroupVerColl> coll)
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

bool XmsgImGroupVerCollOper::insert(void* conn, shared_ptr<XmsgImGroupVerColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?)", XmsgImGroupDb::xmsgImGroupVerColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->cgt->toString()) 
	->addLong(coll->ver) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->uts);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupVerColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupVerColl.c_str(), coll->toString().c_str())
	});
}

bool XmsgImGroupVerCollOper::initGroupMsgid(SptrCgt cgt)
{
	shared_ptr<XmsgImGroupVerColl> coll(new XmsgImGroupVerColl());
	coll->cgt = cgt;
	coll->ver = XmsgImGroupCfg::instance()->cfgPb->misc().verapplycount();
	coll->gts = DateMisc::nowGmt0();
	coll->uts = coll->gts;
	return this->insert(coll);
}

bool XmsgImGroupVerCollOper::reApply4all(unordered_map<string, shared_ptr<XmsgImGroupVerColl>>& ver)
{
	XmsgImGroupMemberGlocal::setApplyMsgidCb([](SptrGl gl, function<void (ullong msgIdEnd)> cb)
	{
		XmsgImGroupDb::instance()->future([gl, cb]
				{
					cb(XmsgImGroupVerCollOper::instance()->applyMsgid(gl));
				}, gl->cgt);
	});
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s", XmsgImGroupDb::xmsgImGroupVerColl.c_str())
	bool ret = MysqlMisc::query(conn, sql, [&ver](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImGroupDb::xmsgImGroupVerColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImGroupDb::xmsgImGroupVerColl.c_str())
			return true;
		}
		auto coll = XmsgImGroupVerCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s",XmsgImGroupDb::xmsgImGroupVerColl.c_str(), row->toString().c_str())
			return false; 
		}
		coll->ver += XmsgImGroupCfg::instance()->cfgPb->misc().verapplycount(); 
		ver[coll->cgt->toString()] = coll;
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return !ret ? ret : this->update4all(XmsgImGroupCfg::instance()->cfgPb->misc().verapplycount());
}

bool XmsgImGroupVerCollOper::update4all(int inc)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	ullong sts = DateMisc::dida();
	string sql;
	SPRINTF_STRING(&sql, "update %s set ver = ver + %d", XmsgImGroupDb::xmsgImGroupVerColl.c_str(), inc)
	int ret = 0;
	string desc;
	int affected = 0;
	bool r = MysqlMisc::sql(conn, sql, ret, desc, &affected);
	if (!r)
	{
		LOG_ERROR("update %s failed, elap: %dms, err: %s", XmsgImGroupDb::xmsgImGroupVerColl.c_str(), DateMisc::elapDida(sts), desc.c_str())
	} else
	{
		LOG_INFO("update %s successful, elap: %dms", XmsgImGroupDb::xmsgImGroupVerColl.c_str(), DateMisc::elapDida(sts))
	}
	MysqlConnPool::instance()->relConn(conn, r);
	return r;
}

bool XmsgImGroupVerCollOper::queryOrInitGroupMsgid(unordered_map<string, shared_ptr<XmsgImGroupVerColl>>& ver, shared_ptr<XmsgImGroupColl> coll, ullong& msgIdCurrent, ullong& msgIdEnd)
{
	if (!XmsgImGroupMisc::isLocalGroup(coll->cgt)) 
	{
		LOG_FAULT("it`s a bug, unsupported foreign group, coll: %s", coll->toString().c_str())
		return false;
	}
	auto it = ver.find(coll->cgt->toString());
	if (it == ver.end()) 
	{
		LOG_WARN("have a local group message id does not initialize, coll: %s", coll->toString().c_str())
		if (!XmsgImGroupVerCollOper::instance()->initGroupMsgid(coll->cgt))
		{
			LOG_ERROR("local group message id initialize failed, coll: %s", coll->toString().c_str())
			return false;
		}
		msgIdCurrent = 0ULL;
		msgIdEnd = XmsgImGroupCfg::instance()->cfgPb->misc().verapplycount();
		LOG_WARN("local group message id initialize successful, current: %llu, end: %llu, coll: %s", msgIdCurrent, msgIdEnd, coll->toString().c_str())
		return true;
	}
	msgIdCurrent = it->second->ver - XmsgImGroupCfg::instance()->cfgPb->misc().verapplycount();
	msgIdEnd = it->second->ver;
	return true;
}

ullong XmsgImGroupVerCollOper::applyMsgid(SptrGl gl)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, local group: %s", gl->toString().c_str())
		return 0ULL;
	}
	string sql;
	SPRINTF_STRING(&sql, "update %s set ver = ver + %d, uts = ? where cgt = ?", XmsgImGroupDb::xmsgImGroupVerColl.c_str(), XmsgImGroupCfg::instance()->cfgPb->misc().verapplycount())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addDateTime(DateMisc::nowGmt0()) 
	->addVarchar(gl->cgt->toString());
	int ret = 0;
	string desc;
	int affected = 0;
	if (!MysqlMisc::sql(conn, req, ret, desc, &affected))
	{
		LOG_ERROR("update %s failed, err: %s, local group: %s", XmsgImGroupDb::xmsgImGroupVerColl.c_str(), desc.c_str(), gl->toString().c_str())
		MysqlConnPool::instance()->relConn(conn, false);
		return 0ULL;
	}
	sql.clear();
	SPRINTF_STRING(&sql, "select ver from %s where cgt = '%s'", XmsgImGroupDb::xmsgImGroupVerColl.c_str(), gl->cgt->toString().c_str())
	ullong ver;
	if (!MysqlMisc::longVal(conn, sql, ver))
	{
		LOG_ERROR("query version for local group int table %s failed, local group: %s", XmsgImGroupDb::xmsgImGroupVerColl.c_str(), gl->toString().c_str())
		MysqlConnPool::instance()->relConn(conn, false);
		return 0ULL;
	}
	MysqlConnPool::instance()->relConn(conn);
	return ver;
}

shared_ptr<XmsgImGroupVerColl> XmsgImGroupVerCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	string str;
	if (!row->getStr("cgt", str))
	{
		LOG_ERROR("can not found field: cgt")
		return nullptr;
	}
	auto cgt = ChannelGlobalTitle::parse(str);
	if (cgt == nullptr)
	{
		LOG_ERROR("cgt format error: %s", str.c_str())
		return nullptr;
	}
	ullong ver;
	if (!row->getLong("ver", ver))
	{
		LOG_ERROR("can not found field: ver, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	ullong uts;
	if (!row->getLong("uts", uts))
	{
		LOG_ERROR("can not found field: uts, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgImGroupVerColl> coll(new XmsgImGroupVerColl());
	coll->cgt = cgt;
	coll->ver = ver;
	coll->gts = gts;
	coll->uts = uts;
	return coll;
}

XmsgImGroupVerCollOper::~XmsgImGroupVerCollOper()
{

}

