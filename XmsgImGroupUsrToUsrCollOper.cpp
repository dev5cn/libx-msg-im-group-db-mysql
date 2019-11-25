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
#include "XmsgImGroupCollOper.h"
#include "XmsgImGroupDb.h"
#include "XmsgImGroupMemberInfoCollOper.h"
#include "XmsgImGroupUsrToUsrCollOper.h"
#include "XmsgImGroupMemberCollOper.h"
#include "XmsgImGroupVerCollOper.h"

XmsgImGroupUsrToUsrCollOper* XmsgImGroupUsrToUsrCollOper::inst = new XmsgImGroupUsrToUsrCollOper();

XmsgImGroupUsrToUsrCollOper::XmsgImGroupUsrToUsrCollOper()
{

}

XmsgImGroupUsrToUsrCollOper* XmsgImGroupUsrToUsrCollOper::instance()
{
	return XmsgImGroupUsrToUsrCollOper::inst;
}

bool XmsgImGroupUsrToUsrCollOper::load(bool (*loadCb)(shared_ptr<XmsgImGroupUsrToUsrColl> coll))
{
	XmsgImGroupMgr::instance()->setInitXmsgImGroupMemberUlocalcb([](SptrCgt cgt, function<void(int ret, const string& desc, SptrUl ul)> cb) 
	{
		XmsgImGroupDb::instance()->future([cgt, cb]
				{
					int ret = 0;
					string desc;
					cb(ret, desc, XmsgImGroupUsrToUsrCollOper::instance()->initGroupUsr(cgt, ret, desc));
				}, cgt);
	});
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s", XmsgImGroupDb::xmsgImGroupUsrToUsrColl.c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImGroupDb::xmsgImGroupUsrToUsrColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImGroupDb::xmsgImGroupUsrToUsrColl.c_str())
			return true;
		}
		auto coll = XmsgImGroupUsrToUsrCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s",XmsgImGroupDb::xmsgImGroupUsrToUsrColl.c_str(), row->toString().c_str())
			return false; 
		}
		if (!loadCb(coll))
		{
			return false;
		}
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImGroupUsrToUsrCollOper::insert(shared_ptr<XmsgImGroupUsrToUsrColl> coll)
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

bool XmsgImGroupUsrToUsrCollOper::insert(void* conn, shared_ptr<XmsgImGroupUsrToUsrColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?)", XmsgImGroupDb::xmsgImGroupUsrToUsrColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->key) 
	->addVarchar(coll->gcgt->toString()) 
	->addVarchar(coll->u0->toString()) 
	->addVarchar(coll->u1->toString()) 
	->addDateTime(coll->gts);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupUsrToUsrColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupUsrToUsrColl.c_str(), coll->toString().c_str())
	});
}

SptrGl XmsgImGroupUsrToUsrCollOper::createLocalGroup4localUsr(SptrUl u0, SptrUl u1, shared_ptr<XmsgImGroupUsrToUsrColl> coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, coll: %s", coll->toString().c_str())
		return nullptr;
	}
	if (!MysqlMisc::start(conn))
	{
		LOG_ERROR("start transaction failed, err: %s, coll: %s", ::mysql_error(conn), coll->toString().c_str())
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	ullong now = Xsc::clock;
	shared_ptr<XmsgImGroupColl> groupColl(new XmsgImGroupColl());
	groupColl->cgt = coll->gcgt;
	groupColl->enable = true;
	groupColl->info.reset(new XmsgKv());
	XmsgMisc::insertKv(groupColl->info->mutable_kv(), "private", "true"); 
	groupColl->ver = XmsgImGroupColl::version.fetch_add(1);
	groupColl->gts = now;
	groupColl->uts = now;
	if (!XmsgImGroupCollOper::instance()->insert(conn, groupColl))
	{
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupColl.c_str(), groupColl->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	shared_ptr<XmsgImGroupVerColl> groupVerColl(new XmsgImGroupVerColl());
	groupVerColl->cgt = coll->gcgt;
	groupVerColl->ver = XmsgImGroupCfg::instance()->cfgPb->misc().verapplycount();
	groupVerColl->gts = now;
	groupVerColl->uts = now;
	if (!XmsgImGroupVerCollOper::instance()->insert(conn, groupVerColl))
	{
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupVerColl.c_str(), groupVerColl->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	shared_ptr<XmsgImGroupMemberInfoColl> gmiColl0(new XmsgImGroupMemberInfoColl());
	gmiColl0->gcgt = coll->gcgt;
	gmiColl0->mcgt = u0->cgt;
	gmiColl0->enable = true;
	gmiColl0->info.reset(new XmsgKv());
	gmiColl0->ver = XmsgImGroupColl::version.fetch_add(1);
	gmiColl0->gts = now;
	gmiColl0->uts = now;
	if (!XmsgImGroupMemberInfoCollOper::instance()->insert(conn, gmiColl0))
	{
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str(), gmiColl0->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	shared_ptr<XmsgImGroupMemberInfoColl> gmiColl1(new XmsgImGroupMemberInfoColl());
	gmiColl1->gcgt = coll->gcgt;
	gmiColl1->mcgt = u1->cgt;
	gmiColl1->enable = true;
	gmiColl1->info.reset(new XmsgKv());
	gmiColl1->ver = XmsgImGroupColl::version.fetch_add(1);
	gmiColl1->gts = now;
	gmiColl1->uts = now;
	if (!XmsgImGroupMemberInfoCollOper::instance()->insert(conn, gmiColl1))
	{
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str(), gmiColl1->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	if (!XmsgImGroupUsrToUsrCollOper::instance()->insert(conn, coll))
	{
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupUsrToUsrColl.c_str(), coll->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	shared_ptr<XmsgImGroupMemberColl> gmColl0(new XmsgImGroupMemberColl());
	gmColl0->mcgt = u0->cgt;
	gmColl0->gcgt = coll->gcgt;
	gmColl0->enable = true;
	gmColl0->latestReadMsgId = 0ULL;
	gmColl0->ver = XmsgImGroupMemberColl::version.fetch_add(1);
	gmColl0->gts = now;
	gmColl0->uts = now;
	if (!XmsgImGroupMemberCollOper::instance()->insert(conn, gmColl0))
	{
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupMemberColl.c_str(), gmColl0->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	shared_ptr<XmsgImGroupMemberColl> gmColl1(new XmsgImGroupMemberColl());
	gmColl1->mcgt = u1->cgt;
	gmColl1->gcgt = coll->gcgt;
	gmColl1->enable = true;
	gmColl1->latestReadMsgId = 0ULL;
	gmColl1->ver = XmsgImGroupMemberColl::version.fetch_add(1);
	gmColl1->gts = now;
	gmColl1->uts = now;
	if (!XmsgImGroupMemberCollOper::instance()->insert(conn, gmColl1))
	{
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupMemberColl.c_str(), gmColl1->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	if (!MysqlMisc::commit(conn))
	{
		LOG_ERROR("commit transaction failed, err: %s, coll: %s", ::mysql_error(conn), coll->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	MysqlConnPool::instance()->relConn(conn);
	SptrGl group(new XmsgImGroupMemberGlocal(groupColl, 0ULL, groupVerColl->ver, XmsgImGroupCfg::instance()->cfgPb->misc().groupmsgqueuelimit()));
	shared_ptr<XmsgImGroupMemberInfo> gmi0(new XmsgImGroupMemberInfo());
	shared_ptr<XmsgImGroupMemberInfoHistory> histo0(new XmsgImGroupMemberInfoHistory());
	histo0->set_start(gmiColl0->gts);
	histo0->set_end(gmiColl0->uts);
	gmi0->cgt = gmiColl0->mcgt;
	gmi0->enable = gmiColl0->enable;
	gmi0->info = gmiColl0->info;
	gmi0->history.push_back(histo0);
	gmi0->ver = gmiColl0->ver;
	gmi0->gts = gmiColl0->gts;
	gmi0->uts = gmiColl0->uts;
	shared_ptr<XmsgImGroupMemberInfo> gmi1(new XmsgImGroupMemberInfo());
	shared_ptr<XmsgImGroupMemberInfoHistory> histo1(new XmsgImGroupMemberInfoHistory());
	histo1->set_start(gmiColl1->gts);
	histo1->set_end(gmiColl1->uts);
	gmi1->cgt = gmiColl1->mcgt;
	gmi1->enable = gmiColl1->enable;
	gmi1->info = gmiColl1->info;
	gmi1->history.push_back(histo1);
	gmi1->ver = gmiColl1->ver;
	gmi1->gts = gmiColl1->gts;
	gmi1->uts = gmiColl1->uts;
	group->addMember(gmi0); 
	group->addMember(gmi1); 
	XmsgImGroupMgr::instance()->addGroup(group); 
	XmsgImGroupMgr::instance()->addUsrToUsrGroup(coll->key, group); 
	u0->group[group->cgt->toString()] = gmColl0; 
	u1->group[group->cgt->toString()] = gmColl1; 
	return group;
}

SptrUl XmsgImGroupUsrToUsrCollOper::initGroupUsr(SptrCgt ucgt, int& ret, string& desc)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, coll: %s", ucgt->toString().c_str())
		return nullptr;
	}
	SptrCgt gcgt = ChannelGlobalTitle::genGroup(XmsgImGroupCfg::instance()->cgt); 
	shared_ptr<XmsgImGroupColl> groupColl(new XmsgImGroupColl()); 
	shared_ptr<XmsgImGroupVerColl> groupVerColl(new XmsgImGroupVerColl()); 
	shared_ptr<XmsgImGroupMemberInfoColl> groupMemberInfoColl(new XmsgImGroupMemberInfoColl()); 
	shared_ptr<XmsgImGroupUsrToUsrColl> groupUsrToUsrColl(new XmsgImGroupUsrToUsrColl()); 
	shared_ptr<XmsgImGroupMemberColl> groupMemberColl(new XmsgImGroupMemberColl()); 
	if (!MysqlMisc::start(conn))
	{
		LOG_ERROR("start transaction failed, err: %s, coll: %s", ::mysql_error(conn), ucgt->toString().c_str())
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	ullong now = Xsc::clock;
	groupColl->cgt = gcgt;
	groupColl->enable = true;
	groupColl->info.reset(new XmsgKv());
	XmsgMisc::insertKv(groupColl->info->mutable_kv(), "private", "true"); 
	groupColl->ver = XmsgImGroupColl::version.fetch_add(1);
	groupColl->gts = now;
	groupColl->uts = now;
	if (!XmsgImGroupCollOper::instance()->insert(conn, groupColl))
	{
		ret = RET_EXCEPTION;
		SPRINTF_STRING(&desc, "insert into %s failed", XmsgImGroupDb::xmsgImGroupColl.c_str())
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupColl.c_str(), groupColl->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	groupVerColl->cgt = gcgt;
	groupVerColl->ver = XmsgImGroupCfg::instance()->cfgPb->misc().verapplycount();
	groupVerColl->gts = now;
	groupVerColl->uts = now;
	if (!XmsgImGroupVerCollOper::instance()->insert(conn, groupVerColl))
	{
		ret = RET_EXCEPTION;
		SPRINTF_STRING(&desc, "insert into %s failed", XmsgImGroupDb::xmsgImGroupVerColl.c_str())
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupVerColl.c_str(), groupVerColl->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	groupMemberInfoColl->gcgt = gcgt;
	groupMemberInfoColl->mcgt = ucgt;
	groupMemberInfoColl->enable = true;
	groupMemberInfoColl->info.reset(new XmsgKv());
	groupMemberInfoColl->ver = XmsgImGroupColl::version.fetch_add(1);
	groupMemberInfoColl->gts = now;
	groupMemberInfoColl->uts = now;
	if (!XmsgImGroupMemberInfoCollOper::instance()->insert(conn, groupMemberInfoColl))
	{
		ret = RET_EXCEPTION;
		SPRINTF_STRING(&desc, "insert into %s failed", XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str())
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str(), groupMemberInfoColl->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	groupUsrToUsrColl->key = XmsgImGroupMgr::instance()->genKey(ucgt, ucgt);
	groupUsrToUsrColl->gcgt = gcgt;
	groupUsrToUsrColl->u0 = ucgt;
	groupUsrToUsrColl->u1 = ucgt;
	groupUsrToUsrColl->gts = now;
	if (!XmsgImGroupUsrToUsrCollOper::instance()->insert(conn, groupUsrToUsrColl))
	{
		ret = RET_EXCEPTION;
		SPRINTF_STRING(&desc, "insert into %s failed", XmsgImGroupDb::xmsgImGroupUsrToUsrColl.c_str())
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupUsrToUsrColl.c_str(), groupUsrToUsrColl->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	groupMemberColl->mcgt = ucgt;
	groupMemberColl->gcgt = gcgt;
	groupMemberColl->enable = true;
	groupMemberColl->latestReadMsgId = 0ULL;
	groupMemberColl->ver = XmsgImGroupMemberColl::version.fetch_add(1);
	groupMemberColl->gts = now;
	groupMemberColl->uts = now;
	if (!XmsgImGroupMemberCollOper::instance()->insert(conn, groupMemberColl))
	{
		ret = RET_EXCEPTION;
		SPRINTF_STRING(&desc, "insert into %s failed", XmsgImGroupDb::xmsgImGroupMemberColl.c_str())
		LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupMemberColl.c_str(), groupMemberColl->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	if (!MysqlMisc::commit(conn))
	{
		LOG_ERROR("commit transaction failed, err: %s, ucgt: %s", ::mysql_error(conn), ucgt->toString().c_str())
		ret = RET_EXCEPTION;
		SPRINTF_STRING(&desc, "commit transaction failed, err: %s, ucgt: %s", ::mysql_error(conn), ucgt->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	MysqlConnPool::instance()->relConn(conn);
	SptrGl group(new XmsgImGroupMemberGlocal(groupColl, 0ULL, groupVerColl->ver, XmsgImGroupCfg::instance()->cfgPb->misc().groupmsgqueuelimit()));
	shared_ptr<XmsgImGroupMemberInfo> gmi(new XmsgImGroupMemberInfo());
	shared_ptr<XmsgImGroupMemberInfoHistory> histo(new XmsgImGroupMemberInfoHistory());
	histo->set_start(groupMemberInfoColl->gts);
	histo->set_end(groupMemberInfoColl->uts);
	gmi->cgt = ucgt;
	gmi->enable = groupMemberInfoColl->enable;
	gmi->info = groupMemberInfoColl->info;
	gmi->history.push_back(histo);
	gmi->ver = groupMemberInfoColl->ver;
	gmi->gts = groupMemberInfoColl->gts;
	gmi->uts = groupMemberInfoColl->uts;
	group->addMember(gmi);
	XmsgImGroupMgr::instance()->addGroup(group);
	SptrUl ul(new XmsgImGroupMemberUlocal(ucgt));
	ul->group[groupColl->cgt->toString()] = groupMemberColl;
	XmsgImGroupMgr::instance()->addUsr(ul);
	XmsgImGroupMgr::instance()->addUsrToUsrGroup(groupUsrToUsrColl->key, group);
	ret = 0;
	desc.clear();
	return ul;
}

shared_ptr<XmsgImGroupUsrToUsrColl> XmsgImGroupUsrToUsrCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	string key;
	if (!row->getStr("key", key))
	{
		LOG_ERROR("can not found field: key")
		return nullptr;
	}
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
	if (!row->getStr("u0", str))
	{
		LOG_ERROR("can not found field: u0")
		return nullptr;
	}
	SptrCgt u0 = ChannelGlobalTitle::parse(str);
	if (u0 == nullptr)
	{
		LOG_ERROR("scgt format error: %s", str.c_str())
		return nullptr;
	}
	if (!row->getStr("u1", str))
	{
		LOG_ERROR("can not found field: u1")
		return nullptr;
	}
	SptrCgt u1 = ChannelGlobalTitle::parse(str);
	if (u1 == nullptr)
	{
		LOG_ERROR("dcgt format error: %s", str.c_str())
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts")
		return nullptr;
	}
	shared_ptr<XmsgImGroupUsrToUsrColl> coll(new XmsgImGroupUsrToUsrColl());
	coll->key = key;
	coll->gcgt = gcgt;
	coll->u0 = u0;
	coll->u1 = u1;
	coll->gts = gts;
	return coll;
}

XmsgImGroupUsrToUsrCollOper::~XmsgImGroupUsrToUsrCollOper()
{

}

