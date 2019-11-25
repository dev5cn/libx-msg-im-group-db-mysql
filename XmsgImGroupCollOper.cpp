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
#include "XmsgImGroupMemberCollOper.h"
#include "XmsgImGroupMemberInfoCollOper.h"
#include "XmsgImGroupMsgCollOper.h"
#include "XmsgImGroupVerCollOper.h"
#include "XmsgImGroupUsrToUsrCollOper.h"

XmsgImGroupCollOper* XmsgImGroupCollOper::inst = new XmsgImGroupCollOper();

XmsgImGroupCollOper::XmsgImGroupCollOper()
{

}

XmsgImGroupCollOper* XmsgImGroupCollOper::instance()
{
	return XmsgImGroupCollOper::inst;
}

bool XmsgImGroupCollOper::load(bool (*loadCb4groupLocal)(shared_ptr<XmsgImGroupColl> group, list<shared_ptr<XmsgImGroupMemberInfoColl>>& lis, list<shared_ptr<XmsgImGroupMsgNotice>>& latestMsg, 
		ullong msgIdCurrent, ullong msgIdEnd), unordered_map<string, shared_ptr<XmsgImGroupVerColl>>& ver)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s", XmsgImGroupDb::xmsgImGroupColl.c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb4groupLocal, &ver](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImGroupDb::xmsgImGroupColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImGroupDb::xmsgImGroupColl.c_str())
			return true;
		}
		auto coll = XmsgImGroupCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s",XmsgImGroupDb::xmsgImGroupColl.c_str(), row->toString().c_str())
			return false; 
		}
		list<shared_ptr<XmsgImGroupMemberInfoColl>> lis;
		if (!XmsgImGroupMemberInfoCollOper::instance()->load(coll->cgt->toString(), lis))
		{
			LOG_ERROR("load group member failed, coll: %s", coll->toString().c_str())
			return false;
		}
		if(XmsgImGroupMisc::isLocalGroup(coll->cgt)) 
		{
			ullong msgIdCurrent = 0ULL;
			ullong msgIdEnd = 0ULL;
			if (XmsgImGroupMisc::isLocalGroup(coll->cgt) && !XmsgImGroupVerCollOper::instance()->queryOrInitGroupMsgid(ver, coll, msgIdCurrent, msgIdEnd)) 
			{
				LOG_ERROR("init group message id failed, coll: %s", coll->toString().c_str())
				return false;
			}
			list<shared_ptr<XmsgImGroupMsgNotice>> latestMsg;
			if (!XmsgImGroupMsgCollOper::instance()->loadLatestMsg(coll->cgt, XmsgImGroupCfg::instance()->cfgPb->misc().groupmsgqueuelimit(), latestMsg)) 
			{
				LOG_ERROR("load group latest message failed, coll: %s", coll->toString().c_str())
				return false;
			}
			if (!loadCb4groupLocal(coll, lis, latestMsg, msgIdCurrent, msgIdEnd))
			{
				return false;
			}
			return true;
		}
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImGroupCollOper::insert(shared_ptr<XmsgImGroupColl> coll)
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

bool XmsgImGroupCollOper::insert(void* conn, shared_ptr<XmsgImGroupColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?)", XmsgImGroupDb::xmsgImGroupColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->cgt->toString()) 
	->addBool(coll->enable) 
	->addBlob(coll->info->SerializeAsString()) 
	->addLong(coll->ver) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->uts);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupColl.c_str(), coll->toString().c_str())
	});
}

bool XmsgImGroupCollOper::update(SptrCgt cgt, shared_ptr<XmsgKv> info, ullong ver, ullong uts)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, creator: %s", cgt->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "update %s set info = ? ver = ?, uts = ? where cgt = ?", XmsgImGroupDb::xmsgImGroupColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addBlob(info->SerializeAsString()) 
	->addLong(ver) 
	->addDateTime(uts) 
	->addVarchar(cgt->toString());
	int ret = 0;
	string desc;
	int affected = 0;
	if (!MysqlMisc::sql(conn, req, ret, desc, &affected))
	{
		LOG_ERROR("update %s failed, cgt: %s, err: %s", XmsgImGroupDb::xmsgImGroupColl.c_str(), cgt->toString().c_str(), desc.c_str())
		MysqlConnPool::instance()->relConn(conn, false);
		return false;
	}
	MysqlConnPool::instance()->relConn(conn);
	return true;
}

SptrGl XmsgImGroupCollOper::createGroupSimple(shared_ptr<XmsgImGroupCreateReq> req, shared_ptr<unordered_map<string, pair<SptrUl, shared_ptr<XmsgKv>>>> member, SptrUl creator)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, creator: %s", creator->toString().c_str())
		return nullptr;
	}
	if (!MysqlMisc::start(conn))
	{
		LOG_ERROR("start transaction failed, err: %s, creator: %s", ::mysql_error(conn), creator->toString().c_str())
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	ullong now = Xsc::clock;
	shared_ptr<XmsgImGroupColl> groupColl(new XmsgImGroupColl());
	groupColl->cgt = ChannelGlobalTitle::genGroup(XmsgImGroupCfg::instance()->cgt);
	groupColl->enable = true;
	groupColl->info.reset(new XmsgKv());
	for (auto& it : req->info())
		XmsgMisc::insertKv(groupColl->info->mutable_kv(), it.first, it.second);
	groupColl->info->mutable_kv()->erase("private"); 
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
	groupVerColl->cgt = groupColl->cgt;
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
	SptrGl gl(new XmsgImGroupMemberGlocal(groupColl, 0, groupVerColl->ver, XmsgImGroupCfg::instance()->cfgPb->misc().groupmsgqueuelimit()));
	unordered_map<string , shared_ptr<XmsgImGroupMemberColl> > gms;
	for (auto& it : *member) 
	{
		shared_ptr<XmsgImGroupMemberInfoColl> gmiColl(new XmsgImGroupMemberInfoColl());
		gmiColl->gcgt = groupColl->cgt;
		gmiColl->mcgt = it.second.first->cgt;
		gmiColl->enable = true;
		gmiColl->info = it.second.second;
		gmiColl->ver = XmsgImGroupColl::version.fetch_add(1);
		gmiColl->gts = now;
		gmiColl->uts = now;
		if (!XmsgImGroupMemberInfoCollOper::instance()->insert(conn, gmiColl))
		{
			LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupMemberInfoColl.c_str(), gmiColl->toString().c_str())
			MysqlMisc::rollBack(conn);
			MysqlConnPool::instance()->relConn(conn, false);
			return nullptr;
		}
		shared_ptr<XmsgImGroupMemberInfo> gmi(new XmsgImGroupMemberInfo());
		gmi->cgt = gmiColl->mcgt;
		gmi->enable = gmiColl->enable;
		gmi->info = gmiColl->info;
		shared_ptr<XmsgImGroupMemberInfoHistory> histo(new XmsgImGroupMemberInfoHistory());
		histo->set_start(gmiColl->gts);
		histo->set_end(gmiColl->uts);
		gmi->ver = gmiColl->ver;
		gmi->gts = now;
		gmi->uts = now;
		gl->addMember(gmi); 
		shared_ptr<XmsgImGroupMemberColl> gmColl(new XmsgImGroupMemberColl());
		gmColl->mcgt = it.second.first->cgt;
		gmColl->gcgt = groupColl->cgt;
		gmColl->enable = true;
		gmColl->latestReadMsgId = 0ULL;
		gmColl->ver = XmsgImGroupMemberColl::version.fetch_add(1); 
		gmColl->gts = now;
		gmColl->uts = now;
		if (!XmsgImGroupMemberCollOper::instance()->insert(conn, gmColl)) 
		{
			LOG_ERROR("insert into %s failed, coll: %s", XmsgImGroupDb::xmsgImGroupMemberColl.c_str(), gmColl->toString().c_str())
			MysqlMisc::rollBack(conn);
			MysqlConnPool::instance()->relConn(conn, false);
			return nullptr;
		}
		gms[gmColl->mcgt->toString()] = gmColl; 
	}
	if (!MysqlMisc::commit(conn))
	{
		LOG_ERROR("commit transaction failed, err: %s, creator: %s", ::mysql_error(conn), creator->toString().c_str())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return nullptr;
	}
	MysqlConnPool::instance()->relConn(conn);
	XmsgImGroupMgr::instance()->addGroup(gl);
	for (auto& it : gms) 
	{
		SptrUl ul = member->find(it.first)->second.first;
		shared_ptr<XmsgImGroupMemberColl> gm = it.second;
		ul->future([ul, gm]
		{
			ul->group[gm->gcgt->toString()] = gm;
		});
	}
	LOG_INFO("create local group successful, member-size: %zu, creator: %s, group: %s", member->size(), creator->toString().c_str(), gl->toString().c_str())
	return gl;
}

shared_ptr<XmsgImGroupColl> XmsgImGroupCollOper::loadOneFromIter(void* it)
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
	bool enable;
	if (!row->getBool("enable", enable))
	{
		LOG_ERROR("can not found field: enable, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	if (!row->getBin("info", str))
	{
		LOG_ERROR("can not found field: info, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgKv> info(new XmsgKv());
	if (!info->ParseFromString(str))
	{
		LOG_ERROR("info format error, cgt: %s", cgt->toString().c_str())
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
	shared_ptr<XmsgImGroupColl> coll(new XmsgImGroupColl());
	coll->cgt = cgt;
	coll->enable = enable;
	coll->info = info;
	coll->ver = ver;
	coll->gts = gts;
	coll->uts = uts;
	return coll;
}

XmsgImGroupCollOper::~XmsgImGroupCollOper()
{

}

