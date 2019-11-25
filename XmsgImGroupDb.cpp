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
#include "XmsgImGroupCfgCollOper.h"
#include "XmsgImGroupCollOper.h"
#include "XmsgImGroupObjInfoCollOper.h"
#include "XmsgImGroupMemberCollOper.h"
#include "XmsgImGroupMsgCollOper.h"
#include "XmsgImGroupUsrClientCollOper.h"
#include "XmsgImGroupUsrToUsrCollOper.h"
#include "XmsgImGroupVerCollOper.h"

XmsgImGroupDb* XmsgImGroupDb::inst = new XmsgImGroupDb();

string XmsgImGroupDb::xmsgImGroupCfgColl = "tb_x_msg_im_group_cfg"; 
string XmsgImGroupDb::xmsgImGroupColl = "tb_x_msg_im_group"; 
string XmsgImGroupDb::xmsgImGroupObjInfoColl = "tb_x_msg_im_group_obj_info"; 
string XmsgImGroupDb::xmsgImGroupMemberColl = "tb_x_msg_im_group_member"; 
string XmsgImGroupDb::xmsgImGroupMemberInfoColl = "tb_x_msg_im_group_member_info"; 
string XmsgImGroupDb::xmsgImGroupMsgColl = "tb_x_msg_im_group_msg"; 
string XmsgImGroupDb::xmsgImGroupUsrClientColl = "tb_x_msg_im_group_usr_client"; 
string XmsgImGroupDb::xmsgImGroupUsrToUsrColl = "tb_x_msg_im_group_usr_to_usr"; 
string XmsgImGroupDb::xmsgImGroupVerColl = "tb_x_msg_im_group_ver"; 

XmsgImGroupDb::XmsgImGroupDb()
{

}

XmsgImGroupDb* XmsgImGroupDb::instance()
{
	return XmsgImGroupDb::inst;
}

bool XmsgImGroupDb::load()
{
	this->setupDbThread();
	auto& cfg = XmsgImGroupCfg::instance()->cfgPb->mysql();
	if (!MysqlConnPool::instance()->init(cfg.host(), cfg.port(), cfg.db(), cfg.usr(), cfg.password(), cfg.poolsize()))
		return false;
	LOG_INFO("init mysql connection pool successful, host: %s:%d, db: %s", cfg.host().c_str(), cfg.port(), cfg.db().c_str())
	if ("mysql" == XmsgImGroupCfg::instance()->cfgPb->cfgtype() && !this->initCfg())
		return false;
	ullong sts = DateMisc::dida();
	unordered_map<string, shared_ptr<XmsgImGroupVerColl>> ver;
	if (!XmsgImGroupVerCollOper::instance()->reApply4all(ver))
		return false;
	LOG_INFO("load %s.%s successful, count: %zu, elap: %dms", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupVerColl.c_str(), ver.size(), DateMisc::elapDida(sts))
	sts = DateMisc::dida();
	if (!XmsgImGroupCollOper::instance()->load(XmsgImGroupMgr::loadCb4groupLocal, ver))
		return false;
	LOG_INFO("load %s.%s successful, count: %zu, elap: %dms", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupColl.c_str(), XmsgImGroupMgr::instance()->size4group(), DateMisc::elapDida(sts))
	sts = DateMisc::dida();
	if (!XmsgImGroupObjInfoCollOper::instance()->load(XmsgImGroupObjInfoMgr::loadCb4objInfo))
		return false;
	LOG_INFO("load %s.%s successful, count: %d, elap: %dms", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupObjInfoColl.c_str(), XmsgImGroupObjInfoMgr::instance()->size(), DateMisc::elapDida(sts))
	sts = DateMisc::dida();
	if (!XmsgImGroupUsrToUsrCollOper::instance()->load(XmsgImGroupMgr::loadCb4usrToUsr))
		return false;
	LOG_INFO("load %s.%s successful, count: %zu, elap: %dms", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupUsrToUsrColl.c_str(), XmsgImGroupMgr::instance()->size4usr2usr(), DateMisc::elapDida(sts))
	XmsgImGroupUsrClientCollOper::instance()->init();
	sts = DateMisc::dida();
	if (!XmsgImGroupMemberCollOper::instance()->load(XmsgImGroupMgr::loadCb4groupMember))
		return false;
	ullong version = XmsgImGroupMemberColl::version;
	LOG_INFO("load %s.%s successful, count: %zu, max-ver: %llu, elap: %dms", MysqlConnPool::instance()->getDbName().c_str(), XmsgImGroupDb::xmsgImGroupMemberColl.c_str(), XmsgImGroupMgr::instance()->size4usr(), version, DateMisc::elapDida(sts))
	return true;
}

void XmsgImGroupDb::future(function<void()> cb, SptrCgt cgt)
{
	static atomic_uint seq(Crypto::randomInt());
	int index = (cgt != nullptr) ? (cgt->hashCode & 0x00FF) : (seq.fetch_add(1) & 0x00FF);
	this->abst.at(index % this->abst.size())->future(cb);
}

void XmsgImGroupDb::future(function<void()> cb, int hash)
{
	int index = hash & 0x00FF;
	this->abst.at(index % this->abst.size())->future(cb);
}

void XmsgImGroupDb::setupDbThread()
{
	for (int i = 0; i < 1; ++i)
	{
		string name;
		SPRINTF_STRING(&name, "group-%02X", i)
		this->abst.push_back(shared_ptr<ActorBlockingSingleThread>(new ActorBlockingSingleThread(name)));
	}
	this->abstGmsgSave.reset(new ActorBlockingSingleThread("gmsg-save"));
	this->abstGmsgSave->future([]
	{
		XmsgImGroupMemberGlocal::setSaveMsgCb([](SptrGroupMsg msg, function<void(int ret, const string& desc)> cb)
				{
					XmsgImGroupMsgCollOper::instance()->saveMsg(msg, cb);
				});
		while(true)
		{
			XmsgImGroupMsgCollOper::instance()->loop();
		}
	});
}

bool XmsgImGroupDb::initCfg()
{
	auto coll = XmsgImGroupCfgCollOper::instance()->load();
	if (coll == NULL)
		return false;
	LOG_INFO("got a x-msg-im-group config from db: %s", coll->toString().c_str())
	XmsgImGroupCfg::instance()->cfgPb->Clear();
	XmsgImGroupCfg::instance()->cfgPb->CopyFrom(coll->cfg);
	return true;
}

XmsgImGroupDb::~XmsgImGroupDb()
{

}

