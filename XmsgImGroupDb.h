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

#ifndef XMSGIMGROUPDB_H_
#define XMSGIMGROUPDB_H_

#include <libx-msg-im-group-core.h>

class XmsgImGroupDb
{
public:
	bool load(); 
	void future(function<void()> cb, SptrCgt cgt = nullptr); 
	void future(function<void()> cb, int hash); 
	static XmsgImGroupDb* instance();
public:
	static string xmsgImGroupCfgColl; 
	static string xmsgImGroupColl; 
	static string xmsgImGroupObjInfoColl; 
	static string xmsgImGroupMemberColl; 
	static string xmsgImGroupMemberInfoColl; 
	static string xmsgImGroupMsgColl; 
	static string xmsgImGroupUsrClientColl; 
	static string xmsgImGroupUsrToUsrColl; 
	static string xmsgImGroupVerColl; 
private:
	vector<shared_ptr<ActorBlockingSingleThread>> abst; 
	shared_ptr<ActorBlockingSingleThread> abstGmsgSave; 
	static XmsgImGroupDb* inst;
	void setupDbThread(); 
	bool initCfg(); 
	XmsgImGroupDb();
	virtual ~XmsgImGroupDb();
};

#endif 
