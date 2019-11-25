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

#ifndef XMSGIMGROUPCOLLOPER_H_
#define XMSGIMGROUPCOLLOPER_H_

#include <libx-msg-im-group-core.h>

class XmsgImGroupCollOper
{
public:
	bool load(bool (*loadCb4groupLocal)(shared_ptr<XmsgImGroupColl> group, list<shared_ptr<XmsgImGroupMemberInfoColl>>& lis, list<shared_ptr<XmsgImGroupMsgNotice>>& latestMsg, 
			ullong msgIdCurrent, ullong msgIdEnd), unordered_map<string, shared_ptr<XmsgImGroupVerColl>>& ver); 
	bool insert(shared_ptr<XmsgImGroupColl> coll); 
	bool insert(void* conn, shared_ptr<XmsgImGroupColl> coll); 
	bool update(SptrCgt cgt, shared_ptr<XmsgKv> info, ullong ver, ullong uts); 
	SptrGl createGroupSimple(shared_ptr<XmsgImGroupCreateReq> req, shared_ptr<unordered_map<string, pair<SptrUl, shared_ptr<XmsgKv>>>> member, SptrUl creator); 
	static XmsgImGroupCollOper* instance();
private:
	static XmsgImGroupCollOper* inst;
	shared_ptr<XmsgImGroupColl> loadOneFromIter(void* it); 
	XmsgImGroupCollOper();
	virtual ~XmsgImGroupCollOper();
};

#endif 
