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

#ifndef XMSGIMGROUPMSGCOLLOPER_H_
#define XMSGIMGROUPMSGCOLLOPER_H_

#include <libx-msg-im-group-core.h>

class XmsgImGroupMsgCollOper
{
public:
	bool loadLatestMsg(SptrCgt cgt, int latest, list<shared_ptr<XmsgImGroupMsgNotice>>& latestMsg); 
	bool queryMsgByPage(SptrCgt cgt, ullong msgId , bool before , int pageSize , list<shared_ptr<XmsgImGroupMsgColl>>& msg); 
	void saveMsg(SptrGroupMsg msg, function<void(int ret, const string& desc)> cb); 
	void loop(); 
	static XmsgImGroupMsgCollOper* instance();
private:
	static XmsgImGroupMsgCollOper* inst;
	queue<shared_ptr<x_msg_group_msg>> msgQueue; 
	mutex lock4msgQueue; 
	condition_variable cond4msgQueue; 
	void insertBatch(const list<shared_ptr<x_msg_group_msg>>& lis, int& ret, string& desc); 
	shared_ptr<XmsgImGroupMsgColl> loadOneFromIter(void* it); 
	XmsgImGroupMsgCollOper();
	virtual ~XmsgImGroupMsgCollOper();
};

#endif 
