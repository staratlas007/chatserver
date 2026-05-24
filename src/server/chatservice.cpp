#include "chatservice.hpp"
#include "public.hpp"
#include "bcrypt/BCrypt.hpp"
#include <muduo/base/Logging.h>
#include <vector>
#include <iostream>
using namespace muduo;
using namespace placeholders;
using namespace std;

//获取单例对象的接口函数
ChatService* ChatService::instance()
{
    static ChatService service;
    return &service;
}

//注册消息以及对应的Handler回调操作
ChatService::ChatService()
{
    _msgHandlerMap.insert({LOGIN_MSG, std::bind(&ChatService::login, this, _1, _2, _3)});
    _msgHandlerMap.insert({REG_MSG, std::bind(&ChatService::reg, this, _1, _2, _3)});
    _msgHandlerMap.insert({ONE_CHAT_MSG, std::bind(&ChatService::oneChat, this, _1, _2, _3)});
    _msgHandlerMap.insert({ADD_FRIEND_MSG, std::bind(&ChatService::addFriend, this, _1, _2, _3)});
    _msgHandlerMap.insert({CREATE_GROUP_MSG, std::bind(&ChatService::createGroup, this, _1, _2, _3)});
    _msgHandlerMap.insert({ADD_GROUP_MSG, std::bind(&ChatService::addGroup, this, _1, _2, _3)});
    _msgHandlerMap.insert({GROUP_CHAT_MSG, std::bind(&ChatService::groupChat, this, _1, _2, _3)});
    _msgHandlerMap.insert({LOGINOUT_MSG, std::bind(&ChatService::loginOut, this, _1, _2, _3)});

    if(_redis.connect())
    {
        _redis.init_notify_handler(std::bind(&ChatService::handleRedisSubscribeMessage, this, _1, _2));
    }

}

//获取消息对应的处理器
MsgHandler ChatService::getHandler(int msgid)
{
    //记录错误日志，msgid没有对应的事件处理回调
    auto it = _msgHandlerMap.find(msgid);
    if(it == _msgHandlerMap.end())
    {
        //返回一个默认的处理器
        return [=](const TcpConnectionPtr &conn, json &js, Timestamp) {
            LOG_ERROR << "msgid:" << msgid << " 找不到对应的处理器";
        };
    }
    return it->second;
}

//处理登录业务
 void ChatService::login(const TcpConnectionPtr &conn, json &js, Timestamp)
 {
    int id = js["id"].get<int>();
    string pwd = js["password"];

    User user = _usermodel.query(id);
    bool pwdcheck = BCrypt::validatePassword(pwd, user.getPwd());
    if(user.getID() == id && pwdcheck)
    {
        if(user.getState() == "online")
        {
            //用户已登陆，禁止重复登陆
            json response;
            response["msgid"] = LOGIN_MSG_ACK;
            response["errno"] = 2;
            response["errmsg"] = "您已登录";
            conn->send(response.dump() + '\0');
        }
        else
        {
            //登陆成功，记录用户连接信息
            {
                lock_guard<mutex> lock(_connMutex);
                _userConnMap.insert({id, conn});
            }
            
            //id用户登录成功后，向redis订阅channel
            _redis.subscribe(id);

            //登陆成功,修改登录状态  
            user.setState("online");
            _usermodel.updateState(user);

            json response;
            response["msgid"] = LOGIN_MSG_ACK;
            response["errno"] = 0;
            response["id"] = user.getID();
            response["name"] = user.getName();
            
            //登陆成功后查询该用户是否有离线消息
            vector<string> vec = _offlineMsgModel.query(id);
            if(!vec.empty())
            {
                response["offlinemsg"] = vec;
                //读取离线消息后，删除离线消息
                if(_offlineMsgModel.remove(id))
                {
                    LOG_INFO<< "删除离线消息成功";
                }
                else
                {
                    LOG_ERROR << "删除离线消息失败";
                }
            }

            //查询该用户的好友信息并返回
            vector<User> userVec = _friendModel.query(id);
            if(!userVec.empty())
            {
                vector<string> vec2;
                for(User &user : userVec)
                {
                    json js;
                    js["id"] = user.getID();
                    js["name"] = user.getName();
                    js["state"] = user.getState();
                    vec2.push_back(js.dump());  
                }
                response["friends"] = vec2;
            }

            //查询用户的群组信息
            vector<Group> groupuserVec = _groupModel.queryGroups(id);
            if(!groupuserVec.empty())
            {
                vector<string> groupv;
                for(Group &group : groupuserVec)
                {
                    json js;
                    js["id"] = group.getID();
                    js["groupname"] = group.getName();
                    js["groupdesc"] = group.getDesc();

                    vector<string> userv;
                    for(GroupUser &user : group.getUsers())
                    {
                        json js1;
                        js1["id"] = user.getID();
                        js1["name"] = user.getName();
                        js1["state"] = user.getState();
                        js1["role"] = user.getRole();
                        userv.push_back(js1.dump());
                    }

                    js["users"] = userv;
                    groupv.push_back(js.dump());  
                }
                response["groups"] = groupv;
            }

            conn->send(response.dump() + '\0');
        }
        
    }  
    else
    {
        //用户不存在，登陆失败
        json response;
        response["msgid"] = LOGIN_MSG_ACK;
        response["errno"] = 1;
        response["errmsg"] = "用户名或密码错误";
        conn->send(response.dump() + '\0');
    }
 }

 //处理注册业务
void ChatService::reg(const TcpConnectionPtr &conn, json &js, Timestamp)
{
    string name = js["name"];
    string pwd = js["password"];

    User user;
    user.setName(name);
    user.setPwd(pwd);
    bool state =  _usermodel.insert(user);
    if(state)
    {
        //注册成功
        json response;
        response["msgid"] = REG_MSG_ACK;
        response["errno"] = 0;
        response["id"] = user.getID();
        conn->send(response.dump() + '\0');
    }
    else
    {
        //注册失败
        json response;
        response["msgid"] = REG_MSG_ACK;
        response["errno"] = 1;
        conn->send(response.dump() + '\0');
    }
}



//处理客户端异常退出
void ChatService::clientCloseException(const TcpConnectionPtr &conn)
{
    User user;
    {
        lock_guard<mutex> lock(_connMutex);
        for(auto it = _userConnMap.begin(); it != _userConnMap.end(); ++it)
        {
            if(it->second == conn)
            {
                //从map表删除用户的连接信息
                user.setID(it->first);
                _userConnMap.erase(it);
                break;
            }
        }
    }

    _redis.unsubscribe(user.getID());
    
    //更新用户状态信息
    if(user.getID() != -1)
    {
         user.setState("offline");
        _usermodel.updateState(user);
    }
   
}

//服务器异常，业务重置方法
void ChatService::reset()
{
    _usermodel.resetState();
}

//一对一聊天业务
void ChatService::oneChat(const TcpConnectionPtr &conn, json &js, Timestamp)
{
    int toid = js["toid"].get<int>();

    {
        lock_guard<mutex> lock(_connMutex);
        auto it = _userConnMap.find(toid);
        if(it != _userConnMap.end())
        {
            //对方在线,转发消息
            it->second->send(js.dump() + '\0');
            return;
        }
    }

    User user = _usermodel.query(toid);
    if(user.getState() == "online")
    {
        _redis.publish(toid, js.dump());
        return;
    }

    //对方不在线,存储离线消息
    if(_offlineMsgModel.insert(toid, js.dump()))
    {
        LOG_INFO << "离线消息存储成功";
    }
    else
    {
        LOG_ERROR << "离线消息存储失败";
    }
}

//添加好友业务
void ChatService::addFriend(const TcpConnectionPtr &conn, json &js, Timestamp)
{
    int userid = js["id"].get<int>();
    int friendid = js["friendid"].get<int>();

    json response;

    if (_friendModel.insert(userid, friendid))
    {
        response["msgid"] = RESPONSE;
        response["res"] = "添加好友成功";
    }
    else
    {
        response["msgid"] = RESPONSE;
        response["res"] = "添加好友失败";
    }

    conn->send(response.dump() + '\0');
}

 //创建群组业务
void ChatService::createGroup(const TcpConnectionPtr &conn, json &js, Timestamp)
{
    int userid = js["id"].get<int>();
    string name = js["groupname"];
    string desc = js["groupdesc"];

    json response;

    Group group(-1, name, desc);
    if(_groupModel.createGroup(group))
    {
        if(_groupModel.addGroup(userid, group.getID(), "creator"))
        {
            response["msgid"] = RESPONSE;
            response["res"] = "建群成功，您以成为群主";
        }
        else
        {
            response["msgid"] = RESPONSE;
            response["res"] = "建群成功，但您不在群内";
        }
    }
    else
    {
        response["msgid"] = RESPONSE;
        response["res"] = "建群失败";
    }

    conn->send(response.dump() + '\0');
}

//加入群组业务
void ChatService::addGroup(const TcpConnectionPtr &conn, json &js, Timestamp)
{
    int userid = js["id"].get<int>();
    int groupid = js["groupid"];

    json response;
    if(_groupModel.addGroup(userid, groupid, "normal"))
    {
        response["msgid"] = RESPONSE;
        response["res"] = "加群成功";
    }
    else
    {
        response["msgid"] = RESPONSE;
        response["res"] = "加群失败";
    }
    
    conn->send(response.dump() + '\0');
}

//群组聊天业务
void ChatService::groupChat(const TcpConnectionPtr &conn, json &js, Timestamp)
{
    int userid = js["id"].get<int>();
    int groupid = js["groupid"].get<int>();
    vector<int> useridVec = _groupModel.queryGroupUsers(userid, groupid);

    lock_guard<mutex> lock(_connMutex);
    for(int id : useridVec)
    {
        auto it = _userConnMap.find(id);
        if(it != _userConnMap.end())
        {
            //转发群消息
            it->second->send(js.dump() + '\0');
        }
        else
        {
            User user = _usermodel.query(id);
            if(user.getState() == "online")
            {
                _redis.publish(id, js.dump());
                return;
            }
            else
            {
                //存储离线消息
                if(_offlineMsgModel.insert(id, js.dump()))
                {
                    LOG_INFO << "离线消息存储成功";
                }
                else
                {
                    LOG_ERROR << "离线消息存储失败";
                }
            }
        }
    }
}

//处理登出
void ChatService::loginOut(const TcpConnectionPtr &conn, json &js, Timestamp)
{
    int id = js["id"].get<int>();

    {
        lock_guard<mutex> lock(_connMutex);
        auto it = _userConnMap.find(id);
        if(it != _userConnMap.end())
        {
            _userConnMap.erase(it);
        }
    }

    _redis.unsubscribe(id);

    User user(id, "", "", "offline");
    _usermodel.updateState(user);
}

// 从redis消息队列中获取订阅的消息
void ChatService::handleRedisSubscribeMessage(int userid, string msg)
{
    lock_guard<mutex> lock(_connMutex);
    auto it = _userConnMap.find(userid);
    if (it != _userConnMap.end())
    {
        it->second->send(msg + '\0');
        return;
    }

    // 存储该用户的离线消息
    if(_offlineMsgModel.insert(userid, msg))
    {
        LOG_INFO << "离线消息存储成功";
    }
    else
    {
        LOG_ERROR << "离线消息存储失败";
    }

}