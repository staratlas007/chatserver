#ifndef CHATSERVICE_H
#define CHATSERVICE_H

#include "json.hpp"
#include "offlinemessagemodel.hpp"
#include "friendmodel.hpp"
#include "groupmodel.hpp"
#include "redis.hpp"
#include "usermodel.hpp"

#include <muduo/net/TcpConnection.h>
#include <unordered_map>
#include <functional>
#include <mutex>

using namespace std;
using namespace muduo;
using namespace muduo::net; 
using json = nlohmann::json;

using MsgHandler = function<void(const TcpConnectionPtr &conn, json &js, Timestamp)>;
//聊天服务器业务类
class ChatService
{
public:
    //获取单例对象的接口函数
    static ChatService* instance();

    //处理登陆业务
    void login(const TcpConnectionPtr &conn, json &js, Timestamp);

    //处理注册业务
    void reg(const TcpConnectionPtr &conn, json &js, Timestamp);

    //一对一聊天业务
    void oneChat(const TcpConnectionPtr &conn, json &js, Timestamp);

    //添加好友业务
    void addFriend(const TcpConnectionPtr &conn, json &js, Timestamp);

    //创建群组业务
    void createGroup(const TcpConnectionPtr &conn, json &js, Timestamp);

    //加入群组业务
    void addGroup(const TcpConnectionPtr &conn, json &js, Timestamp);

    //群组聊天业务
    void groupChat(const TcpConnectionPtr &conn, json &js, Timestamp);

    //获取消息对应的处理器
    MsgHandler getHandler(int msgid);

    //正常退出
    void loginOut(const TcpConnectionPtr &conn, json &js, Timestamp);

    //处理客户端异常退出
    void clientCloseException(const TcpConnectionPtr &conn);

    //服务器异常，业务重置方法
    void reset();

    // 从redis消息队列中获取订阅的消息
    void handleRedisSubscribeMessage(int, string);

private:
    ChatService();

    //存储消息id和对应的业务处理方法
    unordered_map<int, MsgHandler> _msgHandlerMap;

    //数据操作类对象
    UserModel _usermodel;
    OfflineMsgModel _offlineMsgModel;
    FriendModel _friendModel;
    GroupModel _groupModel;

    //定义互斥锁，保证_userConnMap的线程安全
    mutex _connMutex;

    //存储在线用户的通信连接
    unordered_map<int, TcpConnectionPtr> _userConnMap;

    Redis _redis;


};

#endif