#include "chatserver.hpp"
#include "json.hpp"
#include <functional>
#include <string>
#include "chatservice.hpp"

using namespace std;
using namespace placeholders;
using json = nlohmann::json;

//聊天服务器对象初始化
ChatServer::ChatServer(EventLoop* loop,
                const InetAddress& listenAddr,
                const string& nameArg)
                :_server(loop,listenAddr,nameArg)
                ,_loop(loop)
{
    //注册链接回调
    _server.setConnectionCallback(std::bind(&ChatServer::onConnection, this, _1));

    //注册消息回调
    _server.setMessageCallback(std::bind(&ChatServer::onMessage, this, _1,_2,_3));

    //设置线程数量
    _server.setThreadNum(4);
}

//启动服务
void ChatServer::start()
{
    _server.start();
}

//上报链接信息的回调函数
void ChatServer::onConnection(const TcpConnectionPtr& conn)
{
    if(!conn -> connected())
    {
        ChatService::instance()->clientCloseException(conn);
        conn -> shutdown();
    }
}

//上报读写事件相关信息的回调函数
void ChatServer::onMessage(const TcpConnectionPtr& conn,
                            Buffer* buffer,
                            Timestamp time)
{
    string buf = buffer -> retrieveAllAsString();

    //数据的反序列化
    json js = json::parse(buf);

    //通过js["msgid"]获取业务handler
    auto msgHandler = ChatService::instance()->getHandler(js["msgid"].get<int>());
    msgHandler(conn, js, time);
}