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
    //以\0作分隔符解决粘包
    while (buffer->readableBytes() > 0)
    {
        const char* data = buffer->peek();
        size_t len = buffer->readableBytes();

        // 查找消息分隔符 \0（发送端用 strlen+1 发送，末尾自带\0）
        const char* nullPos = static_cast<const char*>(memchr(data, '\0', len));
        if (nullPos == nullptr) return;  // 半包：消息不完整，等待下次数据到达

        size_t msgLen = nullPos - data;
        string buf(data, msgLen);             // 提取消息体（不含\0）
        buffer->retrieve(msgLen + 1);         // 消费：消息体 + \0

        json js = json::parse(buf);
        auto msgHandler = ChatService::instance()->getHandler(js["msgid"].get<int>());
        msgHandler(conn, js, time);
    }
}