#include<muduo/net/TcpServer.h>
#include<muduo/net/EventLoop.h>
#include<iostream>
#include<functional>
using namespace std;
using namespace muduo;
using namespace muduo::net;
using namespace placeholders;

class ChatServer
{
public:
    ChatServer(EventLoop* loop,//事件循环
                const InetAddress& listenAddr,//IP+端口
                const string& nameArg)//名字
        :_server(loop,listenAddr,nameArg),_loop(loop)
    {
        _server.setConnectionCallback(std::bind(&ChatServer::onConnection,this,_1));

        _server.setMessageCallback(std::bind(&ChatServer::onMessage,this,_1,_2,_3));

        _server.setThreadNum(4);
    }

    void start()
    {
        _server.start();
    }
private:
    void onConnection(const TcpConnectionPtr &conn){
        if(conn->connected())
        {
            cout << conn->peerAddress().toIpPort() << "->" << conn->localAddress().toIpPort() << "state:online" << endl;
        }
        else
        {
            cout << conn->peerAddress().toIpPort() << "->" << conn->localAddress().toIpPort() << "state:offline" << endl;
            conn->shutdown();
        }     
    }
    
    void onMessage(const TcpConnectionPtr &conn,
                    Buffer *buffer,
                    Timestamp time)
    {
        string buf = buffer->retrieveAllAsString();
        cout << "recv data:" << buf << "time:" << time.toString() << endl;
        conn->send(buf);
    }

    TcpServer _server;
    EventLoop *_loop;
};

int main()
{
    EventLoop loop;
    InetAddress addr("127.0.0.1",6000);
    ChatServer server(&loop,addr,"chatserver");
    server.start();
    loop.loop();//epoll-wait以阻塞方式等待链接或事件
    return 0;
}