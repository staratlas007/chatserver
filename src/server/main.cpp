#include "chatserver.hpp"
#include "chatservice.hpp"
#include "connection_pool.h"
#include <iostream>
#include <signal.h>

using namespace std;

//处理服务器ctrl+c结束后，重置user的状态信息
void resetHandler(int)
{
    ChatService::instance()->reset();
    exit(0);
}

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        cerr << "command invalid! example: ./ChatServer 127.0.0.1 6000" << endl;
        exit(-1);
    }

    // 初始化数据库连接池
    if (!ConnectionPool::instance()->init(
            "127.0.0.1", "root", "123456", "chat", 3306, 8))
    {
        cerr << "数据库连接池初始化失败" << endl;
        exit(-1);
    }

    char *ip = argv[1];
    uint16_t port = atoi(argv[2]);

    signal(SIGINT, resetHandler);

    EventLoop loop;
    InetAddress addr(ip, port);
    ChatServer server(&loop,addr,"chatserver");

    server.start();
    loop.loop();
    
    return 0;
}