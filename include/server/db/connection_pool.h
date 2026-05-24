#ifndef CONNECTION_POOL_H
#define CONNECTION_POOL_H

#include <mysql/mysql.h>
#include <muduo/base/Logging.h>

#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <string>

class ConnectionPool
{
public:
    static ConnectionPool* instance();

    // 从池中获取一个连接（阻塞等待直到有可用连接或超时）
    MYSQL* acquire(int timeoutMs = 3000);

    // 归还连接到池中
    void release(MYSQL* conn);

    // 初始化连接池
    bool init(const std::string& host, const std::string& user,
              const std::string& password, const std::string& dbname,
              int port = 3306, int poolSize = 8);

    ~ConnectionPool();

private:
    ConnectionPool() = default;
    //禁止拷贝构造，确保连接池为单例对象
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    MYSQL* createConnection();

    std::queue<MYSQL*> _pool;//空闲连接队列
    std::mutex _mtx;
    std::condition_variable _cv;

    // 连接配置
    std::string _host;
    std::string _user;
    std::string _password;
    std::string _dbname;
    int _port = 3306;
    int _poolSize = 8;
    bool _initialized = false;
};

class ConnectionGuard {
public:
    ConnectionGuard(ConnectionPool* pool) : _pool(pool), _conn(pool->acquire()) {}
    
    ~ConnectionGuard() 
    {
        if (_conn) 
        {
            _pool->release(_conn); 
        }
    }
    
    MYSQL* get() { return _conn; }
    
private:
    ConnectionPool* _pool;
    MYSQL* _conn;
};
#endif