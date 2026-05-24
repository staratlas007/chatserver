#include "connection_pool.h"
#include <muduo/base/Logging.h>

ConnectionPool* ConnectionPool::instance()
{
    static ConnectionPool pool;
    return &pool;
}

bool ConnectionPool::init(const std::string& host, const std::string& user,
                          const std::string& password, const std::string& dbname,
                          int port, int poolSize)
{
    _host = host;
    _user = user;
    _password = password;
    _dbname = dbname;
    _port = port;
    _poolSize = poolSize;

    for (int i = 0; i < poolSize; ++i)
    {
        MYSQL* conn = createConnection();
        if (!conn)
        {
            LOG_ERROR << "ConnectionPool: 创建第" << i << "个连接失败";
            return false;
        }
        _pool.push(conn);
    }

    _initialized = true;
    LOG_INFO << "ConnectionPool: 成功创建" << poolSize << "个连接";
    return true;
}

MYSQL* ConnectionPool::createConnection()
{
    MYSQL* conn = mysql_init(nullptr);
    if (!conn) return nullptr;

    // 设置自动重连
    bool reconnect = 1;
    mysql_options(conn, MYSQL_OPT_RECONNECT, &reconnect);

    // 设置连接超时
    int timeout = 3;
    mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);

    MYSQL* p = mysql_real_connect(conn, _host.c_str(), _user.c_str(),
                                   _password.c_str(), _dbname.c_str(),
                                   _port, nullptr, 0);
    if (!p)
    {
        LOG_ERROR << "ConnectionPool: 连接失败: " << mysql_error(conn);
        mysql_close(conn);
        return nullptr;
    }

    mysql_query(conn, "set names utf8mb3");
    return conn;
}

MYSQL* ConnectionPool::acquire(int timeoutMs)
{
     MYSQL* conn = nullptr;
    {
        std::unique_lock<std::mutex> lock(_mtx);
        if (!_cv.wait_for(lock, std::chrono::milliseconds(timeoutMs),[this] { return !_pool.empty(); }))
        {
            LOG_ERROR << "ConnectionPool: 获取连接超时 (" << timeoutMs << "ms)";
            return nullptr;
        }

        conn = _pool.front();
        _pool.pop();
    }
    
    // 健康检查：如果连接断开，重新创建
    if (mysql_ping(conn) != 0)
    {
        LOG_WARN << "ConnectionPool: 连接已断开，重新创建";
        mysql_close(conn);
        conn = createConnection();
        if (!conn)
        {
            LOG_ERROR << "ConnectionPool: 重连失败";
            return nullptr;
        }
    }

    return conn;
}

void ConnectionPool::release(MYSQL* conn)
{
    if (!conn) return;
    {
        std::lock_guard<std::mutex> lock(_mtx);
        _pool.push(conn);
    }
    _cv.notify_one();
}

ConnectionPool::~ConnectionPool()
{
    std::lock_guard<std::mutex> lock(_mtx);
    while (!_pool.empty())
    {
        MYSQL* conn = _pool.front();
        _pool.pop();
        mysql_close(conn);
    }
}