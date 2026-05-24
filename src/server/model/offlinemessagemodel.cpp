#include "offlinemessagemodel.hpp"
#include "mysql_prepared_stmt.h"
#include "connection_pool.h"

//存储离线消息
bool OfflineMsgModel::insert(int userid, string msg)
{
    string sql = "insert into OfflineMessage values(?, ?)";

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return false;
    }

    PreparedStmt stmt(conn, sql);
    stmt.bind_int(1, userid);
    stmt.bind_str(2, msg);

    if(stmt.execute())
    {
        return true;
    }

    return false;
}

//删除离线消息
bool OfflineMsgModel::remove(int userid)
{
    string sql = "delete from OfflineMessage where userid = ?";

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return false;
    }

    PreparedStmt stmt(conn, sql);
    stmt.bind_int(1, userid);

    if(stmt.execute())
    {
        return true;
    }

    return false;
}

//查询用户的离线消息
vector<string> OfflineMsgModel::query(int userid)
{
    string sql = "select message from OfflineMessage where userid = ?";
    
    vector<string> vec;

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return vec;
    }
    
    PreparedStmt stmt(conn, sql);
    stmt.bind_int(1, userid);
    
    if (!stmt.execute()) {
        return vec;
    }
    
    SimpleResultSet srs(stmt.getStmt());
    
    while (srs.next()) {
        vec.push_back(srs.getString(0)); 
    }
    
    return vec;
}