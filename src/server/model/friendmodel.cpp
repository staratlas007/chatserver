#include "friendmodel.hpp"
#include "mysql_prepared_stmt.h"
#include "connection_pool.h"

//添加好友关系
bool FriendModel::insert(int userid, int friendid)
{
    string sql = "insert into Friend values(?, ?)";

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return false;
    }

    PreparedStmt stmt(conn, sql);
    stmt.bind_int(1, userid);
    stmt.bind_int(2, friendid);

    return stmt.execute();
}

//返回用户好友列表
vector<User> FriendModel::query(int userid)
{
    string sql = "select a.id,a.name,a.state from User a inner join Friend b on b.friendid = a.id where b.userid = ?";
    
    vector<User> vec;

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return vec;
    }

    PreparedStmt stmt(conn, sql);
    stmt.bind_int(1, userid);

    if (!stmt.execute())
    {
        return vec;
    }

    SimpleResultSet srs(stmt.getStmt());

    while(srs.next())
    {
        User user;
        user.setID(srs.getInt(0));      
        user.setName(srs.getString(1)); 
        user.setState(srs.getString(2)); 
        vec.push_back(user);
    }

    return vec;
}