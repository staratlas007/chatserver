#include "usermodel.hpp"
#include "mysql_prepared_stmt.h"
#include "bcrypt/BCrypt.hpp"
#include "connection_pool.h"

//User表的增加方法
bool UserModel::insert(User& user)
{
    string sql = "insert into User(name, password, state) values(?, ?, ?)";
    
    string hashpwd = BCrypt::generateHash(user.getPwd(),6);

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return false;
    }

    PreparedStmt stmt(conn, sql);
    stmt.bind_str(1, user.getName());
    stmt.bind_str(2, hashpwd);
    stmt.bind_str(3, user.getState());

    if(stmt.execute())
    {
        user.setID(stmt.lastInsertId());
        return true;
    }

    return false;
}

//根据用户id查询用户信息
User UserModel::query(int id)
{
    string sql = "select * from User where id = ?";

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return User();
    }

    PreparedStmt stmt(conn, sql);
    stmt.bind_int(1, id);

    if(!stmt.execute())
    {
        return User();
    }

    SimpleResultSet srs(stmt.getStmt());

    if(srs.next())
    {
        User user;
        user.setID(srs.getInt(0));      
        user.setName(srs.getString(1)); 
        user.setPwd(srs.getString(2));   
        user.setState(srs.getString(3));
        return user;
    }

    return User();
}

//更新用户状态信息
void UserModel::updateState(User& user)
{
    string sql = "update User set state = ? where id = ?";
    
    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return;
    }
    
    PreparedStmt stmt(conn, sql);
    stmt.bind_str(1, user.getState());   
    stmt.bind_int(2, user.getID());      
    stmt.execute();

    return;
}

 //重置用户状态信息
void UserModel::resetState()
{                                                                                                                           
    string sql = "update User set state = 'offline' where state = 'online'";

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return;
    }
    PreparedStmt stmt(conn, sql);

    stmt.execute();

    return;
}

