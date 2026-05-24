#include "groupmodel.hpp"
#include "mysql_prepared_stmt.h"
#include "connection_pool.h"

//创建群组
bool GroupModel::createGroup(Group& group)
{   
    string sql =  "insert into AllGroup(groupname, groupdesc) values(?, ?)";

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return false;
    }

    PreparedStmt stmt(conn, sql);
    stmt.bind_str(1, group.getName());
    stmt.bind_str(2, group.getDesc());

    if(stmt.execute())
    {
        group.setID(stmt.lastInsertId());
        return true;
    }

    return false;
}

//加入群组
bool GroupModel::addGroup(int userid, int groupid, string role)
{
    string sql =  "insert into GroupUser values(?, ?, ?)";

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return false;
    }

    PreparedStmt stmt(conn, sql);
    stmt.bind_int(1, groupid);
    stmt.bind_int(2, userid);
    stmt.bind_str(3, role);

    if(stmt.execute())
    {
        return true;
    }

    return false;
}

//查询用户所在群组信息
vector<Group> GroupModel::queryGroups(int userid)
{
    vector<Group> groupVec;

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return groupVec;
    }
    
    // 第一步：查询用户所属的群组信息
    string sql1 = "select a.id, a.groupname, a.groupdesc from AllGroup a inner join GroupUser b on a.id = b.groupid where b.userid = ?";
    
    PreparedStmt stmt1(conn, sql1);
    stmt1.bind_int(1, userid);
    
    if (!stmt1.execute()) {
        return groupVec;
    }
    
    SimpleResultSet rs1(stmt1.getStmt());
    
    while (rs1.next()) {
        Group group;
        group.setID(rs1.getInt(0));
        group.setName(rs1.getString(1));
        group.setDesc(rs1.getString(2));
        groupVec.push_back(group);
    }
    
    // 第二步：查询每个群组的成员信息
    string sql2 = "select a.id, a.name, a.state, b.grouprole from User a inner join GroupUser b on b.userid = a.id where b.groupid = ?";
    
    for (Group &group : groupVec) {
        PreparedStmt stmt2(conn, sql2);
        stmt2.bind_int(1, group.getID());
        
        if (!stmt2.execute()) {
            continue;  // 跳过失败的群组
        }
        
        SimpleResultSet rs2(stmt2.getStmt());
        
        while (rs2.next()) {
            GroupUser user;
            user.setID(rs2.getInt(0));
            user.setName(rs2.getString(1));
            user.setState(rs2.getString(2));
            user.setRole(rs2.getString(3));
            group.getUsers().push_back(user);
        }
    }

    return groupVec;
 }

//根据指定groupid查询群组用户id列表，除userid自己，
vector<int> GroupModel::queryGroupUsers(int userid, int groupid)
{
    string sql = "select userid from GroupUser where groupid = ? and userid != ?";

    vector<int> idVec;

    ConnectionGuard guard(ConnectionPool::instance());  
    MYSQL* conn = guard.get();
    if(!conn)
    {
        LOG_ERROR << "获取连接失败";
        return idVec;
    }

    PreparedStmt stmt(conn, sql);
    stmt.bind_int(1, groupid);
    stmt.bind_int(2, userid);

    if(!stmt.execute())
    {
        return idVec;
    }

    SimpleResultSet srs(stmt.getStmt());
    while(srs.next())
    {
        idVec.push_back(srs.getInt(0));
    }

    return idVec;
}