#ifndef MYSQL_PREPARED_STMT_H
#define MYSQL_PREPARED_STMT_H

#include <mysql/mysql.h>
#include <string>
#include <vector>

//mysql预处理
class PreparedStmt {
public:
    PreparedStmt(MYSQL* conn, const std::string& sql);
    
    ~PreparedStmt();
    
    //绑定整型参数
    void bind_int(int index, int value);
    
    //绑定字符串参数
    void bind_str(int index, const std::string& value);
    
    //执行并传递参数
    bool execute();
    
    //返回最后一次查询操作生成的自增id
    long long lastInsertId();

    //确保数组有足够空间
    void ensure_size(int index);

    //获取句柄指针
    MYSQL_STMT* getStmt() const;

private:
    MYSQL_STMT* _stmt;
    std::vector<MYSQL_BIND> _binds;
    std::vector<int> _int_values;
    std::vector<std::string> _str_values;
};

//结果集处理
class SimpleResultSet {
public:
    SimpleResultSet(MYSQL_STMT* stmt);

    ~SimpleResultSet();
    
    //获取下一行数据
    bool next();
    
    //获取字符串
    std::string getString(int col);
    
    //获取整数
    int getInt(int col);

private:
    MYSQL_STMT* _stmt;
    MYSQL_RES* _metadata;
    int _num_fields;
    std::vector<MYSQL_BIND> _binds;
    std::vector<std::string> _buffers;
    std::vector<unsigned long> _lengths;
};

#endif 