#include "mysql_prepared_stmt.h"
#include <muduo/base/Logging.h>

PreparedStmt::PreparedStmt(MYSQL* conn, const std::string& sql) 
{
    _stmt = mysql_stmt_init(conn);
    mysql_stmt_prepare(_stmt, sql.c_str(), sql.size());
}
    
PreparedStmt::~PreparedStmt() 
{
    if(_stmt)
    {
        mysql_stmt_close(_stmt);
    }
}
    
//绑定整型参数
void PreparedStmt::bind_int(int index, int value) 
{
    ensure_size(index);
    _int_values[index-1] = value;
    _binds[index-1].buffer_type = MYSQL_TYPE_LONG;
    _binds[index-1].buffer = &_int_values[index-1];
}

//绑定字符串参数
void PreparedStmt::bind_str(int index, const std::string& value) 
{
    ensure_size(index);
    _str_values[index-1] = value;
    _binds[index-1].buffer_type = MYSQL_TYPE_STRING;
    _binds[index-1].buffer = &_str_values[index-1][0];
    _binds[index-1].buffer_length = _str_values[index-1].size();
}

//执行并传递参数
bool PreparedStmt::execute() 
{
    size_t str_idx = 0;
    size_t int_idx = 0;

    for (size_t i = 0; i < _binds.size(); i++) 
    {
        switch (_binds[i].buffer_type) 
        {
            case MYSQL_TYPE_STRING:
                if (str_idx < _str_values.size()) 
                {
                    _binds[i].buffer = &_str_values[i][0];
                    _binds[i].buffer_length = _str_values[i].size();
                    str_idx++;
                }
                break;
                
            case MYSQL_TYPE_LONG:
                if (int_idx < _int_values.size()) 
                {
                    _binds[i].buffer = &_int_values[i];
                    int_idx++;
                }
                break;
        }
    }

    if(!_binds.empty())
    {
        mysql_stmt_bind_param(_stmt, _binds.data());
    }
    bool state = (mysql_stmt_execute(_stmt) == 0);

    LOG_ERROR << "错误信息：" << mysql_stmt_error(_stmt);
    return state; 
}

//返回最后一次查询操作生成的自增id
long long PreparedStmt::lastInsertId() 
{
    return mysql_stmt_insert_id(_stmt);
}

//确保数组有足够空间
void PreparedStmt::ensure_size(int index) 
{
    size_t need = index;
    if (_binds.size() < need) {
        _binds.resize(need);
        _int_values.resize(need);
        _str_values.resize(need);
    }
}

//获取句柄指针
MYSQL_STMT* PreparedStmt::getStmt() const
{
    return _stmt;
}


 SimpleResultSet::SimpleResultSet(MYSQL_STMT* stmt)
 {
    this->_stmt = stmt;
    _metadata = mysql_stmt_result_metadata(stmt);
    _num_fields = mysql_num_fields(_metadata);
    _binds.resize(_num_fields);
    _buffers.resize(_num_fields);
    _lengths.resize(_num_fields);

    for(int i = 0; i < _num_fields; i++)
    {
        _buffers[i].resize(1024);
        _binds[i].buffer_type = MYSQL_TYPE_STRING;
        _binds[i].buffer = &_buffers[i][0];
        _binds[i].buffer_length = _buffers[i].size();
        _binds[i].length = &_lengths[i];
    }

    mysql_stmt_bind_result(stmt, _binds.data());
 }

SimpleResultSet::~SimpleResultSet()
{
    if (_metadata) 
    {
        mysql_free_result(_metadata);  
        _metadata = nullptr;
    }
}

//获取下一行数据
bool SimpleResultSet::next()
{
    return mysql_stmt_fetch(_stmt) == 0;
}

//获取字符串
std::string SimpleResultSet::getString(int col)
{
    return std::string(_buffers[col].data(), _lengths[col]);
}

//获取整数
int SimpleResultSet::getInt(int col)
{
    return atoi(getString(col).c_str());
}