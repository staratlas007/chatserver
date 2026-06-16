# chatserver

基于 muduo 网络库实现的集群聊天服务器，支持 nginx TCP 负载均衡与 Redis 跨节点消息同步。

## 项目简介

本项目是一个 **C++ 集群聊天服务器**，采用 muduo 网络库的 Reactor 多线程模型，配合 nginx TCP 负载均衡实现水平扩展，通过 Redis 发布/订阅机制完成跨服务器节点的消息路由，使用 MySQL 持久化用户、好友、群组及离线消息数据。

### 已实现功能

-  **用户注册与登录** — BCrypt 密码哈希, 登录态管理
-  **一对一聊天** — 在线直达 / 跨服务器 Redis 路由 / 离线消息存储
-  **群组管理** — 创建群组、加入群组
-  **群组聊天** — 群内广播, 支持在线 + 离线 + 跨节点三种路径
-  **好友管理** — 添加好友, 查看好友列表及在线状态
-  **离线消息** — 离线时消息存入 MySQL, 登录后自动推送并删除
-  **终端客户端** — 带菜单的完整 TUI 聊天客户端
-  **压力测试工具** — 基于 epoll + 状态机的并发压测客户端 

### 技术栈

| 层次 | 技术 | 用途 |
|---|---|---|
| 网络框架 | [muduo](https://github.com/chenshuo/muduo) | Reactor 事件循环、TCP 连接管理、定时器、日志 |
| 数据库 | MySQL 8.0 | 用户/好友/群组/离线消息持久化 |
| 缓存/消息中间件 | Redis (hiredis) | 跨服务器节点 Pub/Sub 消息同步 |
| 密码安全 | BCrypt | 密码哈希与验证 (cost=12) |
| JSON | [nlohmann/json](https://github.com/nlohmann/json) | 通信消息序列化/反序列化 |
| 负载均衡 | nginx (stream 模块) | TCP 四层负载均衡, 分发客户端连接到各 ChatServer |
| 并发 | muduo EventLoopThreadPool + 自定义 ThreadPool | I/O 线程池 + 阻塞任务线程池 (卸出 BCrypt/DB) |
| 构建 | CMake 3.0+ | 跨平台构建系统 |
| 语言标准 | C++14 | GCC (Linux) |

### 环境要求

| 依赖 | 版本 | 说明 |
|---|---|---|
| OS | Linux (Ubuntu 20.04+ / CentOS 7+) | Windows 不可用 (POSIX API) |
| GCC | 7.0+ | 需要 C++14 支持 |
| CMake | 3.0+ | 构建系统 |
| muduo | master 分支 | 从 GitHub 源码编译安装 |
| MySQL | 8.0 | 数据持久化 |
| Redis | 5.0+ | 跨节点消息中间件 |
| hiredis | — | Redis C 客户端 (apt install) |
| libmysqlclient | — | MySQL C 连接器 (apt install) |
| BCrypt | — | 已内嵌在 `thirdparty/libbcrypt/` 中, 无需单独安装 |

### 安装依赖 (Ubuntu)

```bash
# 基础编译工具
sudo apt update
sudo apt install -y g++ cmake make git

# muduo 网络库
git clone https://github.com/chenshuo/muduo.git
cd muduo && ./build.sh && sudo ./build.sh install
cd ..

# MySQL 开发库
sudo apt install -y libmysqlclient-dev mysql-server

# Redis + hiredis
sudo apt install -y redis-server libhiredis-dev

# 启动服务
sudo systemctl start mysql redis-server
```

### 初始化数据库

```sql
-- 创建数据库
CREATE DATABASE chat;

USE chat;

-- 用户表
CREATE TABLE User (
    id       INT AUTO_INCREMENT PRIMARY KEY,
    name     VARCHAR(50)  NOT NULL,
    password VARCHAR(255) NOT NULL,
    state    VARCHAR(10)  DEFAULT 'offline'
);

-- 好友表
CREATE TABLE Friend (
    userid   INT NOT NULL,
    friendid INT NOT NULL,
    PRIMARY KEY (userid, friendid)
);

-- 群组表
CREATE TABLE AllGroup (
    id       INT AUTO_INCREMENT PRIMARY KEY,
    groupname VARCHAR(100) NOT NULL,
    groupdesc VARCHAR(200) DEFAULT ''
);

-- 群组成员表
CREATE TABLE GroupUser (
    groupid INT NOT NULL,
    userid  INT NOT NULL,
    grouprole VARCHAR(20) DEFAULT 'normal',
    PRIMARY KEY (groupid, userid)
);

-- 离线消息表
CREATE TABLE OfflineMessage (
    userid  INT  NOT NULL,
    message TEXT NOT NULL
);
```

### 编译

```bash
cd chatserver
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

### 运行

**1. 单节点部署 (开发调试)**

```bash
# 启动服务端 
./bin/ChatServer 127.0.0.1 6000

# 启动客户端 
./bin/ChatClient 127.0.0.1 6000
```

**2. 集群部署 (nginx 负载均衡)**

```nginx
# nginx.conf — stream 模块
stream {
    upstream MyServer {
        hash $remote_addr consistent;

        server 127.0.0.1:6000 max_fails=3 fail_timeout=30s;
        server 127.0.0.1:6002 max_fails=3 fail_timeout=30s;
    }
    server {
        listen 8000;
        proxy_pass MyServer;
    }

}
```

```bash
# 启动 2 个 ChatServer 实例
./bin/ChatServer 127.0.0.1 6000 
./bin/ChatServer 127.0.0.1 6002 

# 客户端连接 nginx 入口
./bin/ChatClient 127.0.0.1 8000
```

---

### 许可证

本项目仅用于学习交流。


