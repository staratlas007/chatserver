/**
 * chatserver 压测客户端
 *
 * 设计:
 *   单线程 epoll + 非阻塞 socket + 状态机驱动
 *   一个进程可支撑数千并发虚拟用户, 无线程切换开销
 *
 *
 * 用法:
 *   ./stress_client <ip> <port> [-c N] [-d N] [-r N] [-s N] [-a]
 *
 * 选项:
 *   -c N    并发连接数 (默认 100)
 *   -d N    压测持续时间秒 (默认 60)
 *   -r N    全局消息速率上限/秒 (默认 0=不限)
 *   -s N    场景: 0=纯连接, 1=登录, 2=登录+单聊, 3=混合 (默认 2)
 *   -a      自注册模式: 先注册新用户再登录, 无需预置数据库用户
 */

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <cassert>
#include <ctime>
#include <cmath>
#include <vector>
#include <unordered_map>
#include <deque>
#include <string>
#include <chrono>
#include <atomic>
#include <random>
#include <algorithm>
#include <iostream>
#include <iomanip>

// ── 单文件内嵌 nlohmann/json ────────────────────────────────────
#include "json.hpp"

// ══════════════════════════════════════════════════════════════════
// 消息类型 (与服务端 public.hpp 保持一致)
// ══════════════════════════════════════════════════════════════════
enum MsgType : int {
    LOGIN_MSG       = 1,
    LOGIN_MSG_ACK   = 2,
    REG_MSG         = 3,
    REG_MSG_ACK     = 4,
    ONE_CHAT_MSG    = 5,
    ADD_FRIEND_MSG  = 6,
    CREATE_GROUP_MSG= 7,
    ADD_GROUP_MSG   = 8,
    GROUP_CHAT_MSG  = 9,
    LOGINOUT_MSG    = 10,
    RESPONSE        = 11,
};

// ══════════════════════════════════════════════════════════════════
// 统计模块
// ══════════════════════════════════════════════════════════════════
struct LatencyRecorder {
    std::vector<double> samples;
    void record(double ms) { samples.push_back(ms); }
    double p50() const {
        if (samples.empty()) return 0;
        auto v = samples; std::sort(v.begin(), v.end());
        return v[v.size() * 50 / 100];
    }
    double p95() const {
        if (samples.empty()) return 0;
        auto v = samples; std::sort(v.begin(), v.end());
        return v[v.size() * 95 / 100];
    }
    double p99() const {
        if (samples.empty()) return 0;
        auto v = samples; std::sort(v.begin(), v.end());
        return v[v.size() * 99 / 100];
    }
    double avg() const {
        if (samples.empty()) return 0;
        double sum = 0;
        for (double s : samples) sum += s;
        return sum / samples.size();
    }
    size_t count() const { return samples.size(); }
};

struct GlobalStats {
    std::atomic<uint64_t> connects{0};
    std::atomic<uint64_t> connectFail{0};
    std::atomic<uint64_t> disconnects{0};
    std::atomic<uint64_t> loginSent{0};
    std::atomic<uint64_t> loginOk{0};
    std::atomic<uint64_t> loginFail{0};
    std::atomic<uint64_t> regSent{0};
    std::atomic<uint64_t> regOk{0};
    std::atomic<uint64_t> regFail{0};
    std::atomic<uint64_t> chatSent{0};
    std::atomic<uint64_t> chatRecv{0};
    std::atomic<uint64_t> bytesSent{0};
    std::atomic<uint64_t> bytesRecv{0};
    std::atomic<uint64_t> errors{0};

    LatencyRecorder regLatency;
    LatencyRecorder loginLatency;
    LatencyRecorder chatLatency;

    void report(std::chrono::steady_clock::time_point start) const {
        auto elapsed = std::chrono::steady_clock::now() - start;
        double sec = std::chrono::duration<double>(elapsed).count();
        if (sec < 0.1) sec = 0.1;

        std::cout << "\n";
        std::cout << "══════════════════════════════════════════\n";
        std::cout << " 压测报告  (elapsed=" << std::fixed << std::setprecision(1)
                  << sec << "s)\n";
        std::cout << "──────────────────────────────────────────\n";
        std::cout << std::left << std::setw(28) << "  连接成功/失败"
                  << connects << " / " << connectFail
                  << "  (断开:" << disconnects << ")\n";
        if (regSent > 0) {
            std::cout << std::left << std::setw(28) << "  注册 发送/成功/失败"
                      << regSent << " / " << regOk << " / " << regFail
                      << "  (" << std::setprecision(0) << regOk/sec << "/s)\n";
        }
        std::cout << std::left << std::setw(28) << "  登录 发送/成功/失败"
                  << loginSent << " / " << loginOk << " / " << loginFail
                  << "  (" << std::setprecision(0) << loginOk/sec << "/s)\n";
        std::cout << std::left << std::setw(28) << "  单聊 发送/接收"
                  << chatSent << " / " << chatRecv
                  << "  (" << std::setprecision(0) << chatRecv/sec << "/s)\n";
        std::cout << std::left << std::setw(28) << "  网络 发送/接收"
                  << (bytesSent/1024/1024) << "MB / "
                  << (bytesRecv/1024/1024) << "MB\n";
        std::cout << std::left << std::setw(28) << "  错误数" << errors << "\n";
        std::cout << "──────────────────────────────────────────\n";

        if (regLatency.count() > 0) {
            std::cout << "  注册延迟 (ms)\n";
            std::cout << "    avg=" << std::setprecision(1) << regLatency.avg()
                      << "  p50=" << regLatency.p50()
                      << "  p95=" << regLatency.p95()
                      << "  p99=" << regLatency.p99()
                      << "  samples=" << regLatency.count() << "\n";
        }
        if (loginLatency.count() > 0) {
            std::cout << "  登录延迟 (ms)\n";
            std::cout << "    avg=" << std::setprecision(1) << loginLatency.avg()
                      << "  p50=" << loginLatency.p50()
                      << "  p95=" << loginLatency.p95()
                      << "  p99=" << loginLatency.p99()
                      << "  samples=" << loginLatency.count() << "\n";
        }
        if (chatLatency.count() > 0) {
            std::cout << "  单聊延迟 (ms)\n";
            std::cout << "    avg=" << std::setprecision(1) << chatLatency.avg()
                      << "  p50=" << chatLatency.p50()
                      << "  p95=" << chatLatency.p95()
                      << "  p99=" << chatLatency.p99()
                      << "  samples=" << chatLatency.count() << "\n";
        }
        std::cout << "══════════════════════════════════════════\n"
                  << std::flush;
    }
};

// 全局统计实例
GlobalStats g_stats;

// ══════════════════════════════════════════════════════════════════
// 工具函数
// ══════════════════════════════════════════════════════════════════
int setNonBlock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int setNoDelay(int fd) {
    int opt = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
}

uint64_t nowUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ══════════════════════════════════════════════════════════════════
// 虚拟用户连接 (状态机)
// ══════════════════════════════════════════════════════════════════
struct VirtualUser {
    enum State : uint8_t {
        IDLE,           // 尚未建立连接
        CONNECTING,     // connect() 已调用, 等待 EPOLLOUT
        CONNECTED,      // TCP 连接已建立, 待发送业务请求
        REG_SENT,       // 注册请求已发出, 等待 REG_MSG_ACK
        LOGIN_SENT,     // 登录请求已发出, 等待 LOGIN_MSG_ACK
        LOGGED_IN,      // 登录成功, 可以进行聊天
        DONE,           // 完成, 待关闭
    };

    int fd = -1;
    State state = IDLE;
    int userId = 0;             // 服务端分配/使用的用户 ID

    // 自注册模式下的名字和密码
    std::string regName;
    std::string regPassword;

    // 发送缓冲区
    std::string sendBuf;
    size_t sendOff = 0;

    // 接收缓冲区 (null 分隔帧)
    std::string recvBuf;

    // 计时 (用于计算延迟)
    uint64_t regSendUs   = 0;
    uint64_t loginSendUs = 0;
    uint64_t chatSendUs  = 0;

    // 聊天目标
    int chatTarget = 0;

    // 状态机控制
    uint64_t nextActionUs = 0;
    int chatCount = 0;
    int maxChats = 10;

    // 标记已收到响应 (防止多次处理)
    bool regAckReceived   = false;
    bool loginAckReceived = false;

    void reset() {
        if (fd >= 0) { close(fd); fd = -1; }
        state = IDLE;
        sendBuf.clear(); sendOff = 0;
        recvBuf.clear();
        regSendUs = loginSendUs = chatSendUs = 0;
        chatTarget = 0;
        nextActionUs = 0;
        chatCount = 0;
        regAckReceived = false;
        loginAckReceived = false;
    }
};

// ══════════════════════════════════════════════════════════════════
// Epoll 事件循环
// ══════════════════════════════════════════════════════════════════
class EventLoop {
public:
    EventLoop() {
        _epfd = epoll_create1(0);
        assert(_epfd >= 0);
    }
    ~EventLoop() { close(_epfd); }

    void add(int fd, uint32_t events, void* ptr) {
        struct epoll_event ev;
        ev.events = events | EPOLLET;
        ev.data.ptr = ptr;
        int ret = epoll_ctl(_epfd, EPOLL_CTL_ADD, fd, &ev);
        assert(ret == 0);
    }

    void mod(int fd, uint32_t events, void* ptr) {
        struct epoll_event ev;
        ev.events = events | EPOLLET;
        ev.data.ptr = ptr;
        epoll_ctl(_epfd, EPOLL_CTL_MOD, fd, &ev);
    }

    void del(int fd) {
        epoll_ctl(_epfd, EPOLL_CTL_DEL, fd, nullptr);
    }

    void run(int timeoutMs) {
        struct epoll_event events[256];
        int n = epoll_wait(_epfd, events, 256, timeoutMs);
        for (int i = 0; i < n; ++i) {
            auto* vu = static_cast<VirtualUser*>(events[i].data.ptr);
            uint32_t ev = events[i].events;

            if (ev & (EPOLLERR | EPOLLHUP)) {
                handleError(vu);
                continue;
            }
            if (ev & EPOLLOUT) {
                handleWrite(vu);
            }
            if (ev & EPOLLIN) {
                handleRead(vu);
            }
        }
    }

    std::function<void(VirtualUser*)> onConnected;
    std::function<void(VirtualUser*, const std::string&)> onMessage;
    std::function<void(VirtualUser*)> onError;

private:
    int _epfd;

    void handleWrite(VirtualUser* vu) {
        if (vu->state == VirtualUser::CONNECTING) {
            int err = 0;
            socklen_t len = sizeof(err);
            getsockopt(vu->fd, SOL_SOCKET, SO_ERROR, &err, &len);
            if (err != 0) {
                g_stats.connectFail++;
                g_stats.errors++;
                vu->reset();
                return;
            }
            vu->state = VirtualUser::CONNECTED;
            setNoDelay(vu->fd);
            g_stats.connects++;
            if (onConnected) onConnected(vu);
            flushSend(vu);
            return;
        }
        flushSend(vu);
    }

    void handleRead(VirtualUser* vu) {
        char buf[4096];
        for (;;) {
            ssize_t n = recv(vu->fd, buf, sizeof(buf), 0);
            if (n > 0) {
                vu->recvBuf.append(buf, n);
                g_stats.bytesRecv += n;
                processMessages(vu);
            } else if (n == 0) {
                g_stats.disconnects++;
                vu->state = VirtualUser::DONE;
                if (onError) onError(vu);
                return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                g_stats.errors++;
                if (onError) onError(vu);
                vu->reset();
                return;
            }
        }
    }

    void handleError(VirtualUser* vu) {
        g_stats.errors++;
        if (onError) onError(vu);
        vu->reset();
    }

    void processMessages(VirtualUser* vu) {
        while (true) {
            size_t pos = vu->recvBuf.find('\0');
            if (pos == std::string::npos) break;
            std::string msg(vu->recvBuf, 0, pos);
            vu->recvBuf.erase(0, pos + 1);
            if (onMessage) onMessage(vu, msg);
        }
    }

    void flushSend(VirtualUser* vu) {
        if (vu->sendOff >= vu->sendBuf.size()) {
            vu->sendBuf.clear();
            vu->sendOff = 0;
            return;
        }
        while (vu->sendOff < vu->sendBuf.size()) {
            ssize_t n = send(vu->fd,
                             vu->sendBuf.data() + vu->sendOff,
                             vu->sendBuf.size() - vu->sendOff,
                             MSG_NOSIGNAL);
            if (n > 0) {
                vu->sendOff += n;
                g_stats.bytesSent += n;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    struct epoll_event ev_out;
                    ev_out.events = EPOLLIN | EPOLLOUT | EPOLLET;
                    ev_out.data.ptr = vu;
                    epoll_ctl(_epfd, EPOLL_CTL_MOD, vu->fd, &ev_out);
                    return;
                }
                g_stats.errors++;
                vu->reset();
                return;
            }
        }
        vu->sendBuf.clear();
        vu->sendOff = 0;
        struct epoll_event ev_in;
        ev_in.events = EPOLLIN | EPOLLET;
        ev_in.data.ptr = vu;
        epoll_ctl(_epfd, EPOLL_CTL_MOD, vu->fd, &ev_in);
    }
};

// ══════════════════════════════════════════════════════════════════
// 发送消息辅助
// ══════════════════════════════════════════════════════════════════
void sendJson(VirtualUser* vu, const nlohmann::json& js) {
    std::string data = js.dump();
    data.push_back('\0');
    vu->sendBuf += data;
}

// ══════════════════════════════════════════════════════════════════
// 业务回调: 连接建立 → 发送第一条业务请求
// ══════════════════════════════════════════════════════════════════
void onConnectedCb(VirtualUser* vu, int scenario, bool autoRegister) {
    if (scenario == 0) {
        return; // 纯连接压测
    }

    if (autoRegister) {
        // ── 自注册模式: 先注册, 成功后自动登录 ──
        nlohmann::json js;
        js["msgid"]   = REG_MSG;
        js["name"]    = vu->regName;
        js["password"]= vu->regPassword;
        sendJson(vu, js);
        vu->state = VirtualUser::REG_SENT;
        vu->regSendUs = nowUs();
        g_stats.regSent++;
        return;
    }

    // ── 普通模式: 直接登录 ──
    if (scenario >= 3 && (rand() % 10 == 0)) {
        nlohmann::json js;
        js["msgid"]   = REG_MSG;
        js["name"]    = "stress_" + std::to_string(vu->userId);
        js["password"]= "123456";
        sendJson(vu, js);
        vu->state = VirtualUser::REG_SENT;
        vu->regSendUs = nowUs();
        g_stats.regSent++;
    } else {
        nlohmann::json js;
        js["msgid"]   = LOGIN_MSG;
        js["id"]      = vu->userId;
        js["password"]= "123456";
        sendJson(vu, js);
        vu->state = VirtualUser::LOGIN_SENT;
        vu->loginSendUs = nowUs();
        g_stats.loginSent++;
    }
}

// ══════════════════════════════════════════════════════════════════
// 辅助: 发送登录消息 (复用: 注册成功后 → 自动登录)
// ══════════════════════════════════════════════════════════════════
void sendLogin(VirtualUser* vu) {
    nlohmann::json js;
    js["msgid"]   = LOGIN_MSG;
    js["id"]      = vu->userId;          // 注册阶段已更新为服务端分配的 ID
    js["password"]= vu->regPassword;
    sendJson(vu, js);
    vu->state = VirtualUser::LOGIN_SENT;
    vu->loginSendUs = nowUs();
    g_stats.loginSent++;
}

// ══════════════════════════════════════════════════════════════════
// 辅助: 登录成功后初始化聊天参数
// ══════════════════════════════════════════════════════════════════
void onLoginSuccess(VirtualUser* vu, VirtualUser* peerPool, int poolSize) {
    g_stats.loginOk++;
    vu->state = VirtualUser::LOGGED_IN;
    // 用池内下标定位目标, 取对方的服务端 ID, 而非取模
    int myIdx = static_cast<int>(vu - peerPool);
    int peerIdx = (myIdx + 1 + rand() % (poolSize - 1)) % poolSize;
    if (peerIdx == myIdx) peerIdx = (peerIdx + 1) % poolSize;
    vu->chatTarget = peerPool[peerIdx].userId;
    vu->maxChats = 2 + rand() % 8;
    vu->nextActionUs = nowUs() + (1000 + rand() % 4000) * 1000;
}

// ══════════════════════════════════════════════════════════════════
// 业务回调: 收到消息 → 分发处理
// ══════════════════════════════════════════════════════════════════
void onMessageCb(VirtualUser* vu, const std::string& raw,
                 int scenario, VirtualUser* peerPool, int poolSize,
                 bool autoRegister) {
    auto js = nlohmann::json::parse(raw, nullptr, false);
    if (js.is_discarded()) return;

    int msgid = js.value("msgid", -1);

    // ── 注册响应 ──
    if (msgid == REG_MSG_ACK) {
        if (vu->regAckReceived) return;
        vu->regAckReceived = true;

        double latMs = (nowUs() - vu->regSendUs) / 1000.0;
        g_stats.regLatency.record(latMs);

        if (js.value("errno", -1) == 0) {
            g_stats.regOk++;
            if (autoRegister) {
                // 自注册模式: 提取服务端分配的 ID, 立即登录
                vu->userId = js.value("id", 0);
                sendLogin(vu);
            } else {
                // 普通混合场景: 注册完就结束
                vu->state = VirtualUser::DONE;
                vu->nextActionUs = nowUs();
            }
        } else {
            g_stats.regFail++;
            vu->state = VirtualUser::DONE;
            vu->nextActionUs = nowUs();
        }
        return;
    }

    // ── 登录响应 ──
    if (msgid == LOGIN_MSG_ACK) {
        if (vu->loginAckReceived) return;
        vu->loginAckReceived = true;

        double latMs = (nowUs() - vu->loginSendUs) / 1000.0;
        g_stats.loginLatency.record(latMs);

        if (js.value("errno", -1) == 0) {
            onLoginSuccess(vu, peerPool, poolSize);
        } else {
            g_stats.loginFail++;
            vu->state = VirtualUser::DONE;
            vu->nextActionUs = nowUs();
        }
        return;
    }

    // ── 单聊消息 ──
    if (msgid == ONE_CHAT_MSG) {
        g_stats.chatRecv++;
        if (vu->chatSendUs > 0) {
            double latMs = (nowUs() - vu->chatSendUs) / 1000.0;
            g_stats.chatLatency.record(latMs);
        }
        return;
    }

    // ── 通用响应 ──
    if (msgid == RESPONSE) {
        return;
    }
}

// ══════════════════════════════════════════════════════════════════
// 状态机推进: 每帧调用, 决定每个 vu 的下一步动作
// ══════════════════════════════════════════════════════════════════
void tickUser(VirtualUser* vu, int scenario, VirtualUser* pool, int poolSize) {
    uint64_t now = nowUs();
    if (now < vu->nextActionUs) return;

    switch (vu->state) {

    case VirtualUser::LOGGED_IN: {
        if (vu->chatCount >= vu->maxChats) {
            // 发完消息, 登出
            nlohmann::json js;
            js["msgid"] = LOGINOUT_MSG;
            js["id"] = vu->userId;
            sendJson(vu, js);
            vu->state = VirtualUser::DONE;
            vu->nextActionUs = now + 500 * 1000;
            return;
        }

        if (scenario == 2 || scenario == 3) {
            nlohmann::json js;
            js["msgid"] = ONE_CHAT_MSG;
            js["id"]    = vu->userId;
            js["name"]  = "user" + std::to_string(vu->userId);
            js["toid"]  = vu->chatTarget;
            js["msg"]   = "hello_" + std::to_string(vu->chatCount);
            js["time"]  = "2026-01-01 00:00:00";
            sendJson(vu, js);
            vu->chatSendUs = now;
            vu->chatCount++;
            g_stats.chatSent++;
            vu->nextActionUs = now + (500 + rand() % 1500) * 1000;
        } else {
            vu->nextActionUs = now + 10 * 1000 * 1000;
        }
        return;
    }

    case VirtualUser::DONE: {
        close(vu->fd);
        vu->fd = -1;
        vu->state = VirtualUser::IDLE;
        return;
    }

    default:
        break;
    }
}

// ══════════════════════════════════════════════════════════════════
// main
// ══════════════════════════════════════════════════════════════════
int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0]
                  << " <ip> <port> [-c N] [-d N] [-r N] [-s N] [-a]\n\n"
                  << "  -c N  并发连接数        (default: 100)\n"
                  << "  -d N  持续时间秒         (default: 60)\n"
                  << "  -r N  消息速率上限/s      (default: 0=unlimited)\n"
                  << "  -s N  场景: 0=connect 1=login 2=login+chat 3=mixed\n"
                  << "  -a    自注册模式         (先注册再登录, 无需预置数据库用户)\n"
                  << "  -o N  ID/用户名起始偏移    (default: 0, 多客户端时避免冲突)\n\n"
                  << "示例:\n"
                  << "  # 自注册模式: 500并发, 60秒, 无需预置用户\n"
                  << "  " << argv[0] << " 127.0.0.1 6000 -c 500 -d 60 -s 2 -a\n"
                  << "\n"
                  << "  # 普通模式: 需要数据库有预置用户 id=1~1100, 密码123456\n"
                  << "  " << argv[0] << " 127.0.0.1 6000 -c 1000 -d 60 -s 2\n"
                  << "\n"
                  << "  # 两个客户端同时对集群压测 (ID 范围不重叠)\n"
                  << "  " << argv[0] << " 192.168.1.10 6000 -c 500 -d 60 -s 2 -a -o 0     # 客户端A\n"
                  << "  " << argv[0] << " 192.168.1.10 6000 -c 500 -d 60 -s 2 -a -o 10000 # 客户端B\n";
        return 1;
    }

    const char* ip   = argv[1];
    uint16_t   port  = static_cast<uint16_t>(atoi(argv[2]));
    int  conns       = 100;
    int  duration    = 60;
    int  rateLimit   = 0;
    int  scenario    = 2;
    bool autoRegister= false;
    int  idOffset    = 0;

    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "-c") == 0 && i+1 < argc) conns = atoi(argv[++i]);
        else if (strcmp(argv[i], "-d") == 0 && i+1 < argc) duration = atoi(argv[++i]);
        else if (strcmp(argv[i], "-r") == 0 && i+1 < argc) rateLimit = atoi(argv[++i]);
        else if (strcmp(argv[i], "-s") == 0 && i+1 < argc) scenario = atoi(argv[++i]);
        else if (strcmp(argv[i], "-o") == 0 && i+1 < argc) idOffset = atoi(argv[++i]);
        else if (strcmp(argv[i], "-a") == 0) autoRegister = true;
    }

    const char* scenarioNames[] = {"connect", "login", "login+chat", "mixed"};

    std::cout << "╔══════════════════════════════════════════╗\n";
    std::cout << "║  chatserver 压测客户端                   ║\n";
    std::cout << "╠══════════════════════════════════════════╣\n";
    std::cout << "║  target:      " << std::left << std::setw(30)
              << (std::string(ip) + ":" + std::to_string(port)) << "║\n";
    std::cout << "║  connections: " << std::setw(30) << conns << "║\n";
    std::cout << "║  duration:    " << std::setw(30) << duration << "║\n";
    std::cout << "║  scenario:    " << std::setw(30)
              << scenarioNames[scenario] << "║\n";
    std::cout << "║  auto-register: " << std::setw(28)
              << (autoRegister ? "ON" : "OFF") << "║\n";
    std::cout << "║  id-offset:   " << std::setw(30) << idOffset << "║\n";
    if (autoRegister) {
        std::cout << "║  (无需预置数据库用户, 自动注册后登录)    ║\n";
    }
    std::cout << "╚══════════════════════════════════════════╝\n\n";

    srand(static_cast<unsigned>(time(nullptr)));

    // ── 创建虚拟用户池 ──
    static std::atomic<int> g_userIdCounter{1 + idOffset};
    std::vector<VirtualUser> users(conns);

    for (int i = 0; i < conns; i++) {
        if (autoRegister) {
            // 自注册模式: 用户名为 "perf_" + 序号, 初始 userId 为 0
            users[i].userId   = 0;
            users[i].regName     = "perf_" + std::to_string(g_userIdCounter++);
            users[i].regPassword = "123456";
        } else {
            // 普通模式: userId 对应数据库预置用户
            users[i].userId = g_userIdCounter++;
        }
        users[i].state = VirtualUser::IDLE;
    }

    // ── 事件循环 ──
    EventLoop loop;

    loop.onConnected = [&](VirtualUser* vu) {
        onConnectedCb(vu, scenario, autoRegister);
    };

    loop.onMessage = [&](VirtualUser* vu, const std::string& raw) {
        onMessageCb(vu, raw, scenario, users.data(), conns, autoRegister);
    };

    loop.onError = [&](VirtualUser* vu) {
        (void)vu;
    };

    // ── 分批建立连接 ──
    int batchSize = 50;
    int connected = 0;

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    std::cout << "建立连接中...\n" << std::flush;

    while (connected < conns) {
        int batchEnd = std::min(connected + batchSize, conns);
        for (int i = connected; i < batchEnd; i++) {
            int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
            if (fd < 0) {
                g_stats.connectFail++;
                continue;
            }
            setNonBlock(fd);
            int ret = connect(fd, (sockaddr*)&addr, sizeof(addr));
            if (ret < 0 && errno != EINPROGRESS) {
                g_stats.connectFail++;
                close(fd);
                continue;
            }
            users[i].fd = fd;
            users[i].state = VirtualUser::CONNECTING;
            loop.add(fd, EPOLLOUT | EPOLLIN, &users[i]);
        }
        connected = batchEnd;

        auto batchStart = std::chrono::steady_clock::now();
        while (std::chrono::duration<double>(
                   std::chrono::steady_clock::now() - batchStart).count() < 3.0) {
            loop.run(50);
            // 预连接阶段也刷 sendBuf, 避免 loginSendUs 包含排队等发时间
            for (int i = 0; i < connected; i++) {
                if (users[i].sendBuf.empty() || users[i].fd < 0) continue;
                size_t& off = users[i].sendOff;
                while (off < users[i].sendBuf.size()) {
                    ssize_t n = send(users[i].fd,
                                     users[i].sendBuf.data() + off,
                                     users[i].sendBuf.size() - off,
                                     MSG_NOSIGNAL);
                    if (n > 0) { off += n; g_stats.bytesSent += n; }
                    else {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        g_stats.errors++;
                        users[i].reset();
                        break;
                    }
                }
                if (off >= users[i].sendBuf.size()) {
                    users[i].sendBuf.clear();
                    users[i].sendOff = 0;
                } else {
                    loop.mod(users[i].fd, EPOLLIN | EPOLLOUT, &users[i]);
                }
            }
        }
        std::cout << "\r已连接: " << connected << "/" << conns << std::flush;
    }

    std::cout << "\n全部连接建立完成, 开始压测...\n" << std::flush;

    // ── 主压测循环 ──
    auto testStart = std::chrono::steady_clock::now();
    int reportIntervalSec = 5;

    while (true) {
        loop.run(10);

        // 推进状态机 + 尝试直接写
        for (int i = 0; i < conns; i++) {
            tickUser(&users[i], scenario, users.data(), conns);

            if (!users[i].sendBuf.empty() && users[i].fd >= 0) {
                size_t& off = users[i].sendOff;
                while (off < users[i].sendBuf.size()) {
                    ssize_t n = send(users[i].fd,
                                     users[i].sendBuf.data() + off,
                                     users[i].sendBuf.size() - off,
                                     MSG_NOSIGNAL);
                    if (n > 0) {
                        off += n;
                        g_stats.bytesSent += n;
                    } else {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        g_stats.errors++;
                        users[i].reset();
                        break;
                    }
                }
                if (off >= users[i].sendBuf.size()) {
                    users[i].sendBuf.clear();
                    users[i].sendOff = 0;
                } else {
                    loop.mod(users[i].fd, EPOLLIN | EPOLLOUT, &users[i]);
                }
            }
        }

        // 中间报告
        auto elapsed = std::chrono::steady_clock::now() - testStart;
        double elapsedSec = std::chrono::duration<double>(elapsed).count();
        if (elapsedSec >= reportIntervalSec) {
            g_stats.report(testStart);
            reportIntervalSec += 5;
        }

        if (elapsedSec >= duration) break;
    }

    // ── 最终报告 ──
    g_stats.report(testStart);

    // ── 清理 ──
    for (auto& u : users) {
        if (u.fd >= 0) close(u.fd);
    }

    std::cout << "\n压测完成.\n";
    return 0;
}
