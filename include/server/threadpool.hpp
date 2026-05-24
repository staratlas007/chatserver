#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>

class ThreadPool {
public:
    explicit ThreadPool(size_t threads = 4) : _stop(false) {
        for (size_t i = 0; i < threads; ++i) {
            _workers.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(_mtx);
                        _cv.wait(lock, [this] { return _stop || !_tasks.empty(); });
                        if (_stop && _tasks.empty()) return;
                        task = std::move(_tasks.front());
                        _tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(_mtx);
            _stop = true;
        }
        _cv.notify_all();
        for (auto& w : _workers) {
            if (w.joinable()) w.join();
        }
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    template<typename F, typename... Args>
    auto enqueue(F&& f, Args&&... args)
        -> std::future<typename std::result_of<F(Args...)>::type> {
        using returnType = typename std::result_of<F(Args...)>::type;

        auto task = std::make_shared<std::packaged_task<returnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));

        std::future<returnType> result = task->get_future();
        {
            std::lock_guard<std::mutex> lock(_mtx);
            _tasks.emplace([task]() { (*task)(); });
        }
        _cv.notify_one();
        return result;
    }

private:
    std::vector<std::thread> _workers;
    std::queue<std::function<void()>> _tasks;
    std::mutex _mtx;
    std::condition_variable _cv;
    bool _stop;
};

#endif
