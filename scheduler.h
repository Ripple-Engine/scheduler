#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <optional>

#include <list>
#include <vector>
#include <random>
#include "circular_buffer.h"

#include <functional>
#include <coroutine>

#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

class Scheduler {
private:

/**
 * @brief represents an inter-job communication channel to check for completion
 */
class Channel {
protected:

    std::atomic<size_t> num_waiting_on;

public:

    Channel() :
        num_waiting_on(0)
    {}

}; /* class Channel */

/**
 * @brief the interface of a Channel as viewed from the owning job
 */
class Channel_Self : public Channel {
public:

    void signal_spawn(size_t num_spawn) {
        num_waiting_on += num_spawn;
    }

    void signal_spawn() {
        signal_spawn(1);
    }

    bool is_ready() {
        size_t expected = 0;

        return num_waiting_on.compare_exchange_strong(expected, 0);
    }

}; /* class Channel_Self */

/**
 * @brief the interface of a Channel as viewed from a child job
 */
class Channel_Parent : public Channel {
public:

    void signal_done() {
        --num_waiting_on;
    }

}; /* class Channel_Parent */

public:

template <typename T>
class Job {
private:

    struct promise_type {

        // the value returned by the coroutine
        T value;

        // communication channel of the coroutine
        Channel channel_curr;

        // pointer to the channel of the parent coroutine
        Channel* channel_parent;

        promise_type() :
            channel_curr(),
            channel_parent(nullptr)
        {}

        Job get_return_object() {
            // TODO: set channel_parent here
            return Job{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_never initial_suspend() {
            return {};
        }

        std::suspend_always final_suspend() noexcept {
            return {};
        }

        void unhandled_exception() {
            std::terminate();
        }

        template <typename U>
        void return_value(U&& v) {
            value = std::forward<U>(v);
        }

    }; /* struct promise_type */

    friend struct promise_type;

    using Handle = std::coroutine_handle<promise_type>;

    Handle handle;

    Job(Handle h) : handle(h) {}

public:

    Job(const Job&) = delete;

    Job(Job&& other) : handle(other.handle) {
        other.handle = nullptr;
    }

    ~Job() {
        if (handle) {
            handle.destroy();
        }
    }

    Job& operator=(const Job&) = delete;

    Job& operator=(Job&& other) {
        if (this != &other) {
            handle = other.handle;
            other.handle = nullptr;
        }
        return *this;
    }

}; /* class Job<T> */

private:

class Worker {
private:

    /* the current job context */
    // TODO: where do we even set this?
    Channel_Parent* channel_curr;

    Circular_Buffer<std::coroutine_handle<>> queue_jobs;

    // TODO: special termination job to terminate the worker
    bool flag_term;

public:

    Worker(size_t num_jobs_max) :
        channel_curr(nullptr),
        queue_jobs(num_jobs_max)
    {}

    void work() {
        while (!flag_term) {
            auto job$ = queue_jobs.pop_and_get_front_if_possible();
                
            // if no jobs, then try to steal a job from another worker at random            
            while(!job$) {
                auto& worker = get_random_worker();
                job$ = worker.queue_jobs.steal_if_possible();

                // TODO: fix this busy wait
                if(!job$)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            auto& job = job$.value();

            // process the job
            job.resume();

            if(!job.done()) {
                // TODO: consider whether or not we can block here

            }
        }
    }

}; /* class Worker */

friend class Worker;

static std::vector<Worker> workers;

inline static thread_local Worker* worker$ = nullptr;

static Worker& get_random_worker() {
    static thread_local std::mt19937 gen(std::random_device{}());
    static thread_local std::uniform_int_distribution<size_t> dist(0, workers.size() - 1);

    size_t idx = dist(gen);

    return workers[idx];
}

public:

static void Run(size_t num_hw_threads, size_t num_jobs_max) {

    workers.reserve(num_hw_threads);

    for(size_t i = 0; i < num_hw_threads; ++i) {
        workers.emplace_back(num_jobs_max);
    }

    worker$ = &workers[0];

    // thread pool for the workers, except the first one
    std::vector<std::thread> threads;
    threads.reserve(num_hw_threads - 1);

    // create the worker threads
    for(size_t i = 1; i < num_hw_threads; ++i)
        threads.emplace_back(std::bind(&Worker::work, &workers[i]));

    // run the first worker in the main thread
    workers[0].work();

    for(auto& thread : threads)
        thread.join();

}
    
}; /* class Scheduler */

#endif /* SCHEDULER_H */