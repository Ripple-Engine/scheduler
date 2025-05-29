#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <cassert>
#include <optional>

#include <deque>
#include <vector>
#include <random>
#include "../lib/containers/circular_buffer.h"

#include <functional>
#include <coroutine>

#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <thread>
#include <atomic>

namespace Ripple {

class Scheduler {
private: 

struct Promise_Base {

    std::shared_mutex mut;

    std::atomic<size_t> num_waiting_on;
    Promise_Base* parent;

    Promise_Base();

    inline bool is_unblocked() const noexcept {
        return num_waiting_on.load() == 0;
    }

    inline static Promise_Base* get(std::coroutine_handle<> handle) {
        // TODO: what in the heebus jeebus is there a better way to do this?
        return reinterpret_cast<Promise_Base*>(handle.address());
    }

}; /* struct Promise_Base */

class Worker {
private:

    Circular_Buffer<std::coroutine_handle<>> queue_jobs;
    std::deque<std::coroutine_handle<>> queue_overflow;

    // TODO: special termination job to terminate the worker
    bool flag_term;

    /**
     * @brief Flushes the overflow queue to the job queue; this is called every time within the work loop
     */
    void flush_overflow_to_jobs();

    std::coroutine_handle<> acquire_job();

public:

    Worker(size_t num_jobs_max);

    Worker(const Worker&) = delete;

    Worker(Worker&&);

    Worker& operator=(const Worker&) = delete;

    Worker& operator=(Worker&&);

    // TODO:
    void assign_job(std::coroutine_handle<> job);

    void work();

}; /* class Worker */

inline static std::vector<Worker> workers;

inline static thread_local Worker* worker_this_thread$ = nullptr;

static Worker& get_random_worker();

public:

/**
 * @brief user facing coroutine type that can be used to create jobs; the coroutine must return a value of type T
 */
template <typename T>
class Job {
    friend class Awaiter;
public:

    struct promise_type : public Promise_Base {

        // the value returned by the coroutine
        T* value;

        promise_type() :
            Promise_Base(),
            value(nullptr)
        {}

        Job get_return_object() {
            value = new T();

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
            std::unique_lock lock(mut);
            *value = std::forward<U>(v);
        }

    }; /* struct promise_type */

    friend struct promise_type;

private:

    using Handle = std::coroutine_handle<promise_type>;

    Handle handle;

    T* result;

    Job(Handle h) : 
        handle(h),
        result(handle.promise().value)
    {
        // std::unique_lock lock(Promise_Base::get(handle)->mut);
        worker_this_thread$->assign_job(handle);
    }

public:

    Job(const Job&) = delete;

    Job(Job&& other) :
        handle(other.handle),
        result(other.result)
    {
        other.result = nullptr;
        other.handle = nullptr;
    }

    ~Job() {
        std::unique_lock lock(Promise_Base::get(handle)->mut);

        if (handle) {
            handle.destroy();
            handle = nullptr;
        }

        delete result;
    }

    Job& operator=(const Job&) = delete;

    Job& operator=(Job&& other) {
        std::shared_lock lock(Promise_Base::get(handle)->mut);

        if (this != &other) {
            handle = other.handle;
            other.handle = nullptr;
            result = other.result;
            other.handle = nullptr;
        }
        return *this;
    }

    T get_result() {
        assert(handle);

        std::shared_lock lock(Promise_Base::get(handle)->mut);

        assert(handle.done());

        return *result;
    }

    bool await_ready() const noexcept {
        std::shared_lock lock(Promise_Base::get(handle)->mut);

        if(handle) 
            return handle.done();
        else
            return false;
    }

    void await_suspend(std::coroutine_handle<> handle_parent) noexcept {
        // add this job to the parent job's (the coroutine we suspended on) waiting list

        // TODO: consider deadlock?
        auto promise_base_parent$ = Promise_Base::get(handle_parent);
        auto promise_base_child$ = Promise_Base::get(handle);

        std::unique_lock lock(promise_base_parent$->mut);
        std::unique_lock lock_child(promise_base_child$->mut);
        
        ++promise_base_parent$->num_waiting_on;
        promise_base_child$->parent = promise_base_parent$;

        // worker_this_thread$->assign_job(handle_parent);
    }

    T await_resume() {
        // notify the parent that we are done

        auto promise_base$ = Promise_Base::get(handle);
        std::shared_lock lock(promise_base$->mut);

        // we don't have to lock the parent since it's using a std::atomic<size_t>
        --promise_base$->parent->num_waiting_on;

        // if the parent is now unblocked, then add it to the worker's job queue
        if(promise_base$->parent->is_unblocked()) {
            std::coroutine_handle<> handle_parent = std::coroutine_handle<>::from_address(promise_base$->parent);
            worker_this_thread$->assign_job(handle_parent);
        }

        return *result;
    }

}; /* class Job<T> */

template <typename T>
friend class Job;

/**
 * @brief Runs the scheduler with the specified number of hardware threads and maximum jobs per worker; initializes all required data structures above
 */
static int Run(size_t num_hw_threads, size_t num_jobs_max, Job<int> (*generator_job_god)());

}; /* class Scheduler */

}; /* namespace Ripple */

#endif /* SCHEDULER_H */