#include "scheduler.h"

using namespace Ripple;

// start Scheduler
// start Promise_Base

Scheduler::Promise_Base::Promise_Base() :
    num_waiting_on(0),
    parent(nullptr)
{}

// end Promise_Base
// start Worker

Scheduler::Worker::Worker(size_t num_jobs_max) :
    queue_jobs(num_jobs_max),
    flag_term(false)
{}

void Scheduler::Worker::work() {
    // TODO: account for stickies (1) (2)
    while(!flag_term) {
        // TODO:(1)
        auto job$ = queue_jobs.pop_and_get_front_if_possible();
            
        // if no jobs, then try to first obtain a new set of jobs from the overflow buffer, then, upon failure, try to steal a job from another worker at random            
        while(!job$) {

            // try to get jobs from the overflow queue
            auto cbuf_overflow = queue_overflow.get_full_or_last();

            // if there are available overflow buffers, then we incorporate one as our job queue
            if(!cbuf_overflow.empty()) {
                queue_jobs = std::move(cbuf_overflow);
                job$ = queue_jobs.pop_and_get_front_if_possible();
            }

            // if no overflow jobs, then try to steal a job from another worker
            else {
                auto& worker = get_random_worker();
                job$ = worker.queue_jobs.steal_if_possible();

                // TODO: fix this busy wait w/ cvs
                if(!job$)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

        } /* while(!job$) */

        auto& job = job$.value();

        auto promise_base$ = Promise_Base::get(job);

        // process the job
        if(promise_base$->is_unblocked()) {
            // if the job is unblocked, then resume it
            job.resume();
        } else {
            // TODO: (2)
            // if the job is blocked, then we reassign it and move on to the next job
            assign_job(job);
        }
    } /* while(!flag_term) */
}

void Scheduler::Worker::assign_job(std::coroutine_handle<> job) {
    // assign the job to this worker's queue
    if(!queue_jobs.insert_tail_if_possible(job)) {
        // if the queue is full, then assign the job to the overflow queue
        assign_overflow(job);
    }
}

// end Worker
// start Queue_Overflow

Scheduler::Queue_Overflow::Queue_Overflow(size_t num_jobs_max) :
    queues_overflow(1, Circular_Buffer<std::coroutine_handle<>>(num_jobs_max)),
    num_jobs_max(num_jobs_max)
{}

void Scheduler::Queue_Overflow::assign_job(std::coroutine_handle<> job) {
    std::lock_guard<std::mutex> lock(mut);

    if(queues_overflow.empty() || !queues_overflow.back().insert_tail_if_possible(job)) {
        queues_overflow.emplace_back(num_jobs_max);
        queues_overflow.back().insert_tail_if_possible(job);
    }
}

Circular_Buffer<std::coroutine_handle<>> Scheduler::Queue_Overflow::get_full_or_last() {
    if(queues_overflow.empty())
        return Circular_Buffer<std::coroutine_handle<>>(0);
    else {
        auto cbuf_last(std::move(queues_overflow.back()));
        queues_overflow.pop_back();
        return cbuf_last;
    }
}

// end Queue_Overflow

Scheduler::Worker& Scheduler::get_random_worker() {
    static thread_local std::mt19937 gen(std::random_device{}());
    static thread_local std::uniform_int_distribution<size_t> dist(0, workers.size() - 1);

    size_t idx = dist(gen);

    return workers[idx];
}

void Scheduler::Run(size_t num_hw_threads, size_t num_jobs_max) {

    Queue_Overflow queue_overflow(num_jobs_max);

    workers.reserve(num_hw_threads);

    for(size_t i = 0; i < num_hw_threads; ++i) {
        workers.emplace_back(num_jobs_max);
    }

    worker_this_thread$ = &workers[0];

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