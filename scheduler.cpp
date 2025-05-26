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
    while (!flag_term) {
        // TODO:(1)
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

        auto promise_base$ = Promise_Base::get(job);

        // process the job
        std::shared_lock lock(promise_base$->mut);

        if(promise_base$->is_unblocked()) {
            // if the job is unblocked, then resume it
            job.resume();
        } else {
            // TODO: (2)
            // if the job is blocked, then we move on to the next job
            queue_jobs.insert_tail_if_possible(std::move(job));
        }
    }
}

// end Worker

Scheduler::Worker& Scheduler::get_random_worker() {
    static thread_local std::mt19937 gen(std::random_device{}());
    static thread_local std::uniform_int_distribution<size_t> dist(0, workers.size() - 1);

    size_t idx = dist(gen);

    return workers[idx];
}

void Scheduler::Run(size_t num_hw_threads, size_t num_jobs_max) {

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