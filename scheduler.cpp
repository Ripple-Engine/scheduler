#include "scheduler.h"
#include "../lib/debug.h"

using namespace Ripple;

// start Scheduler
// start Promise_Base

Scheduler::Promise_Base::Promise_Base() :
    num_waiting_on(0),
    parent(nullptr)
{}

// end Promise_Base
// start Worker

void Scheduler::Worker::flush_overflow_to_jobs() {
    DPRINT("worker ", worker_this_thread$, " flushing overflow to jobs");

    while(!queue_overflow.empty()) {
        auto job = queue_overflow.front();
        queue_overflow.pop_front();

        // try to insert the job into the job queue
        if(!queue_jobs.insert_tail_if_possible(job)) {
            // exit the loop upon failure

            DPRINT("worker ", worker_this_thread$, " failed to flush job ", job.address(), " to jobs queue; queue is full");

            break; /* while(!queue_overflow.empty()) */
        }

        DPRINT("worker ", worker_this_thread$, " flushed job ", job.address(), " to jobs queue");

    } /* while(!queue_overflow.empty()) */
}

std::coroutine_handle<> Scheduler::Worker::acquire_job() {
    enum class MODE {
        POP,
        STEAL
    };

    MODE mode = MODE::POP;

    std::optional<std::coroutine_handle<>> job$;
    do {
        switch(mode) {
            case MODE::POP:
                DPRINT("worker ", worker_this_thread$, " trying to pop a job from jobs queue");

                // try to pop a job from our job queue
                job$ = queue_jobs.pop_and_get_front_if_possible();

                if(!job$) {
                    mode = MODE::STEAL;
                    DPRINT("worker ", worker_this_thread$, " no jobs in jobs queue, switching to steal mode");
                } 
                
                else {
                    DPRINT("worker ", worker_this_thread$, " popped job ", (job$ ? job$.value().address() : "null"), " from jobs queue");
                }

                break;

            default /*MODE::STEAL*/:
                DPRINT("worker ", worker_this_thread$, " trying to steal a job from another worker");

                // try to steal a job from another worker (not the current worker)
                Worker* worker$ = nullptr;
                do {
                    worker$ = &get_random_worker();
                } while(worker$ == worker_this_thread$);

                auto& worker = *worker$;

                DPRINT("worker ", worker_this_thread$, " stealing from worker ", &worker);

                job$ = worker.queue_jobs.steal_if_possible();

                // if we couldn't steal a job, then we wait for a bit before trying again
                // TODO: fix this to be more efficient
                if(!job$) {
                    DPRINT("worker ", worker_this_thread$, " failed to steal a job from worker ", &worker, "; waiting for a bit before trying again");

                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }

                else {
                    DPRINT("worker ", worker_this_thread$, " stole job ", job$.value().address(), " from worker ", &worker);
                }

                break;

        }; /* switch(mode) */

    } while(!job$);

    DPRINT("worker ", worker_this_thread$, " acquired job ", job$.value().address());

    return job$.value();
}

Scheduler::Worker::Worker(size_t num_jobs_max) :
    queue_jobs(num_jobs_max),
    flag_term(false)
{
    worker_this_thread$ = this;
}

Scheduler::Worker::Worker(Worker&& other) :
    queue_jobs(std::move(other.queue_jobs)),
    queue_overflow(std::move(other.queue_overflow)),
    flag_term(other.flag_term)
{
    worker_this_thread$ = this;
}

Scheduler::Worker& Scheduler::Worker::operator=(Worker&& other) {
    if(this != &other) {
        queue_jobs = std::move(other.queue_jobs);
        queue_overflow = std::move(other.queue_overflow);
        flag_term = other.flag_term;

        worker_this_thread$ = this;
    }
    return *this;
}

void Scheduler::Worker::assign_job(std::coroutine_handle<> job) {
    DPRINT("worker ", worker_this_thread$, " assigning job ", job.address(), " to jobs queue");

    // assign the job to this worker's queue
    if(!queue_jobs.insert_tail_if_possible(job)) {
        DPRINT("worker ", worker_this_thread$, " failed to assign job ", job.address(), " to full jobs queue, moving to overflow queue");

        // if the queue is full, then assign the job to the overflow queue
        queue_overflow.push_back(job);
    }

    else {
        DPRINT("worker ", worker_this_thread$, " assigned job ", job.address(), " to jobs queue");
    }
}

void Scheduler::Worker::work() {    
    worker_this_thread$ = this;

    // TODO: account for stickies (1) (2)
    while(!flag_term) {
        DPRINT("worker ", worker_this_thread$, " working...");

        // TODO:(1)

        flush_overflow_to_jobs();

        auto job = acquire_job();
        auto promise_base$ = Promise_Base::get(job);

        // process the job

        // if the job is unblocked, then resume it
        if(promise_base$->is_unblocked()) {
            DPRINT("worker ", worker_this_thread$, " resuming job ", job.address(), " (unblocked)");
            job.resume();
        } 
        
        // if the job is blocked, then we reassign it and move on to the next job
        else {
            // TODO: (2)
            DPRINT("worker ", worker_this_thread$, " job ", job.address(), " is blocked, reassigning it to the worker's job queue");
            assign_job(job);
        }

    } /* while(!flag_term) */
}

// end Worker

Scheduler::Worker& Scheduler::get_random_worker() {
    static thread_local std::mt19937 gen(std::random_device{}());
    static thread_local std::uniform_int_distribution<size_t> dist(0, workers.size() - 1);

    size_t idx = dist(gen);

    return workers[idx];
}

int Scheduler::Run(size_t num_hw_threads, size_t num_jobs_max, Job<int> (*generator_job_god)()) {

    workers.reserve(num_hw_threads);
    for(size_t i = 0; i < num_hw_threads; ++i)
        workers.emplace_back(num_jobs_max);

    // thread pool for the workers, except the firstdo one
    std::vector<std::thread> threads;
    threads.reserve(num_hw_threads - 1);

    // create the worker threads
    for(size_t i = 1; i < num_hw_threads; ++i)
        threads.emplace_back(std::bind(&Worker::work, &workers[i]));

    Job<int> job_god = generator_job_god();

    // run the first worker in the main thread
    workers[0].work();

    for(auto& thread : threads)
        thread.join();

    return job_god.get_result();

}