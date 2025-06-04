#include "../scheduler.h"

#include <iostream>

Ripple::Scheduler::Job<int> job_mult(int a, int b) {
    co_return a * b;
}

Ripple::Scheduler::Job<int> job_god() {
    auto job1 = job_mult(2, 3);
    auto job2 = job_mult(4, 5);
    auto job3 = job_mult(6, 7);
    auto job4 = job_mult(8, 9);
    auto job5 = job_mult(10, 11);

    int result1 = co_await job1;
    int result2 = co_await job2;
    int result3 = co_await job3;
    int result4 = co_await job4;
    int result5 = co_await job5;

    std::cout << "Results: " << result1 << ", " << result2 << ", " << result3 << ", " << result4 << ", " << result5 << std::endl;

    co_return 0;
}

int main() {
    return Ripple::Scheduler::Run(2, 100, &job_god);
}