#include "multi_shared_buffer_test.hpp"
#include "pfc_deadlock_test.hpp"
#include "rr_scheduler_test.hpp"
#include "search_test.hpp"
#include "shared_buffer_test.hpp"
#include "tbf_test.hpp"
#include "test_runner.hpp"
#include <iostream>

using namespace std;

int main(int argc, char** argv) {

    vector<string> arguments(argv + 1, argv + argc);

    TestRunner runner;

    // RRSchedulerTest rrSchedulerTest;
    // rrSchedulerTest.add_to_runner(&runner);

    // TBFTest tbfTest;
    // tbfTest.add_to_runner(&runner);


    // SearchTest searchTest;
    // searchTest.add_to_runner(&runner);

    // SharedBufferTest sharedBufferTest;
    // sharedBufferTest.add_to_runner(&runner);

    // MultiSharedBufferTest multiSharedBufferTest;
    // multiSharedBufferTest.add_to_runner(&runner);

    PFCDeadlockTest pfcDeadlockTest;
    pfcDeadlockTest.add_to_runner(&runner);

    bool result = runner.run(arguments);

    cout << runner.get_report();

    return !result;
}
