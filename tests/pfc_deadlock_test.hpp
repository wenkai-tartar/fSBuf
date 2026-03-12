#ifndef PFC_DEADLOCK_TEST_HPP
#define PFC_DEADLOCK_TEST_HPP

#include "test_class.hpp"

class PFCDeadlockTest : TestClass {
public:
  void add_to_runner(TestRunner *runner) override;
};

#endif
