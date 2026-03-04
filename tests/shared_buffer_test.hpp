#ifndef SHARED_BUFFER_TEST_HPP
#define SHARED_BUFFER_TEST_HPP

#include "test_class.hpp"

class SharedBufferTest : TestClass {
public:
  void add_to_runner(TestRunner *runner) override;
};

#endif
