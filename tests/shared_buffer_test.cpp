#include "shared_buffer_test.hpp"

#include <iostream>

#include "query.hpp"
#include "sonic_switch.hpp"

using namespace std;

bool test_shared_buffer_no_congestion_full_throughput() {
    const unsigned int total_time = 4;

    SharedBufferPointInfo info;
    info.max_demand_per_t = 1;
    info.buffer_slot_cnt = 8;
    info.ingress_pool_size = 8;
    info.headroom_pool_size = 8;
    info.egress_lossless_pool_size = 8;
    info.boundary_egress_ctrl_default_false = true;
    info.timing_mode = SharedBufferPointInfo::TimingMode::DEQ_FIRST;

    SONiCSwitch* sw = new SONiCSwitch(1, 1, total_time, info);

    Workload wl(10, sw->in_queue_cnt(), total_time);
    wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::GE), new Time(1)),
                          time_range_t(0, total_time - 1),
                          total_time));
    sw->set_base_workload(wl);

    cid_t queue_id = sw->get_in_queues()[0]->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(total_time - 1, total_time - 1),
                queue_id,
                metric_t::CENQ,
                Op(Op::Type::GE),
                total_time);
    sw->set_query(query);

    solver_res_t result = sw->satisfy_query();
    cout << result << endl;

    return result == solver_res_t::SAT;
}

void SharedBufferTest::add_to_runner(TestRunner* runner) {
    runner->add_test_case("shared_buffer_no_congestion_full_throughput",
                          test_shared_buffer_no_congestion_full_throughput);
}
