#include "shared_buffer_test.hpp"

#include <iostream>

#include "query.hpp"
#include "sonic_switch.hpp"

using namespace std;

static solver_res_t run_single_ingress_cenq_query(unsigned int total_time,
                                                  const SharedBufferEnvInfo& env_info,
                                                  const SharedBufferSwitchInfo& switch_info,
                                                  int demand_lb,
                                                  int cenq_lb) {
    SONiCSwitch sw(total_time, env_info, switch_info);

    Workload wl(10, sw.in_queue_cnt(), total_time);
    wl.add_spec(TimedSpec(
        new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::GE), new Constant(demand_lb)),
        time_range_t(total_time - 1, total_time - 1),
        total_time));
    sw.set_base_workload(wl);

    cid_t queue_id = sw.get_in_queues()[0]->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(total_time - 1, total_time - 1),
                queue_id,
                metric_t::CENQ,
                Op(Op::Type::GE),
                cenq_lb);
    sw.set_query(query);

    return sw.satisfy_query();
}

bool test_shared_buffer_no_congestion_full_throughput() {
    const unsigned int total_time = 4;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 1;
    switch_info.egress_queue_cnt = 1;
    switch_info.buffer_slot_cnt = 8;
    switch_info.ingress_pool_size = 8;
    switch_info.headroom_pool_size = 8;
    switch_info.egress_lossless_pool_size = 8;
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;

    SONiCSwitch* sw = new SONiCSwitch(total_time, env_info, switch_info);

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

bool test_shared_buffer_ingress_rate_limit() {
    const unsigned int total_time = 5;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 1;
    switch_info.egress_queue_cnt = 1;
    switch_info.buffer_slot_cnt = 8;
    switch_info.ingress_pool_size = 8;
    switch_info.headroom_pool_size = 8;
    switch_info.egress_lossless_pool_size = 8;
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;
    switch_info.ingress_ser_factor = {2};

    cout << "[shared_buffer_ingress_rate_limit] SAT case: demand(T)>=4, cenq(T)>=3" << endl;
    solver_res_t sat_res = run_single_ingress_cenq_query(total_time, env_info, switch_info, 4, 3);
    cout << "[shared_buffer_ingress_rate_limit] UNSAT case: demand(T)>=4, cenq(T)>=4" << endl;
    solver_res_t unsat_res = run_single_ingress_cenq_query(total_time, env_info, switch_info, 4, 4);

    cout << "sat-bound: " << sat_res << endl;
    cout << "unsat-bound: " << unsat_res << endl;

    return sat_res == solver_res_t::SAT && unsat_res == solver_res_t::UNSAT;
}

bool test_shared_buffer_egress_backpressure() {
    const unsigned int total_time = 5;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 1;
    switch_info.egress_queue_cnt = 1;
    switch_info.buffer_slot_cnt = 1;
    switch_info.ingress_pool_size = 1;
    switch_info.headroom_pool_size = 1;
    switch_info.egress_lossless_pool_size = 1;
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;
    switch_info.egress_ser_factor = {2};

    cout << "[shared_buffer_egress_backpressure] SAT case: demand(T)>=5, cenq(T)>=3" << endl;
    solver_res_t sat_res = run_single_ingress_cenq_query(total_time, env_info, switch_info, 5, 3);
    cout << "[shared_buffer_egress_backpressure] UNSAT case: demand(T)>=5, cenq(T)>=4" << endl;
    solver_res_t unsat_res =
        run_single_ingress_cenq_query(total_time, env_info, switch_info, 5, 4);

    cout << "sat-bound: " << sat_res << endl;
    cout << "unsat-bound: " << unsat_res << endl;

    return sat_res == solver_res_t::SAT && unsat_res == solver_res_t::UNSAT;
}

void SharedBufferTest::add_to_runner(TestRunner* runner) {
    runner->add_test_case("shared_buffer_no_congestion_full_throughput",
                          test_shared_buffer_no_congestion_full_throughput);
    runner->add_test_case("shared_buffer_ingress_rate_limit",
                          test_shared_buffer_ingress_rate_limit);
    runner->add_test_case("shared_buffer_egress_backpressure",
                          test_shared_buffer_egress_backpressure);
}
