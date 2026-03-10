#include "shared_buffer_test.hpp"

#include <iostream>

#include "query.hpp"
#include "sonic_switch.hpp"

using namespace std;

static solver_res_t
run_single_ingress_cenq_query(unsigned int total_time,
                              const SharedBufferEnvInfo& env_info,
                              const SharedBufferSwitchInfo& switch_info,
                              int demand_lb,
                              int cenq_lb,
                              unique_ptr<SharedBufferRoutePolicy> route_policy = nullptr,
                              int dst_eq = -1) {
    SONiCSwitch sw(total_time, env_info, switch_info, std::move(route_policy));

    Workload wl(10, sw.in_queue_cnt(), total_time);
    wl.add_spec(TimedSpec(
        new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::GE), new Constant(demand_lb)),
        time_range_t(total_time - 1, total_time - 1),
        total_time));
    if (dst_eq >= 0) {
        wl.add_spec(
            TimedSpec(new Comp(new Indiv(metric_t::DST, 0), Op(Op::Type::EQ), new Constant(dst_eq)),
                      time_range_t(0, total_time - 1),
                      total_time));
    }
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

static solver_res_t
run_single_egress_pfc_blocked_query(unsigned int total_time,
                                    const SharedBufferEnvInfo& env_info,
                                    const SharedBufferSwitchInfo& switch_info,
                                    int demand_lb,
                                    unsigned int query_t1,
                                    unsigned int query_t2,
                                    int blocked_lb,
                                    unique_ptr<SharedBufferRoutePolicy> route_policy = nullptr,
                                    int dst_eq = -1) {
    SONiCSwitch sw(total_time, env_info, switch_info, std::move(route_policy));

    Workload wl(10, sw.in_queue_cnt(), total_time);
    wl.add_spec(TimedSpec(
        new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::GE), new Constant(demand_lb)),
        time_range_t(total_time - 1, total_time - 1),
        total_time));
    if (dst_eq >= 0) {
        wl.add_spec(
            TimedSpec(new Comp(new Indiv(metric_t::DST, 0), Op(Op::Type::EQ), new Constant(dst_eq)),
                      time_range_t(0, total_time - 1),
                      total_time));
    }
    sw.set_base_workload(wl);

    cid_t queue_id = sw.get_out_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(query_t1, query_t2),
                queue_id,
                metric_t::PFC_BLOCKED,
                Op(Op::Type::GE),
                blocked_lb);
    sw.set_query(query);

    return sw.satisfy_query();
}

static solver_res_t
run_single_ingress_cenq_query_at_time(unsigned int total_time,
                                      const SharedBufferEnvInfo& env_info,
                                      const SharedBufferSwitchInfo& switch_info,
                                      int demand_lb,
                                      unsigned int query_time,
                                      int cenq_lb,
                                      unique_ptr<SharedBufferRoutePolicy> route_policy = nullptr,
                                      int dst_eq = -1) {
    SONiCSwitch sw(total_time, env_info, switch_info, std::move(route_policy));

    Workload wl(10, sw.in_queue_cnt(), total_time);
    wl.add_spec(TimedSpec(
        new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::GE), new Constant(demand_lb)),
        time_range_t(total_time - 1, total_time - 1),
        total_time));
    if (dst_eq >= 0) {
        wl.add_spec(
            TimedSpec(new Comp(new Indiv(metric_t::DST, 0), Op(Op::Type::EQ), new Constant(dst_eq)),
                      time_range_t(0, total_time - 1),
                      total_time));
    }
    sw.set_base_workload(wl);

    cid_t queue_id = sw.get_in_queues()[0]->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(query_time, query_time),
                queue_id,
                metric_t::CENQ,
                Op(Op::Type::GE),
                cenq_lb);
    sw.set_query(query);

    return sw.satisfy_query();
}

static solver_res_t
run_output_cenq_query(unsigned int total_time,
                      const SharedBufferEnvInfo& env_info,
                      const SharedBufferSwitchInfo& switch_info,
                      const vector<int>& demand_lb,
                      const vector<int>& dst_eq,
                      unsigned int query_egress,
                      int cenq_lb,
                      unique_ptr<SharedBufferRoutePolicy> route_policy = nullptr) {
    SONiCSwitch sw(total_time, env_info, switch_info, std::move(route_policy));

    Workload wl(20, sw.in_queue_cnt(), total_time);
    for (unsigned int q = 0; q < sw.in_queue_cnt(); q++) {
        if (q < demand_lb.size() && demand_lb[q] >= 0) {
            wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q),
                                           Op(Op::Type::GE),
                                           new Constant(demand_lb[q])),
                                  time_range_t(total_time - 1, total_time - 1),
                                  total_time));
        }

        if (q < dst_eq.size() && dst_eq[q] >= 0) {
            wl.add_spec(TimedSpec(
                new Comp(new Indiv(metric_t::DST, q), Op(Op::Type::EQ), new Constant(dst_eq[q])),
                time_range_t(0, total_time - 1),
                total_time));
        }
    }
    sw.set_base_workload(wl);

    cid_t queue_id = sw.get_out_queue(query_egress)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(total_time - 1, total_time - 1),
                queue_id,
                metric_t::CENQ,
                Op(Op::Type::GE),
                cenq_lb);
    sw.set_query(query);

    return sw.satisfy_query();
}

static solver_res_t
run_ingress_cenq_diff_query(unsigned int total_time,
                            const SharedBufferEnvInfo& env_info,
                            const SharedBufferSwitchInfo& switch_info,
                            const vector<int>& demand_lb,
                            const vector<int>& dst_eq,
                            unsigned int lhs_ingress,
                            unsigned int rhs_ingress,
                            unsigned int query_t1,
                            unsigned int query_t2,
                            Op op,
                            unsigned int thresh,
                            unique_ptr<SharedBufferRoutePolicy> route_policy = nullptr) {
    SONiCSwitch sw(total_time, env_info, switch_info, std::move(route_policy));

    Workload wl(20, sw.in_queue_cnt(), total_time);
    for (unsigned int q = 0; q < sw.in_queue_cnt(); q++) {
        if (q < demand_lb.size() && demand_lb[q] >= 0) {
            wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q),
                                           Op(Op::Type::GE),
                                           new Constant(demand_lb[q])),
                                  time_range_t(total_time - 1, total_time - 1),
                                  total_time));
        }

        if (q < dst_eq.size() && dst_eq[q] >= 0) {
            wl.add_spec(TimedSpec(
                new Comp(new Indiv(metric_t::DST, q), Op(Op::Type::EQ), new Constant(dst_eq[q])),
                time_range_t(0, total_time - 1),
                total_time));
        }
    }
    sw.set_base_workload(wl);

    cid_t lhs_qid = sw.get_in_queues()[lhs_ingress]->get_id();
    cid_t rhs_qid = sw.get_in_queues()[rhs_ingress]->get_id();
    Query query(query_quant_t::FORALL,
                time_range_t(query_t1, query_t2),
                qdiff_t(lhs_qid, rhs_qid),
                metric_t::CENQ,
                op,
                thresh);
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
    solver_res_t unsat_res = run_single_ingress_cenq_query(total_time, env_info, switch_info, 5, 4);

    cout << "sat-bound: " << sat_res << endl;
    cout << "unsat-bound: " << unsat_res << endl;

    return sat_res == solver_res_t::SAT && unsat_res == solver_res_t::UNSAT;
}

bool test_shared_buffer_pause_resume_recovery() {
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

    cout << "[shared_buffer_pause_resume_recovery] early case: demand(T)>=5, cenq(t=3)>=3" << endl;
    solver_res_t early_res = run_single_ingress_cenq_query_at_time(
        total_time, env_info, switch_info, 5, 3, 3);

    cout << "[shared_buffer_pause_resume_recovery] late case: demand(T)>=5, cenq(t=4)>=3" << endl;
    solver_res_t late_res = run_single_ingress_cenq_query_at_time(
        total_time, env_info, switch_info, 5, 4, 3);

    cout << "early-bound: " << early_res << endl;
    cout << "late-bound: " << late_res << endl;

    return early_res == solver_res_t::UNSAT && late_res == solver_res_t::SAT;
}

bool test_shared_buffer_modulo_meta1_route_policy() {
    const unsigned int total_time = 5;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 1;
    switch_info.egress_queue_cnt = 2;
    switch_info.buffer_slot_cnt = 8;
    switch_info.ingress_pool_size = 8;
    switch_info.headroom_pool_size = 8;
    switch_info.egress_lossless_pool_size = 8;
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;

    vector<int> demand_lb = {5};

    cout << "[shared_buffer_modulo_meta1_route_policy] dst=3 routes to e1, out1.cenq(T)>=4" << endl;
    solver_res_t fast_res = run_output_cenq_query(total_time,
                                                  env_info,
                                                  switch_info,
                                                  demand_lb,
                                                  vector<int>{3},
                                                  1,
                                                  4,
                                                  make_unique<ModuloMeta1RoutePolicy>());
    cout << "[shared_buffer_modulo_meta1_route_policy] dst=3 routes away from e0, out0.cenq(T)>=1"
         << endl;
    solver_res_t miss_res = run_output_cenq_query(total_time,
                                                  env_info,
                                                  switch_info,
                                                  demand_lb,
                                                  vector<int>{3},
                                                  0,
                                                  1,
                                                  make_unique<ModuloMeta1RoutePolicy>());

    cout << "fast-egress-bound: " << fast_res << endl;
    cout << "wrong-egress-bound: " << miss_res << endl;

    return fast_res == solver_res_t::SAT && miss_res == solver_res_t::UNSAT;
}

bool test_shared_buffer_fixed_ingress_route_policy() {
    const unsigned int total_time = 5;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 1;
    switch_info.egress_queue_cnt = 2;
    switch_info.buffer_slot_cnt = 8;
    switch_info.ingress_pool_size = 8;
    switch_info.headroom_pool_size = 8;
    switch_info.egress_lossless_pool_size = 8;
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;

    vector<int> demand_lb = {5};
    vector<int> dst_eq = {-1};

    cout << "[shared_buffer_fixed_ingress_route_policy] ingress 0 fixed to e1, out1.cenq(T)>=4"
         << endl;
    solver_res_t fast_res = run_output_cenq_query(total_time,
                                                  env_info,
                                                  switch_info,
                                                  demand_lb,
                                                  dst_eq,
                                                  1,
                                                  4,
                                                  make_unique<FixedIngressRoutePolicy>(
                                                      vector<unsigned int>{1}));

    cout << "[shared_buffer_fixed_ingress_route_policy] ingress 0 fixed away from e0, "
            "out0.cenq(T)>=1"
         << endl;
    solver_res_t miss_res = run_output_cenq_query(total_time,
                                                  env_info,
                                                  switch_info,
                                                  demand_lb,
                                                  dst_eq,
                                                  0,
                                                  1,
                                                  make_unique<FixedIngressRoutePolicy>(
                                                      vector<unsigned int>{1}));

    cout << "fast-egress-bound: " << fast_res << endl;
    cout << "wrong-egress-bound: " << miss_res << endl;

    return fast_res == solver_res_t::SAT && miss_res == solver_res_t::UNSAT;
}

bool test_shared_buffer_table_route_policy() {
    const unsigned int total_time = 5;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 1;
    switch_info.egress_queue_cnt = 2;
    switch_info.buffer_slot_cnt = 8;
    switch_info.ingress_pool_size = 8;
    switch_info.headroom_pool_size = 8;
    switch_info.egress_lossless_pool_size = 8;
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;

    vector<int> demand_lb = {5};

    cout << "[shared_buffer_table_route_policy] table[0][0]=1, out1.cenq(T)>=4" << endl;
    solver_res_t fast_res = run_output_cenq_query(total_time,
                                                  env_info,
                                                  switch_info,
                                                  demand_lb,
                                                  vector<int>{0},
                                                  1,
                                                  4,
                                                  make_unique<TableRoutePolicy>(
                                                      vector<vector<unsigned int>>{{1, 0}}));
    cout << "[shared_buffer_table_route_policy] table[0][1]=0, out1.cenq(T)>=1" << endl;
    solver_res_t miss_res = run_output_cenq_query(total_time,
                                                  env_info,
                                                  switch_info,
                                                  demand_lb,
                                                  vector<int>{1},
                                                  1,
                                                  1,
                                                  make_unique<TableRoutePolicy>(
                                                      vector<vector<unsigned int>>{{1, 0}}));

    cout << "fast-egress-bound: " << fast_res << endl;
    cout << "wrong-egress-bound: " << miss_res << endl;

    return fast_res == solver_res_t::SAT && miss_res == solver_res_t::UNSAT;
}

bool test_shared_buffer_ingress_service_gap_control() {
    const unsigned int total_time = 5;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 2;
    switch_info.egress_queue_cnt = 2;
    switch_info.buffer_slot_cnt = 8;
    switch_info.ingress_pool_size = 8;
    switch_info.headroom_pool_size = 8;
    switch_info.egress_lossless_pool_size = 8;
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;

    vector<int> demand_lb = {5, 5};
    vector<int> dst_eq = {-1, -1};
    const unsigned int t1 = 1;
    const unsigned int t2 = 4;

    cout << "[shared_buffer_ingress_service_gap_control] q0-q1 cenq gap stays <= 0 on [1,4]"
         << endl;
    solver_res_t q0_le_q1 = run_ingress_cenq_diff_query(total_time,
                                                        env_info,
                                                        switch_info,
                                                        demand_lb,
                                                        dst_eq,
                                                        0,
                                                        1,
                                                        t1,
                                                        t2,
                                                        Op(Op::Type::LE),
                                                        0,
                                                        make_unique<FixedIngressRoutePolicy>(
                                                            vector<unsigned int>{0, 1}));

    cout << "[shared_buffer_ingress_service_gap_control] q1-q0 cenq gap stays <= 0 on [1,4]"
         << endl;
    solver_res_t q1_le_q0 = run_ingress_cenq_diff_query(total_time,
                                                        env_info,
                                                        switch_info,
                                                        demand_lb,
                                                        dst_eq,
                                                        1,
                                                        0,
                                                        t1,
                                                        t2,
                                                        Op(Op::Type::LE),
                                                        0,
                                                        make_unique<FixedIngressRoutePolicy>(
                                                            vector<unsigned int>{0, 1}));

    cout << "q0_minus_q1_le_0: " << q0_le_q1 << endl;
    cout << "q1_minus_q0_le_0: " << q1_le_q0 << endl;

    return q0_le_q1 == solver_res_t::SAT && q1_le_q0 == solver_res_t::SAT;
}

bool test_shared_buffer_pfc_blocked_quiescent_boundary() {
    const unsigned int total_time = 7;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 1;
    switch_info.egress_queue_cnt = 1;
    switch_info.buffer_slot_cnt = 2;
    switch_info.ingress_pool_size = 2;
    switch_info.headroom_pool_size = 2;
    switch_info.egress_lossless_pool_size = 2;
    switch_info.egress_ser_factor = {2};
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;

    cout << "[shared_buffer_pfc_blocked_quiescent_boundary] EXISTS [2,6]: egress pfc_blocked >= 1"
         << endl;
    solver_res_t res =
        run_single_egress_pfc_blocked_query(total_time, env_info, switch_info, 7, 2, 6, 1);

    cout << "blocked_witness: " << res << endl;

    return res == solver_res_t::UNSAT;
}

bool test_shared_buffer_pfc_blocked_external_pause_witness() {
    const unsigned int total_time = 7;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = false;

    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 1;
    switch_info.egress_queue_cnt = 1;
    switch_info.buffer_slot_cnt = 2;
    switch_info.ingress_pool_size = 2;
    switch_info.headroom_pool_size = 2;
    switch_info.egress_lossless_pool_size = 2;
    switch_info.egress_ser_factor = {2};
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;

    cout << "[shared_buffer_pfc_blocked_external_pause_witness] EXISTS [2,6]: egress pfc_blocked >= 1"
         << endl;
    solver_res_t res =
        run_single_egress_pfc_blocked_query(total_time, env_info, switch_info, 7, 2, 6, 1);

    cout << "blocked_witness: " << res << endl;

    return res == solver_res_t::SAT;
}

void SharedBufferTest::add_to_runner(TestRunner* runner) {
    runner->add_test_case("shared_buffer_no_congestion_full_throughput",
                          test_shared_buffer_no_congestion_full_throughput);
    runner->add_test_case("shared_buffer_ingress_rate_limit",
                          test_shared_buffer_ingress_rate_limit);
    runner->add_test_case("shared_buffer_egress_backpressure",
                          test_shared_buffer_egress_backpressure);
    runner->add_test_case("shared_buffer_pause_resume_recovery",
                          test_shared_buffer_pause_resume_recovery);
    runner->add_test_case("shared_buffer_modulo_meta1_route_policy",
                          test_shared_buffer_modulo_meta1_route_policy);
    runner->add_test_case("shared_buffer_fixed_ingress_route_policy",
                          test_shared_buffer_fixed_ingress_route_policy);
    runner->add_test_case("shared_buffer_table_route_policy",
                          test_shared_buffer_table_route_policy);
    runner->add_test_case("shared_buffer_ingress_service_gap_control",
                          test_shared_buffer_ingress_service_gap_control);
    runner->add_test_case("shared_buffer_pfc_blocked_quiescent_boundary",
                          test_shared_buffer_pfc_blocked_quiescent_boundary);
    runner->add_test_case("shared_buffer_pfc_blocked_external_pause_witness",
                          test_shared_buffer_pfc_blocked_external_pause_witness);
}
