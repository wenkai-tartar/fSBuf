#include "multi_shared_buffer_test.hpp"

#include <iostream>

#include "cbd_shared_buffer.hpp"
#include "multi_shared_buffer.hpp"
#include "query.hpp"

using namespace std;

static solver_res_t
run_cbd_deadlock_query(unsigned int total_time,
                       const SharedBufferEnvInfo& env_info,
                       const SharedBufferSwitchInfo& switch_info,
                       unsigned int query_t1,
                       unsigned int query_t2) {
    auto make_switch_configs = [&switch_info]() {
        vector<CBDSharedBufferSwitchConfig> switch_configs;
        switch_configs.reserve(3);

        CBDSharedBufferSwitchConfig s1_cfg;
        s1_cfg.switch_id = "S1";
        s1_cfg.switch_info = switch_info;
        s1_cfg.route_policy =
            make_unique<TableRoutePolicy>(vector<vector<unsigned int>>{{1, 1, 0}, {0, 0, 0}});
        switch_configs.push_back(std::move(s1_cfg));

        CBDSharedBufferSwitchConfig s2_cfg;
        s2_cfg.switch_id = "S2";
        s2_cfg.switch_info = switch_info;
        s2_cfg.route_policy =
            make_unique<TableRoutePolicy>(vector<vector<unsigned int>>{{1, 0, 1}, {0, 0, 0}});
        switch_configs.push_back(std::move(s2_cfg));

        CBDSharedBufferSwitchConfig s3_cfg;
        s3_cfg.switch_id = "S3";
        s3_cfg.switch_info = switch_info;
        s3_cfg.route_policy =
            make_unique<TableRoutePolicy>(vector<vector<unsigned int>>{{0, 1, 1}, {0, 0, 0}});
        switch_configs.push_back(std::move(s3_cfg));

        return switch_configs;
    };

    auto make_ring_edges = []() {
        return vector<CBDRingEdge>{
            {"S1", 0, 0, "S3", 0, 0},
            {"S3", 0, 0, "S2", 0, 0},
            {"S2", 0, 0, "S1", 0, 0},
        };
    };

    CBDSharedBufferCP cp(total_time, env_info, make_switch_configs(), make_ring_edges());

    Workload wl(12, cp.in_queue_cnt(), total_time);
    for (unsigned int q = 0; q < cp.in_queue_cnt(); q++) {
        wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q),
                                       Op(Op::Type::EQ),
                                       new Constant((int) total_time)),
                              time_range_t(total_time - 1, total_time - 1),
                              total_time));
    }
    wl.add_spec(TimedSpec(
        new Comp(new Indiv(metric_t::CENQ, 0), Op(Op::Type::GE), new Constant(1)),
        time_range_t(0, 0),
        total_time));
    wl.add_spec(TimedSpec(
        new Comp(new Indiv(metric_t::CENQ, 1), Op(Op::Type::GE), new Constant(1)),
        time_range_t(0, 0),
        total_time));
    wl.add_spec(TimedSpec(
        new Comp(new Indiv(metric_t::CENQ, 2), Op(Op::Type::GE), new Constant(1)),
        time_range_t(0, 0),
        total_time));
    wl.add_spec(TimedSpec(
        new Comp(new Indiv(metric_t::DST, 0), Op(Op::Type::EQ), new Constant(0)),
        time_range_t(0, 0),
        total_time));
    wl.add_spec(TimedSpec(
        new Comp(new Indiv(metric_t::DST, 1), Op(Op::Type::EQ), new Constant(2)),
        time_range_t(0, 0),
        total_time));
    wl.add_spec(TimedSpec(
        new Comp(new Indiv(metric_t::DST, 2), Op(Op::Type::EQ), new Constant(1)),
        time_range_t(0, 0),
        total_time));
    cp.set_base_workload(wl);

    qsum_t ring_node_qids;
    ring_node_qids.reserve(cp.ring_nodes().size());
    for (const CBDNodeRef& node : cp.ring_nodes()) {
        SharedBufferSwitchQM* qm = dynamic_cast<SharedBufferSwitchQM*>(cp.get_qm(node.switch_id));
        if (qm == nullptr) {
            throw runtime_error("CBD test failed to resolve switch QM");
        }
        ring_node_qids.push_back(qm->get_in_queue(node.ingress)->get_id());
    }

    Query query(query_quant_t::EXISTS,
                time_range_t(query_t1, query_t2),
                ring_node_qids,
                metric_t::CBD_DEADLOCK_NODE,
                Op(Op::Type::GE),
                ring_node_qids.size());
    cp.set_query(query);

    return cp.satisfy_query();
}

static bool test_cbd_shared_buffer_three_switch_deadlock() {
    const unsigned int total_time = 8;
    const unsigned int query_t1 = 0;
    const unsigned int query_t2 = total_time - 1;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 2; // q0 internal ring ingress, q1 boundary ingress
    switch_info.egress_queue_cnt = 2;  // e0 internal ring egress, e1 boundary egress
    switch_info.buffer_slot_cnt = 2;
    switch_info.ingress_pool_size = 2;
    switch_info.headroom_pool_size = 2;
    switch_info.egress_lossless_pool_size = 2;
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;
    switch_info.egress_ser_factor = {2, 1};

    solver_res_t res = run_cbd_deadlock_query(total_time, env_info, switch_info, query_t1, query_t2);
    cout << "[cbd_shared_buffer_three_switch_deadlock] result: " << res << endl;
    return res != solver_res_t::UNKNOWN;
}

static bool test_two_switch_chain_base_workload_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    SharedBufferSwitchInfo s1_info;
    s1_info.ingress_queue_cnt = 1; // q0 boundary ingress
    s1_info.egress_queue_cnt = 1;  // e0 internal egress
    s1_info.buffer_slot_cnt = 4;
    s1_info.ingress_pool_size = 4;
    s1_info.headroom_pool_size = 4;
    s1_info.egress_lossless_pool_size = 4;
    s1_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;

    SharedBufferSwitchInfo s2_info;
    s2_info.ingress_queue_cnt = 1; // q0 internal ingress
    s2_info.egress_queue_cnt = 1;  // e0 boundary egress
    s2_info.buffer_slot_cnt = 4;
    s2_info.ingress_pool_size = 4;
    s2_info.headroom_pool_size = 4;
    s2_info.egress_lossless_pool_size = 4;
    s2_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;

    vector<MultiSharedBufferSwitchConfig> switch_configs;
    switch_configs.reserve(2);

    MultiSharedBufferSwitchConfig s1_cfg;
    s1_cfg.switch_id = "S1";
    s1_cfg.switch_info = s1_info;
    s1_cfg.route_policy = make_unique<TableRoutePolicy>(vector<vector<unsigned int>>{{0}});
    switch_configs.push_back(std::move(s1_cfg));

    MultiSharedBufferSwitchConfig s2_cfg;
    s2_cfg.switch_id = "S2";
    s2_cfg.switch_info = s2_info;
    s2_cfg.route_policy = make_unique<TableRoutePolicy>(vector<vector<unsigned int>>{{0}});
    switch_configs.push_back(std::move(s2_cfg));

    vector<MultiSharedBufferEdge> internal_edges = {
        {"S1", 0, "S2", 0},
    };

    MultiSharedBufferCP cp(total_time, env_info, std::move(switch_configs), internal_edges);

    Workload wl(8, cp.in_queue_cnt(), total_time);
    wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0),
                                   Op(Op::Type::EQ),
                                   new Constant(1)),
                          time_range_t(total_time - 1, total_time - 1),
                          total_time));

    solver_res_t res = cp.check_workload_without_query(wl);
    cout << "[two_switch_chain_base_workload_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

void MultiSharedBufferTest::add_to_runner(TestRunner* runner) {
    runner->add_test_case("two_switch_chain_base_workload_sat",
                          test_two_switch_chain_base_workload_sat);
    runner->add_test_case("cbd_shared_buffer_three_switch_deadlock",
                          test_cbd_shared_buffer_three_switch_deadlock);
}
