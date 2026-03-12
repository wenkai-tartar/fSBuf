#include "multi_shared_buffer_test.hpp"

#include <iostream>

#include "cbd_shared_buffer.hpp"
#include "multi_shared_buffer.hpp"
#include "query.hpp"

using namespace std;

struct TwoSwitchChainConfig {
    unsigned int s1_buffer_slots = 4;
    unsigned int s1_ingress_pool = 4;
    unsigned int s1_headroom_pool = 4;
    unsigned int s1_egress_pool = 4;
    unsigned int s1_egress_ser = 1;
    unsigned int s2_buffer_slots = 4;
    unsigned int s2_ingress_pool = 4;
    unsigned int s2_headroom_pool = 4;
    unsigned int s2_egress_pool = 4;
    unsigned int s2_egress_ser = 1;
};

static MultiSharedBufferCP make_two_switch_chain_cp(unsigned int total_time,
                                                    const SharedBufferEnvInfo& env_info,
                                                    const TwoSwitchChainConfig& cfg = {}) {
    SharedBufferSwitchInfo s1_info;
    s1_info.ingress_queue_cnt = 1; // q0 boundary ingress
    s1_info.egress_queue_cnt = 1;  // e0 internal egress
    s1_info.buffer_slot_cnt = cfg.s1_buffer_slots;
    s1_info.ingress_pool_size = cfg.s1_ingress_pool;
    s1_info.headroom_pool_size = cfg.s1_headroom_pool;
    s1_info.egress_lossless_pool_size = cfg.s1_egress_pool;
    s1_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;
    s1_info.egress_ser_factor = {cfg.s1_egress_ser};

    SharedBufferSwitchInfo s2_info;
    s2_info.ingress_queue_cnt = 1; // q0 internal ingress
    s2_info.egress_queue_cnt = 1;  // e0 boundary egress
    s2_info.buffer_slot_cnt = cfg.s2_buffer_slots;
    s2_info.ingress_pool_size = cfg.s2_ingress_pool;
    s2_info.headroom_pool_size = cfg.s2_headroom_pool;
    s2_info.egress_lossless_pool_size = cfg.s2_egress_pool;
    s2_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;
    s2_info.egress_ser_factor = {cfg.s2_egress_ser};

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

    return MultiSharedBufferCP(total_time, env_info, std::move(switch_configs), internal_edges);
}

static MultiSharedBufferCP make_two_switch_two_source_chain_cp(
    unsigned int total_time,
    const SharedBufferEnvInfo& env_info,
    const TwoSwitchChainConfig& cfg = {}) {
    SharedBufferSwitchInfo s1_info;
    s1_info.ingress_queue_cnt = 2; // q0,q1 boundary ingresses
    s1_info.egress_queue_cnt = 1;  // e0 internal egress
    s1_info.buffer_slot_cnt = cfg.s1_buffer_slots;
    s1_info.ingress_pool_size = cfg.s1_ingress_pool;
    s1_info.headroom_pool_size = cfg.s1_headroom_pool;
    s1_info.egress_lossless_pool_size = cfg.s1_egress_pool;
    s1_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;
    s1_info.egress_ser_factor = {cfg.s1_egress_ser};

    SharedBufferSwitchInfo s2_info;
    s2_info.ingress_queue_cnt = 1; // q0 internal ingress
    s2_info.egress_queue_cnt = 1;  // e0 boundary egress
    s2_info.buffer_slot_cnt = cfg.s2_buffer_slots;
    s2_info.ingress_pool_size = cfg.s2_ingress_pool;
    s2_info.headroom_pool_size = cfg.s2_headroom_pool;
    s2_info.egress_lossless_pool_size = cfg.s2_egress_pool;
    s2_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;
    s2_info.egress_ser_factor = {cfg.s2_egress_ser};

    vector<MultiSharedBufferSwitchConfig> switch_configs;
    switch_configs.reserve(2);

    MultiSharedBufferSwitchConfig s1_cfg;
    s1_cfg.switch_id = "S1";
    s1_cfg.switch_info = s1_info;
    s1_cfg.route_policy = make_unique<TableRoutePolicy>(vector<vector<unsigned int>>{{0}, {0}});
    switch_configs.push_back(std::move(s1_cfg));

    MultiSharedBufferSwitchConfig s2_cfg;
    s2_cfg.switch_id = "S2";
    s2_cfg.switch_info = s2_info;
    s2_cfg.route_policy = make_unique<TableRoutePolicy>(vector<vector<unsigned int>>{{0}});
    switch_configs.push_back(std::move(s2_cfg));

    vector<MultiSharedBufferEdge> internal_edges = {
        {"S1", 0, "S2", 0},
    };

    return MultiSharedBufferCP(total_time, env_info, std::move(switch_configs), internal_edges);
}

static CBDSharedBufferCP make_three_switch_cbd_cp(unsigned int total_time,
                                                  const SharedBufferEnvInfo& env_info,
                                                  const SharedBufferSwitchInfo& switch_info) {
    auto make_switch_configs = [&switch_info]() {
        vector<CBDSharedBufferSwitchConfig> switch_configs;
        switch_configs.reserve(3);

        CBDSharedBufferSwitchConfig s1_cfg;
        s1_cfg.switch_id = "S1";
        s1_cfg.switch_info = switch_info;
        s1_cfg.route_policy = make_unique<TableRoutePolicy>(
            vector<vector<unsigned int>>{{1, 1, 0}, {0, 0, 0}});
        switch_configs.push_back(std::move(s1_cfg));

        CBDSharedBufferSwitchConfig s2_cfg;
        s2_cfg.switch_id = "S2";
        s2_cfg.switch_info = switch_info;
        s2_cfg.route_policy = make_unique<TableRoutePolicy>(
            vector<vector<unsigned int>>{{1, 0, 1}, {0, 0, 0}});
        switch_configs.push_back(std::move(s2_cfg));

        CBDSharedBufferSwitchConfig s3_cfg;
        s3_cfg.switch_id = "S3";
        s3_cfg.switch_info = switch_info;
        s3_cfg.route_policy = make_unique<TableRoutePolicy>(
            vector<vector<unsigned int>>{{0, 1, 1}, {0, 0, 0}});
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

    return CBDSharedBufferCP(total_time, env_info, make_switch_configs(), make_ring_edges());
}

static Workload make_three_switch_cbd_workload(CBDSharedBufferCP& cp, unsigned int total_time) {
    Workload wl(12, cp.in_queue_cnt(), total_time);
    for (unsigned int q = 0; q < cp.in_queue_cnt(); q++) {
        wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q),
                                       Op(Op::Type::EQ),
                                       new Constant((int) total_time)),
                              time_range_t(total_time - 1, total_time - 1),
                              total_time));
    }
    wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::CENQ, 0), Op(Op::Type::GE), new Constant(1)),
                          time_range_t(0, 0),
                          total_time));
    wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::CENQ, 1), Op(Op::Type::GE), new Constant(1)),
                          time_range_t(0, 0),
                          total_time));
    wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::CENQ, 2), Op(Op::Type::GE), new Constant(1)),
                          time_range_t(0, 0),
                          total_time));
    wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DST, 0), Op(Op::Type::EQ), new Constant(0)),
                          time_range_t(0, 0),
                          total_time));
    wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DST, 1), Op(Op::Type::EQ), new Constant(2)),
                          time_range_t(0, 0),
                          total_time));
    wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DST, 2), Op(Op::Type::EQ), new Constant(1)),
                          time_range_t(0, 0),
                          total_time));
    for (unsigned int q = 0; q < cp.in_queue_cnt(); q++) {
        wl.add_spec(TimedSpec(new Same(metric_t::DST, q), time_range_t(0, total_time - 1), total_time));
    }
    return wl;
}

static Workload make_three_switch_cbd_t0_single_packet_workload(CBDSharedBufferCP& cp,
                                                                unsigned int total_time) {
    Workload wl(15, cp.in_queue_cnt(), total_time);
    for (unsigned int q = 0; q < cp.in_queue_cnt(); q++) {
        wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q), Op(Op::Type::EQ), new Constant(1)),
                              time_range_t(0, 0),
                              total_time));
        wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q), Op(Op::Type::EQ), new Constant(1)),
                              time_range_t(total_time - 1, total_time - 1),
                              total_time));
    }
    wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DST, 0), Op(Op::Type::EQ), new Constant(0)),
                          time_range_t(0, 0),
                          total_time));
    wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DST, 1), Op(Op::Type::EQ), new Constant(2)),
                          time_range_t(0, 0),
                          total_time));
    wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DST, 2), Op(Op::Type::EQ), new Constant(1)),
                          time_range_t(0, 0),
                          total_time));
    for (unsigned int q = 0; q < cp.in_queue_cnt(); q++) {
        wl.add_spec(TimedSpec(new Same(metric_t::DST, q), time_range_t(0, total_time - 1), total_time));
    }
    return wl;
}

static Workload make_three_switch_cbd_t0_single_packet_workload_without_source(
    CBDSharedBufferCP& cp,
    unsigned int total_time,
    unsigned int missing_src_q) {
    if (missing_src_q >= cp.in_queue_cnt()) {
        throw runtime_error("CBD workload helper missing_src_q out of range");
    }

    Workload wl(16, cp.in_queue_cnt(), total_time);
    const vector<int> fixed_dst = {0, 2, 1};

    for (unsigned int q = 0; q < cp.in_queue_cnt(); q++) {
        const int demand_cnt = (q == missing_src_q) ? 0 : 1;
        wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q),
                                       Op(Op::Type::EQ),
                                       new Constant(demand_cnt)),
                              time_range_t(0, 0),
                              total_time));
        wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q),
                                       Op(Op::Type::EQ),
                                       new Constant(demand_cnt)),
                              time_range_t(total_time - 1, total_time - 1),
                              total_time));
        if (q != missing_src_q) {
            wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DST, q),
                                           Op(Op::Type::EQ),
                                           new Constant(fixed_dst[q])),
                                  time_range_t(0, 0),
                                  total_time));
        }
        wl.add_spec(TimedSpec(new Same(metric_t::DST, q), time_range_t(0, total_time - 1), total_time));
    }

    return wl;
}

static Workload make_three_switch_cbd_t0_single_packet_workload_with_overridden_dst(
    CBDSharedBufferCP& cp,
    unsigned int total_time,
    unsigned int src_q,
    int new_dst) {
    if (src_q >= cp.in_queue_cnt()) {
        throw runtime_error("CBD workload helper src_q out of range");
    }

    Workload wl(17, cp.in_queue_cnt(), total_time);
    vector<int> fixed_dst = {0, 2, 1};
    fixed_dst[src_q] = new_dst;

    for (unsigned int q = 0; q < cp.in_queue_cnt(); q++) {
        wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q), Op(Op::Type::EQ), new Constant(1)),
                              time_range_t(0, 0),
                              total_time));
        wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q), Op(Op::Type::EQ), new Constant(1)),
                              time_range_t(total_time - 1, total_time - 1),
                              total_time));
        wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DST, q),
                                       Op(Op::Type::EQ),
                                       new Constant(fixed_dst[q])),
                              time_range_t(0, 0),
                              total_time));
        wl.add_spec(TimedSpec(new Same(metric_t::DST, q), time_range_t(0, total_time - 1), total_time));
    }

    return wl;
}

static solver_res_t run_cbd_deadlock_query(unsigned int total_time,
                                           const SharedBufferEnvInfo& env_info,
                                           const SharedBufferSwitchInfo& switch_info,
                                           const Workload& wl,
                                           query_quant_t quant,
                                           unsigned int query_t1,
                                           unsigned int query_t2) {
    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
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

    Query query(quant,
                time_range_t(query_t1, query_t2),
                ring_node_qids,
                metric_t::CBD_DEADLOCK_NODE,
                Op(Op::Type::GE),
                ring_node_qids.size());
    cp.set_query(query);

    return cp.satisfy_query();
}

static solver_res_t run_cbd_wait_edge_query(unsigned int total_time,
                                            const SharedBufferEnvInfo& env_info,
                                            const SharedBufferSwitchInfo& switch_info,
                                            const Workload& wl,
                                            unsigned int query_t1,
                                            unsigned int query_t2,
                                            unsigned int threshold) {
    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    cp.set_base_workload(wl);

    qsum_t ring_edge_qids;
    ring_edge_qids.reserve(cp.ring_edges().size());
    for (const CBDRingEdge& edge : cp.ring_edges()) {
        SharedBufferSwitchQM* qm = dynamic_cast<SharedBufferSwitchQM*>(cp.get_qm(edge.src_switch));
        if (qm == nullptr) {
            throw runtime_error("CBD test failed to resolve switch QM");
        }
        ring_edge_qids.push_back(qm->get_out_queue(edge.src_egress)->get_id());
    }

    Query query(query_quant_t::EXISTS,
                time_range_t(query_t1, query_t2),
                ring_edge_qids,
                metric_t::CBD_WAIT_EDGE,
                Op(Op::Type::GE),
                threshold);
    cp.set_query(query);

    return cp.satisfy_query();
}

static SharedBufferEnvInfo make_three_switch_cbd_env_info() {
    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;
    return env_info;
}

static SharedBufferSwitchInfo make_three_switch_cbd_switch_info(unsigned int buffer_slot_cnt = 2,
                                                                unsigned int ingress_pool_size = 2,
                                                                unsigned int headroom_pool_size = 2,
                                                                unsigned int egress_lossless_pool_size = 2,
                                                                unsigned int ring_egress_ser = 2,
                                                                unsigned int boundary_egress_ser = 1) {
    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 2; // q0 internal ring ingress, q1 boundary ingress
    switch_info.egress_queue_cnt = 2;  // e0 internal ring egress, e1 boundary egress
    switch_info.buffer_slot_cnt = buffer_slot_cnt;
    switch_info.ingress_pool_size = ingress_pool_size;
    switch_info.headroom_pool_size = headroom_pool_size;
    switch_info.egress_lossless_pool_size = egress_lossless_pool_size;
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;
    switch_info.egress_ser_factor = {ring_egress_ser, boundary_egress_ser};
    return switch_info;
}

static qsum_t get_three_switch_boundary_qids(CBDSharedBufferCP& cp) {
    qsum_t qids;
    qids.reserve(cp.in_queue_cnt());
    for (Queue* queue : cp.get_in_queues()) {
        qids.push_back(queue->get_id());
    }
    return qids;
}

static qsum_t get_three_switch_ring_node_qids(CBDSharedBufferCP& cp) {
    qsum_t qids;
    qids.reserve(cp.ring_nodes().size());
    for (const CBDNodeRef& node : cp.ring_nodes()) {
        SharedBufferSwitchQM* qm = dynamic_cast<SharedBufferSwitchQM*>(cp.get_qm(node.switch_id));
        if (qm == nullptr) {
            throw runtime_error("CBD test failed to resolve switch QM");
        }
        qids.push_back(qm->get_in_queue(node.ingress)->get_id());
    }
    return qids;
}

static bool test_cbd_shared_buffer_three_switch_base_workload_sat() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res =
        cp.check_workload_without_query(make_three_switch_cbd_t0_single_packet_workload(cp, total_time));
    cout << "[cbd_shared_buffer_three_switch_base_workload_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_cbd_shared_buffer_three_switch_boundary_sources_enqueue_at_t0_sat() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    cp.set_base_workload(make_three_switch_cbd_t0_single_packet_workload(cp, total_time));

    Query query(query_quant_t::EXISTS,
                time_range_t(0, 0),
                get_three_switch_boundary_qids(cp),
                metric_t::CENQ,
                Op(Op::Type::GE),
                cp.in_queue_cnt());
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[cbd_shared_buffer_three_switch_boundary_sources_enqueue_at_t0_sat] result: " << res
         << endl;
    return res == solver_res_t::SAT;
}

static bool test_cbd_shared_buffer_three_switch_no_same_slot_ring_handoff() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    cp.set_base_workload(make_three_switch_cbd_t0_single_packet_workload(cp, total_time));

    qsum_t ring_node_qids = get_three_switch_ring_node_qids(cp);
    Query query(query_quant_t::EXISTS,
                time_range_t(0, 0),
                ring_node_qids,
                metric_t::CENQ,
                Op(Op::Type::GE),
                ring_node_qids.size());
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[cbd_shared_buffer_three_switch_no_same_slot_ring_handoff] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_cbd_shared_buffer_three_switch_ring_handoff_at_t1_sat() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    cp.set_base_workload(make_three_switch_cbd_t0_single_packet_workload(cp, total_time));

    qsum_t ring_node_qids = get_three_switch_ring_node_qids(cp);
    Query query(query_quant_t::EXISTS,
                time_range_t(1, 1),
                ring_node_qids,
                metric_t::CENQ,
                Op(Op::Type::GE),
                ring_node_qids.size());
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[cbd_shared_buffer_three_switch_ring_handoff_at_t1_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_cbd_shared_buffer_three_switch_single_packet_any_wait_edge_sat() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = run_cbd_wait_edge_query(total_time,
                                               env_info,
                                               switch_info,
                                               make_three_switch_cbd_t0_single_packet_workload(cp, total_time),
                                               0,
                                               total_time - 1,
                                               1);
    cout << "[cbd_shared_buffer_three_switch_single_packet_any_wait_edge_sat] result: " << res
         << endl;
    return res == solver_res_t::SAT;
}

static bool test_cbd_shared_buffer_three_switch_candidate_workload_unsat() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = cp.check_workload_without_query(make_three_switch_cbd_workload(cp, total_time));
    cout << "[cbd_shared_buffer_three_switch_candidate_workload_unsat] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_cbd_shared_buffer_three_switch_single_packet_deadlock_sat() {
    const unsigned int total_time = 8;
    const unsigned int query_t1 = 0;
    const unsigned int query_t2 = total_time - 1;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = run_cbd_deadlock_query(
        total_time,
        env_info,
        switch_info,
        make_three_switch_cbd_t0_single_packet_workload(cp, total_time),
        query_quant_t::EXISTS,
        query_t1,
        query_t2);
    cout << "[cbd_shared_buffer_three_switch_single_packet_deadlock_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_cbd_shared_buffer_three_switch_single_packet_persistent_deadlock_sat() {
    const unsigned int total_time = 8;
    const unsigned int query_t1 = 2;
    const unsigned int query_t2 = total_time - 1;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = run_cbd_deadlock_query(
        total_time,
        env_info,
        switch_info,
        make_three_switch_cbd_t0_single_packet_workload(cp, total_time),
        query_quant_t::FORALL,
        query_t1,
        query_t2);
    cout << "[cbd_shared_buffer_three_switch_single_packet_persistent_deadlock_sat] result: " << res
         << endl;
    return res == solver_res_t::SAT;
}

static bool test_cbd_shared_buffer_three_switch_missing_s1_flow_no_deadlock() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = run_cbd_deadlock_query(
        total_time,
        env_info,
        switch_info,
        make_three_switch_cbd_t0_single_packet_workload_without_source(cp, total_time, 0),
        query_quant_t::EXISTS,
        0,
        total_time - 1);
    cout << "[cbd_shared_buffer_three_switch_missing_s1_flow_no_deadlock] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_cbd_shared_buffer_three_switch_missing_s2_flow_no_deadlock() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = run_cbd_deadlock_query(
        total_time,
        env_info,
        switch_info,
        make_three_switch_cbd_t0_single_packet_workload_without_source(cp, total_time, 1),
        query_quant_t::EXISTS,
        0,
        total_time - 1);
    cout << "[cbd_shared_buffer_three_switch_missing_s2_flow_no_deadlock] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_cbd_shared_buffer_three_switch_missing_s3_flow_no_deadlock() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = run_cbd_deadlock_query(
        total_time,
        env_info,
        switch_info,
        make_three_switch_cbd_t0_single_packet_workload_without_source(cp, total_time, 2),
        query_quant_t::EXISTS,
        0,
        total_time - 1);
    cout << "[cbd_shared_buffer_three_switch_missing_s3_flow_no_deadlock] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_cbd_shared_buffer_three_switch_override_s1_dst_no_deadlock() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = run_cbd_deadlock_query(
        total_time,
        env_info,
        switch_info,
        make_three_switch_cbd_t0_single_packet_workload_with_overridden_dst(cp, total_time, 0, 1),
        query_quant_t::EXISTS,
        0,
        total_time - 1);
    cout << "[cbd_shared_buffer_three_switch_override_s1_dst_no_deadlock] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_cbd_shared_buffer_three_switch_override_s2_dst_no_deadlock() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = run_cbd_deadlock_query(
        total_time,
        env_info,
        switch_info,
        make_three_switch_cbd_t0_single_packet_workload_with_overridden_dst(cp, total_time, 1, 0),
        query_quant_t::EXISTS,
        0,
        total_time - 1);
    cout << "[cbd_shared_buffer_three_switch_override_s2_dst_no_deadlock] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_cbd_shared_buffer_three_switch_override_s3_dst_no_deadlock() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info = make_three_switch_cbd_switch_info();

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = run_cbd_deadlock_query(
        total_time,
        env_info,
        switch_info,
        make_three_switch_cbd_t0_single_packet_workload_with_overridden_dst(cp, total_time, 2, 2),
        query_quant_t::EXISTS,
        0,
        total_time - 1);
    cout << "[cbd_shared_buffer_three_switch_override_s3_dst_no_deadlock] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_cbd_shared_buffer_three_switch_larger_ingress_pool_no_deadlock() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info =
        make_three_switch_cbd_switch_info(2, 3, 2, 2, 2, 1);

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = run_cbd_deadlock_query(
        total_time,
        env_info,
        switch_info,
        make_three_switch_cbd_t0_single_packet_workload(cp, total_time),
        query_quant_t::EXISTS,
        0,
        total_time - 1);
    cout << "[cbd_shared_buffer_three_switch_larger_ingress_pool_no_deadlock] result: " << res
         << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_cbd_shared_buffer_three_switch_larger_buffer_still_deadlock() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info = make_three_switch_cbd_env_info();
    SharedBufferSwitchInfo switch_info =
        make_three_switch_cbd_switch_info(3, 2, 2, 2, 2, 1);

    CBDSharedBufferCP cp = make_three_switch_cbd_cp(total_time, env_info, switch_info);
    solver_res_t res = run_cbd_deadlock_query(
        total_time,
        env_info,
        switch_info,
        make_three_switch_cbd_t0_single_packet_workload(cp, total_time),
        query_quant_t::EXISTS,
        0,
        total_time - 1);
    cout << "[cbd_shared_buffer_three_switch_larger_buffer_still_deadlock] result: " << res
         << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_chain_base_workload_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);

    Workload wl(8, cp.in_queue_cnt(), total_time);
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(1)),
                  time_range_t(total_time - 1, total_time - 1),
                  total_time));

    solver_res_t res = cp.check_workload_without_query(wl);
    cout << "[two_switch_chain_base_workload_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static Workload make_two_switch_chain_single_packet_workload(MultiSharedBufferCP& cp,
                                                             unsigned int total_time) {
    Workload wl(9, cp.in_queue_cnt(), total_time);
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(1)),
                  time_range_t(total_time - 1, total_time - 1),
                  total_time));
    return wl;
}

static Workload make_two_switch_chain_full_rate_workload(MultiSharedBufferCP& cp,
                                                         unsigned int total_time) {
    Workload wl(12, cp.in_queue_cnt(), total_time);
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0),
                           Op(Op::Type::EQ),
                           new Constant((int) total_time)),
                  time_range_t(total_time - 1, total_time - 1),
                  total_time));
    return wl;
}

static Workload make_two_switch_chain_four_packet_burst_workload(MultiSharedBufferCP& cp,
                                                                 unsigned int total_time) {
    Workload wl(13, cp.in_queue_cnt(), total_time);
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(1)),
                  time_range_t(0, 0),
                  total_time));
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(2)),
                  time_range_t(1, 1),
                  total_time));
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(3)),
                  time_range_t(2, 2),
                  total_time));
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(4)),
                  time_range_t(3, 3),
                  total_time));
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(4)),
                  time_range_t(total_time - 1, total_time - 1),
                  total_time));
    return wl;
}

static Workload make_two_switch_chain_t0_single_packet_workload(MultiSharedBufferCP& cp,
                                                                unsigned int total_time) {
    Workload wl(10, cp.in_queue_cnt(), total_time);
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(1)),
                  time_range_t(0, 0),
                  total_time));
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(1)),
                  time_range_t(total_time - 1, total_time - 1),
                  total_time));
    return wl;
}

static Workload make_two_switch_chain_t0_t1_two_packet_workload(MultiSharedBufferCP& cp,
                                                                unsigned int total_time) {
    Workload wl(11, cp.in_queue_cnt(), total_time);
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(1)),
                  time_range_t(0, 0),
                  total_time));
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(2)),
                  time_range_t(1, 1),
                  total_time));
    wl.add_spec(
        TimedSpec(new Comp(new Indiv(metric_t::DEMAND, 0), Op(Op::Type::EQ), new Constant(2)),
                  time_range_t(total_time - 1, total_time - 1),
                  total_time));
    return wl;
}

static Workload make_two_switch_two_source_t0_two_packet_workload(MultiSharedBufferCP& cp,
                                                                  unsigned int total_time) {
    Workload wl(14, cp.in_queue_cnt(), total_time);
    for (unsigned int q = 0; q < cp.in_queue_cnt(); q++) {
        wl.add_spec(
            TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q), Op(Op::Type::EQ), new Constant(1)),
                      time_range_t(0, 0),
                      total_time));
        wl.add_spec(
            TimedSpec(new Comp(new Indiv(metric_t::DEMAND, q), Op(Op::Type::EQ), new Constant(1)),
                      time_range_t(total_time - 1, total_time - 1),
                      total_time));
    }
    return wl;
}

static bool test_two_switch_chain_source_enqueue_at_t0_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_t0_single_packet_workload(cp, total_time));

    cid_t src_qid = cp.get_in_queues()[0]->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(0, 0),
                src_qid,
                metric_t::CENQ,
                Op(Op::Type::GE),
                1);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_source_enqueue_at_t0_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_chain_internal_handoff_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_single_packet_workload(cp, total_time));

    SharedBufferSwitchQM* s2_qm = dynamic_cast<SharedBufferSwitchQM*>(cp.get_qm("S2"));
    if (s2_qm == nullptr) {
        throw runtime_error("two_switch_chain_internal_handoff_sat failed to resolve S2");
    }

    cid_t s2_internal_ingress = s2_qm->get_in_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(0, total_time - 1),
                s2_internal_ingress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                1);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_internal_handoff_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_chain_no_same_slot_handoff() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_t0_single_packet_workload(cp, total_time));

    SharedBufferSwitchQM* s2_qm = dynamic_cast<SharedBufferSwitchQM*>(cp.get_qm("S2"));
    if (s2_qm == nullptr) {
        throw runtime_error("two_switch_chain_no_same_slot_handoff failed to resolve S2");
    }

    cid_t s2_internal_ingress = s2_qm->get_in_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(0, 0),
                s2_internal_ingress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                1);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_no_same_slot_handoff] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_two_switch_chain_handoff_at_t1_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_t0_single_packet_workload(cp, total_time));

    SharedBufferSwitchQM* s2_qm = dynamic_cast<SharedBufferSwitchQM*>(cp.get_qm("S2"));
    if (s2_qm == nullptr) {
        throw runtime_error("two_switch_chain_handoff_at_t1_sat failed to resolve S2");
    }

    cid_t s2_internal_ingress = s2_qm->get_in_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(1, 1),
                s2_internal_ingress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                1);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_handoff_at_t1_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_chain_two_packets_source_by_t1_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_t0_t1_two_packet_workload(cp, total_time));

    cid_t src_qid = cp.get_in_queues()[0]->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(1, 1),
                src_qid,
                metric_t::CENQ,
                Op(Op::Type::GE),
                2);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_two_packets_source_by_t1_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_chain_no_two_packet_handoff_by_t1() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_t0_t1_two_packet_workload(cp, total_time));

    SharedBufferSwitchQM* s2_qm = dynamic_cast<SharedBufferSwitchQM*>(cp.get_qm("S2"));
    if (s2_qm == nullptr) {
        throw runtime_error("two_switch_chain_no_two_packet_handoff_by_t1 failed to resolve S2");
    }

    cid_t s2_internal_ingress = s2_qm->get_in_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(1, 1),
                s2_internal_ingress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                2);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_no_two_packet_handoff_by_t1] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_two_switch_chain_two_packet_handoff_by_t2_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_t0_t1_two_packet_workload(cp, total_time));

    SharedBufferSwitchQM* s2_qm = dynamic_cast<SharedBufferSwitchQM*>(cp.get_qm("S2"));
    if (s2_qm == nullptr) {
        throw runtime_error("two_switch_chain_two_packet_handoff_by_t2_sat failed to resolve S2");
    }

    cid_t s2_internal_ingress = s2_qm->get_in_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(2, 2),
                s2_internal_ingress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                2);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_two_packet_handoff_by_t2_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_chain_end_to_end_delivery_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_single_packet_workload(cp, total_time));

    cid_t s2_boundary_egress = cp.get_out_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(0, total_time - 1),
                s2_boundary_egress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                1);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_end_to_end_delivery_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_chain_no_t1_end_to_end_delivery() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_t0_single_packet_workload(cp, total_time));

    cid_t s2_boundary_egress = cp.get_out_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(1, 1),
                s2_boundary_egress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                1);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_no_t1_end_to_end_delivery] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_two_switch_chain_delivery_at_t2_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_t0_single_packet_workload(cp, total_time));

    cid_t s2_boundary_egress = cp.get_out_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(2, 2),
                s2_boundary_egress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                1);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_delivery_at_t2_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_chain_no_two_packet_delivery_by_t2() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_t0_t1_two_packet_workload(cp, total_time));

    cid_t s2_boundary_egress = cp.get_out_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(2, 2),
                s2_boundary_egress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                2);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_no_two_packet_delivery_by_t2] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_two_switch_chain_two_packet_delivery_by_t3_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_chain_t0_t1_two_packet_workload(cp, total_time));

    cid_t s2_boundary_egress = cp.get_out_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(3, 3),
                s2_boundary_egress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                2);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_two_packet_delivery_by_t3_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_chain_downstream_bottleneck_delivery_ge_3_sat() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    TwoSwitchChainConfig cfg;
    cfg.s2_egress_ser = 2;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info, cfg);
    cp.set_base_workload(make_two_switch_chain_full_rate_workload(cp, total_time));

    cid_t s2_boundary_egress = cp.get_out_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(total_time - 1, total_time - 1),
                s2_boundary_egress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                3);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_downstream_bottleneck_delivery_ge_3_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_chain_downstream_bottleneck_no_delivery_ge_4_by_t7() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    TwoSwitchChainConfig cfg;
    cfg.s2_egress_ser = 2;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info, cfg);
    cp.set_base_workload(make_two_switch_chain_full_rate_workload(cp, total_time));

    cid_t s2_boundary_egress = cp.get_out_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(total_time - 1, total_time - 1),
                s2_boundary_egress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                4);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_downstream_bottleneck_no_delivery_ge_4_by_t7] result: " << res
         << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_two_switch_chain_backpressure_limits_source_enqueues() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    TwoSwitchChainConfig cfg;
    cfg.s1_buffer_slots = 2;
    cfg.s1_ingress_pool = 2;
    cfg.s1_headroom_pool = 2;
    cfg.s1_egress_pool = 2;
    cfg.s2_buffer_slots = 2;
    cfg.s2_ingress_pool = 2;
    cfg.s2_headroom_pool = 2;
    cfg.s2_egress_pool = 2;
    cfg.s2_egress_ser = 2;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info, cfg);
    cp.set_base_workload(make_two_switch_chain_full_rate_workload(cp, total_time));

    cid_t src_qid = cp.get_in_queues()[0]->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(total_time - 1, total_time - 1),
                src_qid,
                metric_t::CENQ,
                Op(Op::Type::GE),
                8);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_backpressure_limits_source_enqueues] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_two_switch_chain_backpressure_eventually_pauses_source() {
    const unsigned int total_time = 8;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    TwoSwitchChainConfig cfg;
    cfg.s1_buffer_slots = 2;
    cfg.s1_ingress_pool = 2;
    cfg.s1_headroom_pool = 2;
    cfg.s1_egress_pool = 2;
    cfg.s2_buffer_slots = 2;
    cfg.s2_ingress_pool = 2;
    cfg.s2_headroom_pool = 2;
    cfg.s2_egress_pool = 2;
    cfg.s2_egress_ser = 2;

    MultiSharedBufferCP cp = make_two_switch_chain_cp(total_time, env_info, cfg);
    cp.set_base_workload(make_two_switch_chain_full_rate_workload(cp, total_time));

    cid_t src_qid = cp.get_in_queues()[0]->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(0, total_time - 1),
                src_qid,
                metric_t::SENDER_PAUSED,
                Op(Op::Type::GE),
                1);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_chain_backpressure_eventually_pauses_source] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_chain_burst_pause_then_resume() {
    const unsigned int total_time = 10;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    TwoSwitchChainConfig cfg;
    cfg.s1_buffer_slots = 2;
    cfg.s1_ingress_pool = 2;
    cfg.s1_headroom_pool = 2;
    cfg.s1_egress_pool = 2;
    cfg.s2_buffer_slots = 2;
    cfg.s2_ingress_pool = 2;
    cfg.s2_headroom_pool = 2;
    cfg.s2_egress_pool = 2;
    cfg.s2_egress_ser = 2;

    MultiSharedBufferCP paused_cp = make_two_switch_chain_cp(total_time, env_info, cfg);
    paused_cp.set_base_workload(make_two_switch_chain_four_packet_burst_workload(paused_cp, total_time));
    cid_t paused_src_qid = paused_cp.get_in_queues()[0]->get_id();
    Query paused_query(query_quant_t::EXISTS,
                       time_range_t(0, 6),
                       paused_src_qid,
                       metric_t::SENDER_PAUSED,
                       Op(Op::Type::GE),
                       1);
    paused_cp.set_query(paused_query);
    solver_res_t paused_res = paused_cp.satisfy_query();
    cout << "[two_switch_chain_burst_pause_then_resume] paused result: " << paused_res << endl;

    MultiSharedBufferCP resumed_cp = make_two_switch_chain_cp(total_time, env_info, cfg);
    resumed_cp.set_base_workload(make_two_switch_chain_four_packet_burst_workload(resumed_cp, total_time));
    cid_t resumed_src_qid = resumed_cp.get_in_queues()[0]->get_id();
    Query resumed_query(query_quant_t::FORALL,
                        time_range_t(7, total_time - 1),
                        resumed_src_qid,
                        metric_t::SENDER_PAUSED,
                        Op(Op::Type::EQ),
                        0);
    resumed_cp.set_query(resumed_query);
    solver_res_t resumed_res = resumed_cp.satisfy_query();
    cout << "[two_switch_chain_burst_pause_then_resume] resumed result: " << resumed_res << endl;

    return paused_res == solver_res_t::SAT && resumed_res == solver_res_t::SAT;
}

static bool test_two_switch_two_source_enqueue_at_t0_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_two_source_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_two_source_t0_two_packet_workload(cp, total_time));

    qsum_t src_qids;
    for (Queue* queue : cp.get_in_queues()) src_qids.push_back(queue->get_id());

    Query query(query_quant_t::EXISTS,
                time_range_t(0, 0),
                src_qids,
                metric_t::CENQ,
                Op(Op::Type::GE),
                2);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_two_source_enqueue_at_t0_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_two_source_no_two_flow_handoff_by_t1() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_two_source_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_two_source_t0_two_packet_workload(cp, total_time));

    SharedBufferSwitchQM* s2_qm = dynamic_cast<SharedBufferSwitchQM*>(cp.get_qm("S2"));
    if (s2_qm == nullptr) {
        throw runtime_error("two_switch_two_source_no_two_flow_handoff_by_t1 failed to resolve S2");
    }

    cid_t s2_internal_ingress = s2_qm->get_in_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(1, 1),
                s2_internal_ingress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                2);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_two_source_no_two_flow_handoff_by_t1] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_two_switch_two_source_two_flow_handoff_by_t2_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_two_source_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_two_source_t0_two_packet_workload(cp, total_time));

    SharedBufferSwitchQM* s2_qm = dynamic_cast<SharedBufferSwitchQM*>(cp.get_qm("S2"));
    if (s2_qm == nullptr) {
        throw runtime_error("two_switch_two_source_two_flow_handoff_by_t2_sat failed to resolve S2");
    }

    cid_t s2_internal_ingress = s2_qm->get_in_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(2, 2),
                s2_internal_ingress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                2);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_two_source_two_flow_handoff_by_t2_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

static bool test_two_switch_two_source_no_two_flow_delivery_by_t2() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_two_source_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_two_source_t0_two_packet_workload(cp, total_time));

    cid_t s2_boundary_egress = cp.get_out_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(2, 2),
                s2_boundary_egress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                2);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_two_source_no_two_flow_delivery_by_t2] result: " << res << endl;
    return res == solver_res_t::UNSAT;
}

static bool test_two_switch_two_source_two_flow_delivery_by_t3_sat() {
    const unsigned int total_time = 6;

    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;

    MultiSharedBufferCP cp = make_two_switch_two_source_chain_cp(total_time, env_info);
    cp.set_base_workload(make_two_switch_two_source_t0_two_packet_workload(cp, total_time));

    cid_t s2_boundary_egress = cp.get_out_queue(0)->get_id();
    Query query(query_quant_t::EXISTS,
                time_range_t(3, 3),
                s2_boundary_egress,
                metric_t::CENQ,
                Op(Op::Type::GE),
                2);
    cp.set_query(query);

    solver_res_t res = cp.satisfy_query();
    cout << "[two_switch_two_source_two_flow_delivery_by_t3_sat] result: " << res << endl;
    return res == solver_res_t::SAT;
}

void MultiSharedBufferTest::add_to_runner(TestRunner* runner) {
    runner->add_test_case("two_switch_chain_base_workload_sat",
                          test_two_switch_chain_base_workload_sat);
    runner->add_test_case("two_switch_chain_source_enqueue_at_t0_sat",
                          test_two_switch_chain_source_enqueue_at_t0_sat);
    runner->add_test_case("two_switch_chain_no_same_slot_handoff",
                          test_two_switch_chain_no_same_slot_handoff);
    runner->add_test_case("two_switch_chain_handoff_at_t1_sat",
                          test_two_switch_chain_handoff_at_t1_sat);
    runner->add_test_case("two_switch_chain_two_packets_source_by_t1_sat",
                          test_two_switch_chain_two_packets_source_by_t1_sat);
    runner->add_test_case("two_switch_chain_no_two_packet_handoff_by_t1",
                          test_two_switch_chain_no_two_packet_handoff_by_t1);
    runner->add_test_case("two_switch_chain_two_packet_handoff_by_t2_sat",
                          test_two_switch_chain_two_packet_handoff_by_t2_sat);
    runner->add_test_case("two_switch_two_source_enqueue_at_t0_sat",
                          test_two_switch_two_source_enqueue_at_t0_sat);
    runner->add_test_case("two_switch_two_source_no_two_flow_handoff_by_t1",
                          test_two_switch_two_source_no_two_flow_handoff_by_t1);
    runner->add_test_case("two_switch_two_source_two_flow_handoff_by_t2_sat",
                          test_two_switch_two_source_two_flow_handoff_by_t2_sat);
    runner->add_test_case("two_switch_two_source_no_two_flow_delivery_by_t2",
                          test_two_switch_two_source_no_two_flow_delivery_by_t2);
    runner->add_test_case("two_switch_two_source_two_flow_delivery_by_t3_sat",
                          test_two_switch_two_source_two_flow_delivery_by_t3_sat);
    runner->add_test_case("two_switch_chain_internal_handoff_sat",
                          test_two_switch_chain_internal_handoff_sat);
    runner->add_test_case("two_switch_chain_no_t1_end_to_end_delivery",
                          test_two_switch_chain_no_t1_end_to_end_delivery);
    runner->add_test_case("two_switch_chain_delivery_at_t2_sat",
                          test_two_switch_chain_delivery_at_t2_sat);
    runner->add_test_case("two_switch_chain_no_two_packet_delivery_by_t2",
                          test_two_switch_chain_no_two_packet_delivery_by_t2);
    runner->add_test_case("two_switch_chain_two_packet_delivery_by_t3_sat",
                          test_two_switch_chain_two_packet_delivery_by_t3_sat);
    runner->add_test_case("two_switch_chain_downstream_bottleneck_delivery_ge_3_sat",
                          test_two_switch_chain_downstream_bottleneck_delivery_ge_3_sat);
    runner->add_test_case("two_switch_chain_downstream_bottleneck_no_delivery_ge_4_by_t7",
                          test_two_switch_chain_downstream_bottleneck_no_delivery_ge_4_by_t7);
    runner->add_test_case("two_switch_chain_backpressure_limits_source_enqueues",
                          test_two_switch_chain_backpressure_limits_source_enqueues);
    runner->add_test_case("two_switch_chain_backpressure_eventually_pauses_source",
                          test_two_switch_chain_backpressure_eventually_pauses_source);
    runner->add_test_case("two_switch_chain_burst_pause_then_resume",
                          test_two_switch_chain_burst_pause_then_resume);
    runner->add_test_case("two_switch_chain_end_to_end_delivery_sat",
                          test_two_switch_chain_end_to_end_delivery_sat);
    runner->add_test_case("cbd_shared_buffer_three_switch_base_workload_sat",
                          test_cbd_shared_buffer_three_switch_base_workload_sat);
    runner->add_test_case("cbd_shared_buffer_three_switch_boundary_sources_enqueue_at_t0_sat",
                          test_cbd_shared_buffer_three_switch_boundary_sources_enqueue_at_t0_sat);
    runner->add_test_case("cbd_shared_buffer_three_switch_no_same_slot_ring_handoff",
                          test_cbd_shared_buffer_three_switch_no_same_slot_ring_handoff);
    runner->add_test_case("cbd_shared_buffer_three_switch_ring_handoff_at_t1_sat",
                          test_cbd_shared_buffer_three_switch_ring_handoff_at_t1_sat);
    runner->add_test_case("cbd_shared_buffer_three_switch_single_packet_any_wait_edge_sat",
                          test_cbd_shared_buffer_three_switch_single_packet_any_wait_edge_sat);
    runner->add_test_case("cbd_shared_buffer_three_switch_candidate_workload_unsat",
                          test_cbd_shared_buffer_three_switch_candidate_workload_unsat);
    runner->add_test_case("cbd_shared_buffer_three_switch_single_packet_deadlock_sat",
                          test_cbd_shared_buffer_three_switch_single_packet_deadlock_sat);
    runner->add_test_case("cbd_shared_buffer_three_switch_single_packet_persistent_deadlock_sat",
                          test_cbd_shared_buffer_three_switch_single_packet_persistent_deadlock_sat);
    runner->add_test_case("cbd_shared_buffer_three_switch_missing_s1_flow_no_deadlock",
                          test_cbd_shared_buffer_three_switch_missing_s1_flow_no_deadlock);
    runner->add_test_case("cbd_shared_buffer_three_switch_missing_s2_flow_no_deadlock",
                          test_cbd_shared_buffer_three_switch_missing_s2_flow_no_deadlock);
    runner->add_test_case("cbd_shared_buffer_three_switch_missing_s3_flow_no_deadlock",
                          test_cbd_shared_buffer_three_switch_missing_s3_flow_no_deadlock);
    runner->add_test_case("cbd_shared_buffer_three_switch_override_s1_dst_no_deadlock",
                          test_cbd_shared_buffer_three_switch_override_s1_dst_no_deadlock);
    runner->add_test_case("cbd_shared_buffer_three_switch_override_s2_dst_no_deadlock",
                          test_cbd_shared_buffer_three_switch_override_s2_dst_no_deadlock);
    runner->add_test_case("cbd_shared_buffer_three_switch_override_s3_dst_no_deadlock",
                          test_cbd_shared_buffer_three_switch_override_s3_dst_no_deadlock);
    runner->add_test_case("cbd_shared_buffer_three_switch_larger_ingress_pool_no_deadlock",
                          test_cbd_shared_buffer_three_switch_larger_ingress_pool_no_deadlock);
    runner->add_test_case("cbd_shared_buffer_three_switch_larger_buffer_still_deadlock",
                          test_cbd_shared_buffer_three_switch_larger_buffer_still_deadlock);
}
