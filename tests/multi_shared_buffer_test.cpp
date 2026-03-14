#include "multi_shared_buffer_test.hpp"

#include <iostream>

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
    s1_cfg.route_policy = make_unique<TableRoutePolicy>(vector<unsigned int>{0});
    switch_configs.push_back(std::move(s1_cfg));

    MultiSharedBufferSwitchConfig s2_cfg;
    s2_cfg.switch_id = "S2";
    s2_cfg.switch_info = s2_info;
    s2_cfg.route_policy = make_unique<TableRoutePolicy>(vector<unsigned int>{0});
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
    s1_cfg.route_policy = make_unique<TableRoutePolicy>(vector<unsigned int>{0});
    switch_configs.push_back(std::move(s1_cfg));

    MultiSharedBufferSwitchConfig s2_cfg;
    s2_cfg.switch_id = "S2";
    s2_cfg.switch_info = s2_info;
    s2_cfg.route_policy = make_unique<TableRoutePolicy>(vector<unsigned int>{0});
    switch_configs.push_back(std::move(s2_cfg));

    vector<MultiSharedBufferEdge> internal_edges = {
        {"S1", 0, "S2", 0},
    };

    return MultiSharedBufferCP(total_time, env_info, std::move(switch_configs), internal_edges);
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

    MultiSharedBufferCP clear_cp = make_two_switch_chain_cp(total_time, env_info, cfg);
    clear_cp.set_base_workload(make_two_switch_chain_four_packet_burst_workload(clear_cp, total_time));
    cid_t clear_src_qid = clear_cp.get_in_queues()[0]->get_id();
    Query clear_query(query_quant_t::FORALL,
                      time_range_t(0, 1),
                      clear_src_qid,
                      metric_t::SENDER_PAUSED,
                      Op(Op::Type::EQ),
                      0);
    clear_cp.set_query(clear_query);
    solver_res_t clear_res = clear_cp.satisfy_query();
    cout << "[two_switch_chain_burst_pause_then_resume] clear result: " << clear_res << endl;

    MultiSharedBufferCP paused_cp = make_two_switch_chain_cp(total_time, env_info, cfg);
    paused_cp.set_base_workload(make_two_switch_chain_four_packet_burst_workload(paused_cp, total_time));
    cid_t paused_src_qid = paused_cp.get_in_queues()[0]->get_id();
    Query paused_query(query_quant_t::EXISTS,
                       time_range_t(2, 3),
                       paused_src_qid,
                       metric_t::SENDER_PAUSED,
                       Op(Op::Type::EQ),
                       1);
    paused_cp.set_query(paused_query);
    solver_res_t paused_res = paused_cp.satisfy_query();
    cout << "[two_switch_chain_burst_pause_then_resume] paused result: " << paused_res << endl;

    MultiSharedBufferCP resumed_cp = make_two_switch_chain_cp(total_time, env_info, cfg);
    resumed_cp.set_base_workload(make_two_switch_chain_four_packet_burst_workload(resumed_cp, total_time));
    cid_t resumed_src_qid = resumed_cp.get_in_queues()[0]->get_id();
    Query resumed_query(query_quant_t::FORALL,
                        time_range_t(4, total_time - 1),
                        resumed_src_qid,
                        metric_t::SENDER_PAUSED,
                        Op(Op::Type::EQ),
                        0);
    resumed_cp.set_query(resumed_query);
    solver_res_t resumed_res = resumed_cp.satisfy_query();
    cout << "[two_switch_chain_burst_pause_then_resume] resumed result: " << resumed_res << endl;

    return clear_res == solver_res_t::SAT && paused_res == solver_res_t::SAT &&
           resumed_res == solver_res_t::SAT;
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
}
