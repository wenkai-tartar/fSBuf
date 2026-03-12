#include "pfc_deadlock_test.hpp"

#include <array>
#include <iostream>
#include <string>
#include <vector>

#include "cbd_shared_buffer.hpp"
#include "query.hpp"

using namespace std;

namespace {

struct ThreeSwitchRingSwitchParams {
    unsigned int buffer_slot_cnt = 2;
    unsigned int ingress_pool_size = 2;
    unsigned int headroom_pool_size = 2;
    unsigned int egress_lossless_pool_size = 2;
    unsigned int ring_egress_ser_factor = 2;
    unsigned int boundary_egress_ser_factor = 1;
};

struct RingFlowSpec {
    int fixed_dst = -1;
    int dst_anchor_time = -1;
    bool same_dst_across_time = false;
    vector<pair<unsigned int, int>> demand_lower_bounds;
    vector<pair<unsigned int, int>> demand_upper_bounds;
};

enum class DeadlockCheckKind {
    ExistsDeadlock,
    PersistentDeadlock,
};

enum class ScenarioExpectation {
    Infeasible,
    DeadlockSafe,
    DeadlockUnsafe,
};

struct ThreeSwitchRingScenario {
    string name;
    unsigned int total_time = 8;
    SharedBufferEnvInfo env_info;
    ThreeSwitchRingSwitchParams switch_params;
    std::array<RingFlowSpec, 3> flows;
    DeadlockCheckKind check_kind = DeadlockCheckKind::ExistsDeadlock;
    unsigned int query_t1 = 0;
    unsigned int query_t2 = 7;
    ScenarioExpectation expected = ScenarioExpectation::DeadlockUnsafe;
};

struct ScenarioResult {
    solver_res_t workload_res = solver_res_t::UNKNOWN;
    solver_res_t deadlock_res = solver_res_t::UNKNOWN;
};

static SharedBufferEnvInfo make_ring_env_info() {
    SharedBufferEnvInfo env_info;
    env_info.max_demand_per_t = 1;
    env_info.boundary_egress_ctrl_default_false = true;
    return env_info;
}

static SharedBufferSwitchInfo make_ring_switch_info(const ThreeSwitchRingSwitchParams& params) {
    SharedBufferSwitchInfo switch_info;
    switch_info.ingress_queue_cnt = 2; // q0 internal ring ingress, q1 boundary ingress
    switch_info.egress_queue_cnt = 2;  // e0 internal ring egress, e1 boundary egress
    switch_info.buffer_slot_cnt = params.buffer_slot_cnt;
    switch_info.ingress_pool_size = params.ingress_pool_size;
    switch_info.headroom_pool_size = params.headroom_pool_size;
    switch_info.egress_lossless_pool_size = params.egress_lossless_pool_size;
    switch_info.timing_mode = SharedBufferSwitchInfo::TimingMode::DEQ_FIRST;
    switch_info.egress_ser_factor = {
        params.ring_egress_ser_factor,
        params.boundary_egress_ser_factor,
    };
    return switch_info;
}

static CBDSharedBufferCP make_three_switch_ring_cp(unsigned int total_time,
                                                   const SharedBufferEnvInfo& env_info,
                                                   const SharedBufferSwitchInfo& switch_info) {
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

    vector<CBDRingEdge> ring_edges = {
        {"S1", 0, 0, "S3", 0, 0},
        {"S3", 0, 0, "S2", 0, 0},
        {"S2", 0, 0, "S1", 0, 0},
    };

    return CBDSharedBufferCP(total_time, env_info, std::move(switch_configs), ring_edges);
}

static void add_demand_bounds(Workload& wl,
                              unsigned int queue,
                              metric_t metric,
                              const vector<pair<unsigned int, int>>& bounds,
                              Op::Type op,
                              unsigned int total_time) {
    for (const auto& [time, value] : bounds) {
        wl.add_spec(TimedSpec(new Comp(new Indiv(metric, queue), Op(op), new Constant(value)),
                              time_range_t(time, time),
                              total_time));
    }
}

static Workload make_three_switch_ring_workload(CBDSharedBufferCP& cp,
                                                const ThreeSwitchRingScenario& scenario) {
    Workload wl(100, cp.in_queue_cnt(), scenario.total_time);
    for (unsigned int q = 0; q < scenario.flows.size(); q++) {
        const RingFlowSpec& flow = scenario.flows[q];
        add_demand_bounds(wl,
                          q,
                          metric_t::DEMAND,
                          flow.demand_lower_bounds,
                          Op::Type::GE,
                          scenario.total_time);
        add_demand_bounds(wl,
                          q,
                          metric_t::DEMAND,
                          flow.demand_upper_bounds,
                          Op::Type::LE,
                          scenario.total_time);
        if (flow.fixed_dst >= 0 && flow.dst_anchor_time >= 0) {
            wl.add_spec(TimedSpec(new Comp(new Indiv(metric_t::DST, q),
                                           Op(Op::Type::EQ),
                                           new Constant(flow.fixed_dst)),
                                  time_range_t(flow.dst_anchor_time, flow.dst_anchor_time),
                                  scenario.total_time));
            if (flow.same_dst_across_time) {
                wl.add_spec(TimedSpec(new Same(metric_t::DST, q),
                                      time_range_t(flow.dst_anchor_time, scenario.total_time - 1),
                                      scenario.total_time));
            }
        }
    }
    return wl;
}

static Query make_deadlock_query(CBDSharedBufferCP& cp,
                                 const ThreeSwitchRingScenario& scenario) {
    qsum_t ring_node_qids;
    ring_node_qids.reserve(cp.ring_nodes().size());
    for (const CBDNodeRef& node : cp.ring_nodes()) {
        SharedBufferSwitchQM* qm = dynamic_cast<SharedBufferSwitchQM*>(cp.get_qm(node.switch_id));
        if (qm == nullptr) {
            throw runtime_error("PFC deadlock framework failed to resolve switch QM");
        }
        ring_node_qids.push_back(qm->get_in_queue(node.ingress)->get_id());
    }

    query_quant_t quant = query_quant_t::EXISTS;
    if (scenario.check_kind == DeadlockCheckKind::PersistentDeadlock) {
        quant = query_quant_t::FORALL;
    }

    return Query(quant,
                 time_range_t(scenario.query_t1, scenario.query_t2),
                 ring_node_qids,
                 metric_t::CBD_DEADLOCK_NODE,
                 Op(Op::Type::GE),
                 ring_node_qids.size());
}

static ScenarioResult run_three_switch_ring_scenario(const ThreeSwitchRingScenario& scenario) {
    SharedBufferSwitchInfo switch_info = make_ring_switch_info(scenario.switch_params);
    CBDSharedBufferCP cp =
        make_three_switch_ring_cp(scenario.total_time, scenario.env_info, switch_info);

    Workload wl = make_three_switch_ring_workload(cp, scenario);
    ScenarioResult result;
    result.workload_res = cp.check_workload_without_query(wl);
    if (result.workload_res != solver_res_t::SAT) {
        return result;
    }

    cp.set_base_workload(wl);
    Query query = make_deadlock_query(cp, scenario);
    cp.set_query(query);
    result.deadlock_res = cp.satisfy_query();
    return result;
}

static bool matches_expectation(const ThreeSwitchRingScenario& scenario, const ScenarioResult& result) {
    switch (scenario.expected) {
        case ScenarioExpectation::Infeasible:
            return result.workload_res == solver_res_t::UNSAT;
        case ScenarioExpectation::DeadlockSafe:
            return result.workload_res == solver_res_t::SAT &&
                   result.deadlock_res == solver_res_t::UNSAT;
        case ScenarioExpectation::DeadlockUnsafe:
            return result.workload_res == solver_res_t::SAT &&
                   result.deadlock_res == solver_res_t::SAT;
    }
    return false;
}

static RingFlowSpec make_t0_single_packet_flow(unsigned int dst, unsigned int total_time) {
    RingFlowSpec flow;
    flow.fixed_dst = static_cast<int>(dst);
    flow.dst_anchor_time = 0;
    flow.same_dst_across_time = true;
    flow.demand_lower_bounds = {{0, 1}, {total_time - 1, 1}};
    flow.demand_upper_bounds = {{0, 1}, {total_time - 1, 1}};
    return flow;
}

static RingFlowSpec make_disabled_flow(unsigned int total_time) {
    RingFlowSpec flow;
    flow.demand_lower_bounds = {{0, 0}, {total_time - 1, 0}};
    flow.demand_upper_bounds = {{0, 0}, {total_time - 1, 0}};
    return flow;
}

static RingFlowSpec make_full_rate_flow(unsigned int dst, unsigned int total_time) {
    RingFlowSpec flow;
    flow.fixed_dst = static_cast<int>(dst);
    flow.dst_anchor_time = 0;
    flow.same_dst_across_time = true;
    flow.demand_lower_bounds = {{0, 1}, {total_time - 1, static_cast<int>(total_time)}};
    flow.demand_upper_bounds = {{0, 1}, {total_time - 1, static_cast<int>(total_time)}};
    return flow;
}

static ThreeSwitchRingScenario make_base_deadlock_scenario(const string& name,
                                                           ScenarioExpectation expected) {
    ThreeSwitchRingScenario scenario;
    scenario.name = name;
    scenario.total_time = 8;
    scenario.env_info = make_ring_env_info();
    scenario.switch_params = {};
    scenario.flows = {
        make_t0_single_packet_flow(0, scenario.total_time),
        make_t0_single_packet_flow(2, scenario.total_time),
        make_t0_single_packet_flow(1, scenario.total_time),
    };
    scenario.check_kind = DeadlockCheckKind::ExistsDeadlock;
    scenario.query_t1 = 0;
    scenario.query_t2 = scenario.total_time - 1;
    scenario.expected = expected;
    return scenario;
}

static ThreeSwitchRingScenario make_persistent_deadlock_scenario() {
    ThreeSwitchRingScenario scenario =
        make_base_deadlock_scenario("persistent_single_packet_deadlock", ScenarioExpectation::DeadlockUnsafe);
    scenario.check_kind = DeadlockCheckKind::PersistentDeadlock;
    scenario.query_t1 = 2;
    scenario.query_t2 = scenario.total_time - 1;
    return scenario;
}

static ThreeSwitchRingScenario make_free_dst_deadlock_scenario(const string& name,
                                                               ScenarioExpectation expected) {
    ThreeSwitchRingScenario scenario;
    scenario.name = name;
    scenario.total_time = 8;
    scenario.env_info = make_ring_env_info();
    scenario.switch_params = {};
    for (RingFlowSpec& flow : scenario.flows) {
        flow.demand_lower_bounds = {{0, 1}, {scenario.total_time - 1, 1}};
        flow.demand_upper_bounds = {{0, 1}, {scenario.total_time - 1, 1}};
    }
    scenario.check_kind = DeadlockCheckKind::ExistsDeadlock;
    scenario.query_t1 = 0;
    scenario.query_t2 = scenario.total_time - 1;
    scenario.expected = expected;
    return scenario;
}

static bool test_pfc_deadlock_three_switch_ring_framework() {
    vector<ThreeSwitchRingScenario> scenarios;

    scenarios.push_back(make_base_deadlock_scenario("single_packet_deadlock",
                                                    ScenarioExpectation::DeadlockUnsafe));
    scenarios.push_back(make_persistent_deadlock_scenario());
    scenarios.push_back(
        make_free_dst_deadlock_scenario("single_packet_deadlock_with_free_dst",
                                        ScenarioExpectation::DeadlockUnsafe));

    ThreeSwitchRingScenario missing_s1 =
        make_base_deadlock_scenario("missing_s1_flow", ScenarioExpectation::DeadlockSafe);
    missing_s1.flows[0] = make_disabled_flow(missing_s1.total_time);
    scenarios.push_back(missing_s1);

    ThreeSwitchRingScenario missing_s2 =
        make_base_deadlock_scenario("missing_s2_flow", ScenarioExpectation::DeadlockSafe);
    missing_s2.flows[1] = make_disabled_flow(missing_s2.total_time);
    scenarios.push_back(missing_s2);

    ThreeSwitchRingScenario missing_s3 =
        make_base_deadlock_scenario("missing_s3_flow", ScenarioExpectation::DeadlockSafe);
    missing_s3.flows[2] = make_disabled_flow(missing_s3.total_time);
    scenarios.push_back(missing_s3);

    ThreeSwitchRingScenario override_s1 =
        make_base_deadlock_scenario("override_s1_dst", ScenarioExpectation::DeadlockSafe);
    override_s1.flows[0].fixed_dst = 1;
    scenarios.push_back(override_s1);

    ThreeSwitchRingScenario override_s2 =
        make_base_deadlock_scenario("override_s2_dst", ScenarioExpectation::DeadlockSafe);
    override_s2.flows[1].fixed_dst = 0;
    scenarios.push_back(override_s2);

    ThreeSwitchRingScenario override_s3 =
        make_base_deadlock_scenario("override_s3_dst", ScenarioExpectation::DeadlockSafe);
    override_s3.flows[2].fixed_dst = 2;
    scenarios.push_back(override_s3);

    ThreeSwitchRingScenario larger_ingress_pool =
        make_base_deadlock_scenario("larger_ingress_pool", ScenarioExpectation::DeadlockSafe);
    larger_ingress_pool.switch_params.ingress_pool_size = 3;
    scenarios.push_back(larger_ingress_pool);

    ThreeSwitchRingScenario larger_buffer =
        make_base_deadlock_scenario("larger_buffer", ScenarioExpectation::DeadlockUnsafe);
    larger_buffer.switch_params.buffer_slot_cnt = 3;
    scenarios.push_back(larger_buffer);

    ThreeSwitchRingScenario infeasible_full_rate;
    infeasible_full_rate.name = "full_rate_candidate";
    infeasible_full_rate.total_time = 8;
    infeasible_full_rate.env_info = make_ring_env_info();
    infeasible_full_rate.switch_params = {};
    infeasible_full_rate.flows = {
        make_full_rate_flow(0, infeasible_full_rate.total_time),
        make_full_rate_flow(2, infeasible_full_rate.total_time),
        make_full_rate_flow(1, infeasible_full_rate.total_time),
    };
    infeasible_full_rate.check_kind = DeadlockCheckKind::ExistsDeadlock;
    infeasible_full_rate.query_t1 = 0;
    infeasible_full_rate.query_t2 = infeasible_full_rate.total_time - 1;
    infeasible_full_rate.expected = ScenarioExpectation::Infeasible;
    scenarios.push_back(infeasible_full_rate);

    bool ok = true;
    for (const ThreeSwitchRingScenario& scenario : scenarios) {
        ScenarioResult result = run_three_switch_ring_scenario(scenario);
        cout << "[pfc_deadlock_three_switch_ring_framework] " << scenario.name
             << " workload=" << result.workload_res;
        if (result.workload_res == solver_res_t::SAT) {
            cout << " deadlock=" << result.deadlock_res;
        }
        cout << endl;
        ok = matches_expectation(scenario, result) && ok;
    }
    return ok;
}

} // namespace

void PFCDeadlockTest::add_to_runner(TestRunner *runner) {
    runner->add_test_case("pfc_deadlock_three_switch_ring_framework",
                          test_pfc_deadlock_three_switch_ring_framework);
}
