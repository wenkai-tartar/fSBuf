//  shared_buffer_point.cpp
//  FSB
//
//  Created by Wenkai Li on 2/27/26.
//  Copyright © 2026 Wenkai Li. All rights reserved.
//

#include "shared_buffer_point.hpp"

#include <cstdio>
#include <sstream>
#include <stdexcept>

#include "shared_buffer_switch_qm.hpp"

namespace {
bool eval_bool(model& m, const expr& e) {
    return m.eval(e, true).is_true();
}

int eval_int(model& m, const expr& e, int fallback = 0) {
    expr val = m.eval(e, true);
    if (val.is_numeral()) return val.get_numeral_int();
    return fallback;
}

unsigned int eval_uint(model& m, const expr& e, unsigned int fallback = 0) {
    expr val = m.eval(e, true);
    if (val.is_numeral()) return val.get_numeral_uint();
    return fallback;
}

PacketSnapshot eval_packet_snapshot(model& m, NetContext& net_ctx, const expr& pkt) {
    PacketSnapshot snapshot;
    snapshot.valid = eval_bool(m, net_ctx.pkt2val(pkt));
    if (!snapshot.valid) return snapshot;

    snapshot.meta1 = eval_int(m, net_ctx.pkt2meta1(pkt), -1);
    snapshot.meta2 = eval_int(m, net_ctx.pkt2meta2(pkt), -1);
    return snapshot;
}

bool can_emit_now(model& m,
                  NetContext& net_ctx,
                  SharedBufferSwitchQM* qm,
                  unsigned int e,
                  unsigned int t) {
    Queue* outq = qm->get_out_queue(e);
    if (outq->max_enq() == 0) return false;

    unsigned int ser_factor = qm->egress_serialization_factor(e);
    for (unsigned int dt = 1; dt < ser_factor && dt <= t; dt++) {
        if (eval_bool(m, net_ctx.pkt2val(outq->enqs(0)[t - dt]))) return false;
    }
    return true;
}

string query_quant_str(query_quant_t quant) {
    switch (quant) {
        case query_quant_t::FORALL: return "FORALL";
        case query_quant_t::EXISTS: return "EXISTS";
    }
    return "UNKNOWN";
}

string query_lhs_str(const query_lhs_t& lhs) {
    if (holds_alternative<cid_t>(lhs)) {
        return get<cid_t>(lhs);
    }
    if (holds_alternative<qdiff_t>(lhs)) {
        const qdiff_t& diff = get<qdiff_t>(lhs);
        return "[" + diff.first + " - " + diff.second + "]";
    }

    const qsum_t& ids = get<qsum_t>(lhs);
    stringstream ss;
    ss << "[";
    for (unsigned int i = 0; i < ids.size(); i++) {
        if (i > 0) ss << ", ";
        ss << ids[i];
    }
    ss << "]";
    return ss.str();
}
} // namespace

SharedBufferPoint::SharedBufferPoint(unsigned int total_time, SharedBufferEnvInfo info):
ContentionPoint(total_time),
env_info(info) {
}

solver_res_t SharedBufferPoint::satisfy_query_with_model(string& model_str) {
    model_str.clear();

    if (!query_is_set) {
        cout << "SharedBufferPoint::satisfy_query_with_model: Query is not set." << endl;
        return solver_res_t::UNKNOWN;
    }

    z3_solver->push();
    z3_solver->add(base_wl_expr, "base_wl");
    z3_solver->add(query_expr, "query");

    check_result z3_res = z3_solver->check();
    solver_res_t res = get_solver_res_t(z3_res);
    if (z3_res == sat) {
        model m = z3_solver->get_model();
        model_str = get_model_str(m);
    }

    z3_solver->pop();

    return res;
}

solver_res_t SharedBufferPoint::optimize_query_witness_with_model(SBIndexedExample* eg,
                                                                  string& model_str,
                                                                  unsigned int max_empty_queue,
                                                                  qset_t* target_queues,
                                                                  qset_t* zero_queues) {
    return optimize_query_witness_with_model_impl(eg,
                                                  model_str,
                                                  max_empty_queue,
                                                  target_queues,
                                                  zero_queues,
                                                  WitnessObjectiveOrder::SmoothBeforeTraffic,
                                                  "optimize_query_witness_with_model");
}

solver_res_t SharedBufferPoint::optimize_query_witness_traffic_first_with_model(
    SBIndexedExample* eg,
    string& model_str,
    unsigned int max_empty_queue,
    qset_t* target_queues,
    qset_t* zero_queues) {
    return optimize_query_witness_with_model_impl(eg,
                                                  model_str,
                                                  max_empty_queue,
                                                  target_queues,
                                                  zero_queues,
                                                  WitnessObjectiveOrder::TrafficBeforeSmoothness,
                                                  "optimize_query_witness_traffic_first_with_model");
}

solver_res_t SharedBufferPoint::optimize_query_witness_min_simultaneous_enq_with_model(
    SBIndexedExample* eg,
    string& model_str,
    unsigned int max_empty_queue,
    qset_t* target_queues,
    qset_t* zero_queues) {
    return optimize_query_witness_with_model_impl(
        eg,
        model_str,
        max_empty_queue,
        target_queues,
        zero_queues,
        WitnessObjectiveOrder::MinSimultaneousEnqThenSparse,
        "optimize_query_witness_min_simultaneous_enq_with_model");
}

solver_res_t SharedBufferPoint::optimize_query_witness_with_model_impl(
    SBIndexedExample* eg,
    string& model_str,
    unsigned int max_empty_queue,
    qset_t* target_queues,
    qset_t* zero_queues,
    WitnessObjectiveOrder order,
    const char* api_name) {
    model_str.clear();
    if (eg != nullptr) *eg = SBIndexedExample();
    if (target_queues != nullptr) target_queues->clear();
    if (zero_queues != nullptr) zero_queues->clear();

    if (!query_is_set) {
        cout << "SharedBufferPoint::" << api_name << ": Query is not set." << endl;
        return solver_res_t::UNKNOWN;
    }

    z3_optimizer->push();
    z3_optimizer->add(base_wl_expr, "base_wl");
    z3_optimizer->add(query_expr, "query");

    expr_vector simultaneous_enq_cost_vec(net_ctx.z3_ctx());
    expr_vector per_queue_enq_sum(net_ctx.z3_ctx());
    expr_vector empty_queues_expr_vec(net_ctx.z3_ctx());
    expr_vector smooth_enq_slots_vec(net_ctx.z3_ctx());

    for (unsigned int t = 0; t < total_time; t++) {
        expr_vector all_enq_vec(net_ctx.z3_ctx());
        for (unsigned int q = 0; q < in_queues.size(); q++) {
            all_enq_vec.push_back(in_queues[q]->enq_cnt(t) > 0);
        }
        simultaneous_enq_cost_vec.push_back(
            ite(mk_and(all_enq_vec), net_ctx.int_val(1), net_ctx.int_val(0)));
    }

    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        expr_vector this_queues_enqs(net_ctx.z3_ctx());
        for (unsigned int t = 0; t < total_time; t++) {
            this_queues_enqs.push_back(queue->enq_cnt(t));
        }

        expr total_enqs = sum(this_queues_enqs);
        per_queue_enq_sum.push_back(total_enqs);
        empty_queues_expr_vec.push_back(
            ite(total_enqs == 0, net_ctx.int_val(1), net_ctx.int_val(0)));

        for (unsigned int t = 0; t + 1 < total_time; t++) {
            expr pkt1 = queue->enqs(0)[t + 1];
            expr val1 = net_ctx.pkt2val(pkt1);
            expr meta11 = net_ctx.pkt2meta1(pkt1);
            expr meta12 = net_ctx.pkt2meta2(pkt1);

            smooth_enq_slots_vec.push_back(ite(val1, net_ctx.int_val(0), net_ctx.int_val(2)));

            expr prev_empty = net_ctx.bool_val(true);
            for (int t2 = (int) t; t2 >= 0; t2--) {
                expr pkt2 = queue->enqs(0)[t2];
                expr val2 = net_ctx.pkt2val(pkt2);
                expr meta21 = net_ctx.pkt2meta1(pkt2);
                expr meta22 = net_ctx.pkt2meta2(pkt2);

                smooth_enq_slots_vec.push_back(
                    ite(val1 && prev_empty && val2 &&
                            (meta11 != meta21 || meta12 != meta22),
                        net_ctx.int_val(1),
                        net_ctx.int_val(0)));
                prev_empty = prev_empty && !val2;
            }
        }

        expr pkt0 = queue->enqs(0)[0];
        expr val0 = net_ctx.pkt2val(pkt0);
        smooth_enq_slots_vec.push_back(ite(val0, net_ctx.int_val(0), net_ctx.int_val(2)));
    }

    char constr_name[100];
    expr constr_expr = sum(empty_queues_expr_vec) <= (int) max_empty_queue;
    snprintf(constr_name, sizeof(constr_name), "at_most_%u_empty_queues", max_empty_queue);
    z3_optimizer->add(constr_expr, constr_name);

    if (order == WitnessObjectiveOrder::MinSimultaneousEnqThenSparse) {
        z3_optimizer->minimize(sum(simultaneous_enq_cost_vec));
        z3_optimizer->maximize(sum(empty_queues_expr_vec));
        z3_optimizer->minimize(sum(smooth_enq_slots_vec));
        z3_optimizer->minimize(sum(per_queue_enq_sum));
    } else {
        z3_optimizer->maximize(sum(empty_queues_expr_vec));
    }
    if (order == WitnessObjectiveOrder::TrafficBeforeSmoothness) {
        z3_optimizer->minimize(sum(per_queue_enq_sum));
        z3_optimizer->minimize(sum(smooth_enq_slots_vec));
    } else if (order == WitnessObjectiveOrder::SmoothBeforeTraffic) {
        z3_optimizer->minimize(sum(smooth_enq_slots_vec));
        z3_optimizer->minimize(sum(per_queue_enq_sum));
    }

    check_result z3_res = z3_optimizer->check();
    solver_res_t res = get_solver_res_t(z3_res);
    if (z3_res == sat) {
        model m = z3_optimizer->get_model();
        model_str = get_model_str(m);

        if (eg != nullptr) {
            populate_sb_indexed_example_from_model(m, eg);
        }

        for (unsigned int q = 0; q < in_queues.size(); q++) {
            expr total_enqs = m.eval(per_queue_enq_sum[q], true);
            bool is_zero_queue = total_enqs.is_numeral() && total_enqs.get_numeral_uint() == 0;
            if (is_zero_queue) {
                if (zero_queues != nullptr) zero_queues->insert(q);
            } else {
                if (target_queues != nullptr) target_queues->insert(q);
            }
        }
    }

    z3_optimizer->pop();
    return res;
}

void SharedBufferPoint::populate_sb_indexed_example_from_model(model& m, SBIndexedExample* eg) {
    if (eg == nullptr) return;

    Query query = get_query();
    eg->total_time = total_time;
    eg->query_qid = query.get_qid();
    eg->query_quant = query_quant_str(query.get_quant());
    eg->query_time_range = query.get_time_range();
    eg->query_lhs = query_lhs_str(query.get_lhs());
    {
        stringstream ss;
        ss << query.get_metric();
        eg->query_metric = ss.str();
    }
    {
        stringstream ss;
        ss << query.get_op();
        eg->query_op = ss.str();
    }
    eg->query_thresh = query.get_thresh();

    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        eg->input_queue_ids.push_back(queue->get_id());

        vector<unsigned int> enqs;
        vector<unsigned int> deqs;
        vector<unsigned int> demands;
        vector<vector<int>> enqs_meta1;
        vector<vector<int>> enqs_meta2;

        Metric* demand_metric = queue->get_metric(metric_t::DEMAND);
        for (unsigned int t = 0; t < total_time; t++) {
            enqs.push_back(eval_uint(m, queue->enq_cnt(t)));
            deqs.push_back(eval_uint(m, queue->deq_cnt(t)));

            if (demand_metric != nullptr) {
                demands.push_back(eval_uint(m, demand_metric->val(t).second));
            } else {
                demands.push_back(enqs.back());
            }

            vector<int> meta1;
            vector<int> meta2;
            for (unsigned int p = 0; p < queue->max_enq(); p++) {
                expr pkt = queue->enqs(p)[t];
                if (!eval_bool(m, net_ctx.pkt2val(pkt))) continue;
                meta1.push_back(eval_int(m, net_ctx.pkt2meta1(pkt), 1000));
                meta2.push_back(eval_int(m, net_ctx.pkt2meta2(pkt), 1000));
            }

            enqs_meta1.push_back(meta1);
            enqs_meta2.push_back(meta2);
        }

        eg->enqs.push_back(enqs);
        eg->deqs.push_back(deqs);
        eg->demands.push_back(demands);
        eg->enqs_meta1.push_back(enqs_meta1);
        eg->enqs_meta2.push_back(enqs_meta2);
    }

    for (const cid_t& node_id : nodes) {
        SharedBufferSwitchQM* qm = dynamic_cast<SharedBufferSwitchQM*>(id_to_qm[node_id]);
        if (qm == nullptr) continue;

        eg->shared_buffer_switch_ids.push_back(node_id);
        vector<SharedBufferSwitchState> switch_states;
        switch_states.reserve(total_time);

        const unsigned int in_cnt = qm->in_queue_cnt();
        const unsigned int eg_cnt = qm->out_queue_cnt();
        const unsigned int slot_cnt = qm->buffer_slot_cnt();
        const unsigned int micro_steps = qm->micro_step_cnt();
        const unsigned int k_states = micro_steps + 1;

        for (unsigned int t = 0; t < total_time; t++) {
            SharedBufferSwitchState switch_state;
            switch_state.ingress.reserve(in_cnt);
            switch_state.egress.reserve(eg_cnt);
            switch_state.microstates.reserve(k_states);

            for (unsigned int q = 0; q < in_cnt; q++) {
                SBQueue* inq = dynamic_cast<SBQueue*>(qm->get_in_queue(q));
                if (inq == nullptr) {
                    throw runtime_error(
                        "SharedBufferPoint::populate_sb_indexed_example_from_model: ingress queue is not SBQueue");
                }

                SharedBufferIngressState ingress_state;
                ingress_state.ingress_paused = eval_bool(m, qm->ingress_paused(q, t));
                ingress_state.queue_ctrl.vis_pkt =
                    eval_packet_snapshot(m, net_ctx, inq->elem(0)[t]);
                ingress_state.queue_ctrl.pause_tx = eval_bool(m, inq->pause_tx(t));
                ingress_state.queue_ctrl.resume_tx = eval_bool(m, inq->resume_tx(t));
                ingress_state.queue_ctrl.pause_rx = eval_bool(m, inq->pause_rx(t));
                ingress_state.queue_ctrl.resume_rx = eval_bool(m, inq->resume_rx(t));
                ingress_state.queue_ctrl.sender_paused = eval_bool(m, inq->sender_paused(t));
                switch_state.ingress.push_back(ingress_state);
            }

            for (unsigned int e = 0; e < eg_cnt; e++) {
                SBQueue* outq = dynamic_cast<SBQueue*>(qm->get_out_queue(e));
                if (outq == nullptr) {
                    throw runtime_error(
                        "SharedBufferPoint::populate_sb_indexed_example_from_model: egress queue is not SBQueue");
                }

                SharedBufferEgressState egress_state;
                egress_state.sender_paused = eval_bool(m, outq->sender_paused(t));
                egress_state.can_emit = can_emit_now(m, net_ctx, qm, e, t);
                egress_state.has_pkt = eval_bool(m, qm->egress_has_pkt(e, t));
                egress_state.blocked = eval_bool(m, qm->egress_blocked(e, t));
                egress_state.min_stamp = eval_int(m, qm->egress_min_stamp(e, t), -1);
                switch_state.egress.push_back(egress_state);
            }

            for (unsigned int k = 0; k < k_states; k++) {
                SharedBufferMicroState micro_state;
                micro_state.active_q = (k < micro_steps) ? qm->ingress_queue_for_step(k) : -1;
                micro_state.ingress_pool_total = eval_int(m, qm->ingress_pool_total_occ(t, k));
                micro_state.headroom_pool_total = eval_int(m, qm->headroom_pool_total_occ(t, k));
                micro_state.egress_lossless_pool_total =
                    eval_int(m, qm->egress_lossless_pool_total_occ(t, k));

                micro_state.ingress_pool_occ.reserve(in_cnt);
                micro_state.headroom_pool_occ.reserve(in_cnt);
                for (unsigned int q = 0; q < in_cnt; q++) {
                    micro_state.ingress_pool_occ.push_back(
                        eval_int(m, qm->ingress_pool_occ(q, t, k)));
                    micro_state.headroom_pool_occ.push_back(
                        eval_int(m, qm->headroom_pool_occ(q, t, k)));
                    if (k < micro_steps) {
                        micro_state.admit_in.push_back(eval_bool(m, qm->admit_in(q, t, k)));
                        micro_state.admit.push_back(eval_bool(m, qm->admit(q, t, k)));
                        micro_state.alloc_in.push_back(eval_bool(m, qm->alloc_in(q, t, k)));
                        micro_state.alloc_hdr.push_back(eval_bool(m, qm->alloc_hdr(q, t, k)));
                    }
                }

                micro_state.egress_lossless_pool_occ.reserve(eg_cnt);
                for (unsigned int e = 0; e < eg_cnt; e++) {
                    micro_state.egress_lossless_pool_occ.push_back(
                        eval_int(m, qm->egress_lossless_pool_occ(e, t, k)));
                    if (k < micro_steps) {
                        micro_state.admit_e.push_back(eval_bool(m, qm->admit_e(e, t, k)));
                        micro_state.alloc_eg_lossless.push_back(
                            eval_bool(m, qm->alloc_eg_lossless(e, t, k)));
                    }
                }

                micro_state.slots.reserve(slot_cnt);
                for (unsigned int b = 0; b < slot_cnt; b++) {
                    SharedBufferSlotState slot_state;
                    slot_state.pkt = eval_packet_snapshot(m, net_ctx, qm->slot_pkt(b, t, k));
                    slot_state.ingress = eval_int(m, qm->slot_ingress(b, t, k), -1);
                    slot_state.egress = eval_int(m, qm->slot_egress(b, t, k), -1);
                    slot_state.stamp = eval_int(m, qm->slot_stamp(b, t, k), -1);
                    if (k < micro_steps) {
                        slot_state.read = eval_bool(m, qm->read_slot(b, t, k));
                        slot_state.write = eval_bool(m, qm->write_slot(b, t, k));
                        slot_state.write_q = slot_state.write ? micro_state.active_q : -1;
                    }
                    micro_state.slots.push_back(slot_state);
                }

                switch_state.microstates.push_back(micro_state);
            }

            switch_states.push_back(switch_state);
        }

        eg->shared_buffer_states.push_back(switch_states);
    }
}

void SharedBufferPoint::add_in_queue_constrs() {
    ContentionPoint::add_in_queue_constrs();

    map<string, expr> new_constr_map;
    char constr_name[160];

    for (unsigned int q = 0; q < in_queues.size(); q++) {
        SBQueue* inq = dynamic_cast<SBQueue*>(in_queues[q]);
        if (inq == nullptr) {
            throw runtime_error("SharedBufferPoint boundary ingress queue is not an SBQueue");
        }

        Metric* demand_metric = inq->get_metric(metric_t::DEMAND);
        if (demand_metric == nullptr) continue;

        unsigned int demand_ub = env_info.max_demand_per_t;
        if (inq->max_enq() < demand_ub) demand_ub = inq->max_enq();

        for (unsigned int t = 0; t < total_time; t++) {
            expr demand_t = demand_metric->val(t).second;

            snprintf(constr_name, sizeof(constr_name), "sb_boundary_demand_lb_q%u_t%u", q, t);
            // Per-slot external demand must be non-negative.
            new_constr_map.insert(named_constr(constr_name, demand_t >= 0));

            snprintf(constr_name, sizeof(constr_name), "sb_boundary_demand_ub_q%u_t%u", q, t);
            // Per-slot external demand is capped by configured max_demand_per_t and queue max_enq.
            new_constr_map.insert(named_constr(constr_name, demand_t <= (int) demand_ub));
        }
    }

    add_constr_from_map(new_constr_map);
}

void SharedBufferPoint::add_out_queue_constrs() {
    map<string, expr> new_constr_map;
    char constr_name[160];

    if (env_info.boundary_egress_ctrl_default_false) {
        for (unsigned int e = 0; e < out_queues.size(); e++) {
            SBQueue* outq = dynamic_cast<SBQueue*>(out_queues[e]);
            if (outq == nullptr) {
                throw runtime_error("SharedBufferPoint boundary egress queue is not an SBQueue");
            }

            for (unsigned int t = 0; t < total_time; t++) {
                snprintf(constr_name, sizeof(constr_name), "sb_boundary_pause_false_e%u_t%u", e, t);
                // Boundary environment is configured quiescent, so no downstream XOFF is injected
                // on this boundary egress queue.
                new_constr_map.insert(named_constr(constr_name, !outq->pause_tx(t)));

                snprintf(
                    constr_name, sizeof(constr_name), "sb_boundary_resume_false_e%u_t%u", e, t);
                // Boundary environment is configured quiescent, so no downstream XON is injected on
                // this boundary egress queue.
                new_constr_map.insert(named_constr(constr_name, !outq->resume_tx(t)));
            }
        }
    }

    add_constr_from_map(new_constr_map);
}

m_val_expr_t SharedBufferPoint::metric_val_expr(metric_t metric, unsigned int q, unsigned t) {
    return ContentionPoint::metric_val_expr(metric, q, t);
}
