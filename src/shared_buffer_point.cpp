//  shared_buffer_point.cpp
//  FSB
//
//  Created by Wenkai Li on 2/27/26.
//  Copyright © 2026 Wenkai Li. All rights reserved.
//

#include "shared_buffer_point.hpp"

#include <cstdio>

SharedBufferPoint::SharedBufferPoint(unsigned int total_time, SharedBufferPointInfo info):
ContentionPoint(total_time), sb_info(info) {
}

unsigned int SharedBufferPoint::micro_step_cnt() const {
    return (unsigned int) in_queues.size() + 1;
}

unsigned int SharedBufferPoint::deq_step_idx() const {
    if (sb_info.timing_mode == SharedBufferPointInfo::TimingMode::DEQ_FIRST) return 0;
    return (unsigned int) in_queues.size();
}

int SharedBufferPoint::ingress_queue_for_step(unsigned int k) const {
    const unsigned int in_cnt = (unsigned int) in_queues.size();
    if (sb_info.timing_mode == SharedBufferPointInfo::TimingMode::DEQ_FIRST) {
        if (k == 0 || k > in_cnt) return -1;
        return (int) (k - 1);
    }

    if (k >= in_cnt) return -1;
    return (int) k;
}

unsigned int SharedBufferPoint::ingress_pool_cap() const {
    if (sb_info.ingress_pool_size > 0) return sb_info.ingress_pool_size;
    return sb_info.buffer_slot_cnt;
}

unsigned int SharedBufferPoint::headroom_pool_cap() const {
    if (sb_info.headroom_pool_size > 0) return sb_info.headroom_pool_size;
    return sb_info.buffer_slot_cnt;
}

unsigned int SharedBufferPoint::egress_lossless_pool_cap() const {
    if (sb_info.egress_lossless_pool_size > 0) return sb_info.egress_lossless_pool_size;
    return sb_info.buffer_slot_cnt;
}

void SharedBufferPoint::init_shared_vars() {
    const unsigned int in_cnt = (unsigned int) in_queues.size();
    const unsigned int eg_cnt = (unsigned int) out_queues.size();
    const unsigned int slot_cnt = sb_info.buffer_slot_cnt;
    const unsigned int micro_steps = micro_step_cnt();
    const unsigned int k_states = micro_steps + 1;

    admit_in_.clear();
    admit_e_.clear();
    admit_.clear();
    // drop_or_block_.clear();
    alloc_in_.clear();
    release_in_.clear();
    alloc_hdr_.clear();
    release_hdr_.clear();
    alloc_eg_lossless_.clear();
    release_e_.clear();

    buf_pkt_.clear();
    buf_in_.clear();
    buf_e_.clear();
    buf_stamp_.clear();
    paused_.clear();
    ingress_pool_occ_.clear();
    ingress_pool_total_occ_.clear();
    headroom_pool_occ_.clear();
    headroom_pool_total_occ_.clear();
    egress_lossless_pool_occ_.clear();
    egress_lossless_pool_total_occ_.clear();

    write_slot_.clear();
    read_slot_.clear();
    min_stamp_e_.clear();
    has_pkt_e_.clear();

    admit_in_.resize(in_cnt);
    admit_e_.resize(eg_cnt);
    admit_.resize(in_cnt);
    // drop_or_block_.resize(in_cnt);
    alloc_in_.resize(in_cnt);
    release_in_.resize(in_cnt);
    alloc_hdr_.resize(in_cnt);
    release_hdr_.resize(in_cnt);
    alloc_eg_lossless_.resize(eg_cnt);
    release_e_.resize(eg_cnt);

    buf_pkt_.resize(slot_cnt);
    buf_in_.resize(slot_cnt);
    buf_e_.resize(slot_cnt);
    buf_stamp_.resize(slot_cnt);
    paused_.resize(in_cnt);
    ingress_pool_occ_.resize(in_cnt);
    headroom_pool_occ_.resize(in_cnt);
    egress_lossless_pool_occ_.resize(eg_cnt);

    write_slot_.resize(in_cnt);
    read_slot_.resize(eg_cnt);
    min_stamp_e_.resize(eg_cnt);
    has_pkt_e_.resize(eg_cnt);
    ingress_pool_total_occ_.resize(total_time);
    headroom_pool_total_occ_.resize(total_time);
    egress_lossless_pool_total_occ_.resize(total_time);

    char vname[120];

    for (unsigned int b = 0; b < slot_cnt; b++) {
        buf_pkt_[b].resize(total_time);
        buf_in_[b].resize(total_time);
        buf_e_[b].resize(total_time);
        buf_stamp_[b].resize(total_time);
        for (unsigned int t = 0; t < total_time; t++) {
            for (unsigned int k = 0; k < k_states; k++) {
                snprintf(vname, sizeof(vname), "sb_buf_pkt[%u][%u][%u]", b, t, k);
                buf_pkt_[b][t].push_back(net_ctx.pkt_const(vname));
                snprintf(vname, sizeof(vname), "sb_buf_in[%u][%u][%u]", b, t, k);
                buf_in_[b][t].push_back(net_ctx.int_const(vname));
                snprintf(vname, sizeof(vname), "sb_buf_e[%u][%u][%u]", b, t, k);
                buf_e_[b][t].push_back(net_ctx.int_const(vname));
                snprintf(vname, sizeof(vname), "sb_buf_stamp[%u][%u][%u]", b, t, k);
                buf_stamp_[b][t].push_back(net_ctx.int_const(vname));
            }
        }
    }

    for (unsigned int q = 0; q < in_cnt; q++) {
        write_slot_[q].resize(slot_cnt);
        admit_in_[q].resize(total_time);
        admit_[q].resize(total_time);
        // drop_or_block_[q].resize(total_time);
        alloc_in_[q].resize(total_time);
        release_in_[q].resize(total_time);
        alloc_hdr_[q].resize(total_time);
        release_hdr_[q].resize(total_time);
        paused_[q].reserve(total_time);
        ingress_pool_occ_[q].resize(total_time);
        headroom_pool_occ_[q].resize(total_time);

        for (unsigned int t = 0; t < total_time; t++) {
            snprintf(vname, sizeof(vname), "sb_paused[%u][%u]", q, t);
            paused_[q].push_back(net_ctx.bool_const(vname));

            for (unsigned int k = 0; k < micro_steps; k++) {
                snprintf(vname, sizeof(vname), "sb_admit_in[%u][%u][%u]", q, t, k);
                admit_in_[q][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "sb_admit[%u][%u][%u]", q, t, k);
                admit_[q][t].push_back(net_ctx.bool_const(vname));
                // snprintf(vname, sizeof(vname), "sb_drop_or_block[%u][%u][%u]", q, t, k);
                // drop_or_block_[q][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "sb_alloc_in[%u][%u][%u]", q, t, k);
                alloc_in_[q][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "sb_release_in[%u][%u][%u]", q, t, k);
                release_in_[q][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "sb_alloc_hdr[%u][%u][%u]", q, t, k);
                alloc_hdr_[q][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "sb_release_hdr[%u][%u][%u]", q, t, k);
                release_hdr_[q][t].push_back(net_ctx.bool_const(vname));
            }

            for (unsigned int k = 0; k < k_states; k++) {
                snprintf(vname, sizeof(vname), "sb_ingress_pool_occ[%u][%u][%u]", q, t, k);
                ingress_pool_occ_[q][t].push_back(net_ctx.int_const(vname));
                snprintf(vname, sizeof(vname), "sb_headroom_pool_occ[%u][%u][%u]", q, t, k);
                headroom_pool_occ_[q][t].push_back(net_ctx.int_const(vname));
            }
        }

        for (unsigned int b = 0; b < slot_cnt; b++) {
            write_slot_[q][b].resize(total_time);
            for (unsigned int t = 0; t < total_time; t++) {
                for (unsigned int k = 0; k < micro_steps; k++) {
                    snprintf(vname, sizeof(vname), "sb_write_slot[%u][%u][%u][%u]", q, b, t, k);
                    write_slot_[q][b][t].push_back(net_ctx.bool_const(vname));
                }
            }
        }
    }

    for (unsigned int t = 0; t < total_time; t++) {
        for (unsigned int k = 0; k < k_states; k++) {
            snprintf(vname, sizeof(vname), "sb_ingress_pool_total_occ[%u][%u]", t, k);
            ingress_pool_total_occ_[t].push_back(net_ctx.int_const(vname));
            snprintf(vname, sizeof(vname), "sb_headroom_pool_total_occ[%u][%u]", t, k);
            headroom_pool_total_occ_[t].push_back(net_ctx.int_const(vname));
            snprintf(vname, sizeof(vname), "sb_egress_lossless_pool_total_occ[%u][%u]", t, k);
            egress_lossless_pool_total_occ_[t].push_back(net_ctx.int_const(vname));
        }
    }

    for (unsigned int e = 0; e < eg_cnt; e++) {
        read_slot_[e].resize(slot_cnt);
        admit_e_[e].resize(total_time);
        alloc_eg_lossless_[e].resize(total_time);
        release_e_[e].resize(total_time);
        egress_lossless_pool_occ_[e].resize(total_time);

        for (unsigned int t = 0; t < total_time; t++) {
            snprintf(vname, sizeof(vname), "sb_min_stamp_e[%u][%u]", e, t);
            min_stamp_e_[e].push_back(net_ctx.int_const(vname));
            snprintf(vname, sizeof(vname), "sb_has_pkt_e[%u][%u]", e, t);
            has_pkt_e_[e].push_back(net_ctx.bool_const(vname));

            for (unsigned int k = 0; k < micro_steps; k++) {
                snprintf(vname, sizeof(vname), "sb_admit_e[%u][%u][%u]", e, t, k);
                admit_e_[e][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "sb_alloc_eg_lossless[%u][%u][%u]", e, t, k);
                alloc_eg_lossless_[e][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "sb_release_e[%u][%u][%u]", e, t, k);
                release_e_[e][t].push_back(net_ctx.bool_const(vname));
            }

            for (unsigned int k = 0; k < k_states; k++) {
                snprintf(vname, sizeof(vname), "sb_egress_lossless_pool_occ[%u][%u][%u]", e, t, k);
                egress_lossless_pool_occ_[e][t].push_back(net_ctx.int_const(vname));
            }
        }

        for (unsigned int b = 0; b < slot_cnt; b++) {
            read_slot_[e][b].resize(total_time);
            for (unsigned int t = 0; t < total_time; t++) {
                for (unsigned int k = 0; k < micro_steps; k++) {
                    snprintf(vname, sizeof(vname), "sb_read_slot[%u][%u][%u][%u]", e, b, t, k);
                    read_slot_[e][b][t].push_back(net_ctx.bool_const(vname));
                }
            }
        }
    }

    shared_vars_initialized_ = true;
}

void SharedBufferPoint::add_micro_step_constrs(map<string, expr>& new_constr_map) {
    const unsigned int in_cnt = (unsigned int) in_queues.size();
    const unsigned int eg_cnt = (unsigned int) out_queues.size();
    const unsigned int slot_cnt = sb_info.buffer_slot_cnt;
    const unsigned int ingress_pool_cap_cnt = ingress_pool_cap();
    const unsigned int headroom_pool_cap_cnt = headroom_pool_cap();
    const unsigned int egress_lossless_pool_cap_cnt = egress_lossless_pool_cap();
    const unsigned int micro_steps = micro_step_cnt();
    const unsigned int k_states = micro_steps + 1;
    const unsigned int deq_k = deq_step_idx();

    const expr int0 = net_ctx.int_val(0);
    const expr int1 = net_ctx.int_val(1);
    const expr bool0 = net_ctx.bool_val(false);

    auto b2i = [&](const expr& b) { return ite(b, int1, int0); };

    char constr_name[160];

    // Enforce micro-step role semantics: each k is either one ingress action or one global deq step.
    for (unsigned int t = 0; t < total_time; t++) {
        for (unsigned int k = 0; k < micro_steps; k++) {
            const int active_q = ingress_queue_for_step(k);
            const bool is_deq_step = (k == deq_k);

            for (unsigned int q = 0; q < in_cnt; q++) {
                if ((int) q != active_q) {
                    snprintf(constr_name, sizeof(constr_name), "sb_inactive_admit_in_q%u_t%u_k%u", q, t, k);
                    // Inactive ingress queue cannot request admission at this micro-step.
                    new_constr_map.insert(named_constr(constr_name, !admit_in_[q][t][k]));
                    snprintf(constr_name, sizeof(constr_name), "sb_inactive_admit_q%u_t%u_k%u", q, t, k);
                    // Inactive ingress queue cannot be admitted at this micro-step.
                    new_constr_map.insert(named_constr(constr_name, !admit_[q][t][k]));
                    snprintf(constr_name, sizeof(constr_name), "sb_inactive_alloc_in_q%u_t%u_k%u", q, t, k);
                    // Inactive ingress queue cannot allocate ingress pool at this micro-step.
                    new_constr_map.insert(named_constr(constr_name, !alloc_in_[q][t][k]));
                    snprintf(constr_name, sizeof(constr_name), "sb_inactive_alloc_hdr_q%u_t%u_k%u", q, t, k);
                    // Inactive ingress queue cannot allocate headroom pool at this micro-step.
                    new_constr_map.insert(named_constr(constr_name, !alloc_hdr_[q][t][k]));
                }

                if (!is_deq_step) {
                    snprintf(constr_name, sizeof(constr_name), "sb_release_in_non_deq_q%u_t%u_k%u", q, t, k);
                    // Ingress-pool release is only allowed at the dedicated deq micro-step.
                    new_constr_map.insert(named_constr(constr_name, !release_in_[q][t][k]));
                }

                if (!is_deq_step) {
                    snprintf(constr_name, sizeof(constr_name), "sb_release_hdr_non_deq_q%u_t%u_k%u", q, t, k);
                    // Headroom release is only allowed at the dedicated deq micro-step.
                    new_constr_map.insert(named_constr(constr_name, !release_hdr_[q][t][k]));
                }

                for (unsigned int b = 0; b < slot_cnt; b++) {
                    if (is_deq_step || (int) q != active_q) {
                        snprintf(constr_name,
                                 sizeof(constr_name),
                                 "sb_write_inactive_q%u_b%u_t%u_k%u",
                                 q,
                                 b,
                                 t,
                                 k);
                        // Only the active ingress queue in an ingress micro-step may write a slot.
                        new_constr_map.insert(named_constr(constr_name, !write_slot_[q][b][t][k]));
                    }
                }
            }

            for (unsigned int e = 0; e < eg_cnt; e++) {
                if (is_deq_step) {
                    snprintf(constr_name, sizeof(constr_name), "sb_admit_e_deq_step_e%u_t%u_k%u", e, t, k);
                    // Egress admission gate is not used during the deq micro-step.
                    new_constr_map.insert(named_constr(constr_name, !admit_e_[e][t][k]));
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_alloc_eg_non_ingress_e%u_t%u_k%u",
                             e,
                             t,
                             k);
                    // Egress lossless allocation is only defined in ingress micro-steps.
                    new_constr_map.insert(named_constr(constr_name, !alloc_eg_lossless_[e][t][k]));
                } else {
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_release_e_non_deq_e%u_t%u_k%u",
                             e,
                             t,
                             k);
                    // Egress release is only allowed at the dedicated deq micro-step.
                    new_constr_map.insert(named_constr(constr_name, !release_e_[e][t][k]));

                    for (unsigned int b = 0; b < slot_cnt; b++) {
                        snprintf(constr_name,
                                 sizeof(constr_name),
                                 "sb_read_non_deq_e%u_b%u_t%u_k%u",
                                 e,
                                 b,
                                 t,
                                 k);
                        // Slot reads are disallowed outside of the dedicated deq micro-step.
                        new_constr_map.insert(named_constr(constr_name, !read_slot_[e][b][t][k]));
                    }
                }
            }

            if (active_q >= 0) {
                const unsigned int q = (unsigned int) active_q;
                Queue* inq = in_queues[q];
                expr pkt = inq->enqs(0)[t];
                expr has_enq = net_ctx.pkt2val(pkt);
                expr dst = net_ctx.pkt2meta1(pkt);
                expr dst_eg = dst % net_ctx.int_val((int) eg_cnt);

                Metric* demand_metric = inq->get_metric(metric_t::DEMAND);
                if (demand_metric != nullptr) {
                    expr demand_t = demand_metric->val(t).second;
                    if (t > 0) {
                        demand_t = demand_t - demand_metric->val(t - 1).second;
                    }
                    expr has_demand = demand_t > 0;

                    unsigned int demand_ub = sb_info.max_demand_per_t;
                    if (inq->max_enq() < demand_ub) demand_ub = inq->max_enq();

                    snprintf(constr_name, sizeof(constr_name), "sb_demand_lb_q%u_t%u_k%u", q, t, k);
                    // Per-slot demand must be non-negative.
                    new_constr_map.insert(named_constr(constr_name, demand_t >= 0));
                    snprintf(constr_name, sizeof(constr_name), "sb_demand_ub_q%u_t%u_k%u", q, t, k);
                    // Per-slot demand is capped by configured max_demand_per_t and queue max_enq.
                    new_constr_map.insert(
                        named_constr(constr_name, demand_t <= (int) demand_ub));

                    snprintf(constr_name, sizeof(constr_name), "sb_has_enq_from_demand_q%u_t%u_k%u", q, t, k);
                    // Actual packet arrival requires both intent and successful admission.
                    new_constr_map.insert(
                        named_constr(constr_name, has_enq == (has_demand && admit_[q][t][k])));
                    snprintf(constr_name, sizeof(constr_name), "sb_admit_in_from_demand_q%u_t%u_k%u", q, t, k);
                    // Ingress-side request follows intent; egress/paused logic decides final admit.
                    new_constr_map.insert(
                        named_constr(constr_name, admit_in_[q][t][k] == has_demand));
                }

                expr_vector route_cases(net_ctx.z3_ctx());
                for (unsigned int e = 0; e < eg_cnt; e++) {
                    expr route_to_e = dst_eg == net_ctx.int_val((int) e);
                    expr admit_def = admit_[q][t][k] == (admit_in_[q][t][k] && admit_e_[e][t][k]);
                    route_cases.push_back(implies(has_enq && route_to_e, admit_def));

                    snprintf(constr_name, sizeof(constr_name), "sb_admit_e_non_route_false_q%u_e%u_t%u_k%u", q, e, t, k);
                    // In an ingress micro-step, egress gate e is forced false when the packet is empty
                    // or when e is not the routed destination for this packet.
                    new_constr_map.insert(named_constr(
                        constr_name,
                        implies(!has_enq || !route_to_e, !admit_e_[e][t][k])));
                }
                snprintf(constr_name, sizeof(constr_name), "sb_admit_route_q%u_t%u_k%u", q, t, k);
                // Admission result follows the routed egress gate selected by packet destination.
                new_constr_map.insert(named_constr(constr_name, mk_and(route_cases)));

                expr queue_paused = paused_[q][t];

                snprintf(constr_name, sizeof(constr_name), "sb_alloc_in_by_threshold_q%u_t%u_k%u", q, t, k);
                // If queue q is not paused at this step, admitted packets are accounted in ingress pool.
                new_constr_map.insert(
                    named_constr(constr_name, alloc_in_[q][t][k] == (admit_[q][t][k] && !queue_paused)));
                snprintf(constr_name, sizeof(constr_name), "sb_alloc_hdr_by_paused_q%u_t%u_k%u", q, t, k);
                // If queue q is paused at this step, admitted packets are accounted in headroom pool.
                new_constr_map.insert(
                    named_constr(constr_name, alloc_hdr_[q][t][k] == (admit_[q][t][k] && queue_paused)));
                snprintf(constr_name, sizeof(constr_name), "sb_alloc_in_requires_pool_credit_q%u_t%u_k%u", q, t, k);
                // Ingress accounting requires available ingress-pool capacity.
                new_constr_map.insert(named_constr(
                    constr_name,
                    implies(alloc_in_[q][t][k], ingress_pool_total_occ_[t][k] < (int) ingress_pool_cap_cnt)));
                snprintf(constr_name, sizeof(constr_name), "sb_alloc_hdr_requires_pool_credit_q%u_t%u_k%u", q, t, k);
                // Headroom accounting requires available per-queue headroom capacity.
                new_constr_map.insert(named_constr(
                    constr_name,
                    implies(alloc_hdr_[q][t][k], headroom_pool_occ_[q][t][k] < (int) headroom_pool_cap_cnt)));
                snprintf(constr_name, sizeof(constr_name), "sb_alloc_cover_admit_q%u_t%u_k%u", q, t, k);
                // Every admitted packet must be accounted by exactly one allocation path.
                new_constr_map.insert(
                    named_constr(constr_name, admit_[q][t][k] == (alloc_in_[q][t][k] || alloc_hdr_[q][t][k])));

                // Empty enq guards
                snprintf(constr_name, sizeof(constr_name), "sb_noop_on_empty_admit_q%u_t%u_k%u", q, t, k);
                // Empty input packet means no final admission.
                new_constr_map.insert(named_constr(constr_name, implies(!has_enq, !admit_[q][t][k])));
                snprintf(constr_name, sizeof(constr_name), "sb_noop_on_empty_alloc_in_q%u_t%u_k%u", q, t, k);
                // Empty input packet cannot allocate ingress pool.
                new_constr_map.insert(named_constr(constr_name, implies(!has_enq, !alloc_in_[q][t][k])));
                snprintf(constr_name, sizeof(constr_name), "sb_noop_on_empty_alloc_hdr_q%u_t%u_k%u", q, t, k);
                // Empty input packet cannot allocate headroom pool.
                new_constr_map.insert(named_constr(constr_name, implies(!has_enq, !alloc_hdr_[q][t][k])));

                for (unsigned int e = 0; e < eg_cnt; e++) {
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_alloc_eg_route_e%u_q%u_t%u_k%u",
                             e,
                             q,
                             t,
                             k);
                    // Egress lossless allocation is charged to the routed egress on successful admit.
                    new_constr_map.insert(named_constr(
                        constr_name,
                        alloc_eg_lossless_[e][t][k] ==
                            (admit_[q][t][k] && (dst_eg == net_ctx.int_val((int) e)))));

                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_noop_on_empty_alloc_eg_e%u_q%u_t%u_k%u",
                             e,
                             q,
                             t,
                             k);
                    // Empty input packet cannot allocate any egress lossless pool.
                    new_constr_map.insert(named_constr(
                        constr_name,
                        implies(!has_enq, !alloc_eg_lossless_[e][t][k])));
                }

                expr writes_sum = int0;
                expr any_empty = bool0;
                for (unsigned int b = 0; b < slot_cnt; b++) {
                    expr slot_empty = !net_ctx.pkt2val(buf_pkt_[b][t][k]);
                    any_empty = any_empty || slot_empty;
                    writes_sum = writes_sum + b2i(write_slot_[q][b][t][k]);

                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_write_implies_admit_q%u_b%u_t%u_k%u",
                             q,
                             b,
                             t,
                             k);
                    // Any write action must correspond to a successful admission.
                    new_constr_map.insert(
                        named_constr(constr_name, implies(write_slot_[q][b][t][k], admit_[q][t][k])));

                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_write_requires_empty_q%u_b%u_t%u_k%u",
                             q,
                             b,
                             t,
                             k);
                    // Writes can only target currently empty slots.
                    new_constr_map.insert(
                        named_constr(constr_name,
                                     implies(write_slot_[q][b][t][k], slot_empty)));

                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_noop_on_empty_write_q%u_b%u_t%u_k%u",
                             q,
                             b,
                             t,
                             k);
                    // Empty input packet cannot write to any slot.
                    new_constr_map.insert(named_constr(
                        constr_name,
                        implies(!has_enq, !write_slot_[q][b][t][k])));
                }

                snprintf(constr_name, sizeof(constr_name), "sb_admit_write_eq_q%u_t%u_k%u", q, t, k);
                // Exactly one slot is written iff admission succeeds; otherwise no slot is written.
                new_constr_map.insert(named_constr(
                    constr_name,
                    writes_sum == ite(admit_[q][t][k], int1, int0)));
                snprintf(constr_name, sizeof(constr_name), "sb_admit_requires_empty_q%u_t%u_k%u", q, t, k);
                // Admission is only possible when at least one shared-buffer slot is currently empty.
                new_constr_map.insert(
                    named_constr(constr_name, implies(admit_[q][t][k], any_empty)));
            }

            for (unsigned int b = 0; b < slot_cnt; b++) {
                expr slot_writers_sum = int0;
                expr slot_readers_sum = int0;

                for (unsigned int q = 0; q < in_cnt; q++) {
                    slot_writers_sum = slot_writers_sum + b2i(write_slot_[q][b][t][k]);
                }
                for (unsigned int e = 0; e < eg_cnt; e++) {
                    slot_readers_sum = slot_readers_sum + b2i(read_slot_[e][b][t][k]);
                }

                snprintf(constr_name, sizeof(constr_name), "sb_single_writer_b%u_t%u_k%u", b, t, k);
                // A slot has at most one writer in each micro-step.
                new_constr_map.insert(named_constr(constr_name, slot_writers_sum <= 1));
                snprintf(constr_name, sizeof(constr_name), "sb_single_reader_b%u_t%u_k%u", b, t, k);
                // A slot has at most one reader in each micro-step.
                new_constr_map.insert(named_constr(constr_name, slot_readers_sum <= 1));
                snprintf(constr_name, sizeof(constr_name), "sb_no_rw_conflict_b%u_t%u_k%u", b, t, k);
                // Read and write on the same slot cannot happen simultaneously in one micro-step.
                new_constr_map.insert(named_constr(constr_name, slot_writers_sum + slot_readers_sum <= 1));
            }
        }
    }

    // Slot identity consistency for each state snapshot [t][k].
    for (unsigned int t = 0; t < total_time; t++) {
        for (unsigned int k = 0; k < k_states; k++) {
            for (unsigned int b = 0; b < slot_cnt; b++) {
                expr used = net_ctx.pkt2val(buf_pkt_[b][t][k]);

                snprintf(constr_name, sizeof(constr_name), "sb_buf_empty_pkt_null_b%u_t%u_k%u", b, t, k);
                // Empty slots carry null packet identity.
                new_constr_map.insert(
                    named_constr(constr_name, implies(!used, buf_pkt_[b][t][k] == net_ctx.null_pkt())));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_empty_ing_neg1_b%u_t%u_k%u", b, t, k);
                // Empty slots reset ingress owner metadata to -1 (no owner).
                new_constr_map.insert(
                    named_constr(constr_name, implies(!used, buf_in_[b][t][k] == -1)));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_empty_eg_neg1_b%u_t%u_k%u", b, t, k);
                // Empty slots reset egress owner metadata to -1 (no owner).
                new_constr_map.insert(
                    named_constr(constr_name, implies(!used, buf_e_[b][t][k] == -1)));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_empty_stamp_neg1_b%u_t%u_k%u", b, t, k);
                // Empty slots reset timestamp to -1.
                new_constr_map.insert(
                    named_constr(constr_name, implies(!used, buf_stamp_[b][t][k] == -1)));

                snprintf(constr_name, sizeof(constr_name), "sb_buf_occ_ing_lb_b%u_t%u_k%u", b, t, k);
                // Occupied slots must carry a valid ingress owner index.
                new_constr_map.insert(
                    named_constr(constr_name, implies(used, buf_in_[b][t][k] >= 0)));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_occ_ing_ub_b%u_t%u_k%u", b, t, k);
                // Occupied slots must carry a valid ingress owner index.
                new_constr_map.insert(
                    named_constr(constr_name, implies(used, buf_in_[b][t][k] < (int) in_cnt)));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_occ_eg_lb_b%u_t%u_k%u", b, t, k);
                // Occupied slots must carry a valid egress owner index.
                new_constr_map.insert(
                    named_constr(constr_name, implies(used, buf_e_[b][t][k] >= 0)));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_occ_eg_ub_b%u_t%u_k%u", b, t, k);
                // Occupied slots must carry a valid egress owner index.
                new_constr_map.insert(
                    named_constr(constr_name, implies(used, buf_e_[b][t][k] < (int) eg_cnt)));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_occ_stamp_nonneg_b%u_t%u_k%u", b, t, k);
                // Occupied slots must carry a non-negative enqueue timestamp.
                new_constr_map.insert(
                    named_constr(constr_name, implies(used, buf_stamp_[b][t][k] >= 0)));
            }
        }
    }

    // Pool and per-queue bounds for each state snapshot [t][k].
    for (unsigned int t = 0; t < total_time; t++) {
        for (unsigned int k = 0; k < k_states; k++) {
            expr ing_sum = int0;
            expr hdr_sum = int0;
            expr eg_sum = int0;

            for (unsigned int q = 0; q < in_cnt; q++) {
                ing_sum = ing_sum + ingress_pool_occ_[q][t][k];
                hdr_sum = hdr_sum + headroom_pool_occ_[q][t][k];

                snprintf(constr_name, sizeof(constr_name), "sb_ing_pos_lb_q%u_t%u_k%u", q, t, k);
                // Ingress pool ledger must be non-negative.
                new_constr_map.insert(named_constr(constr_name, ingress_pool_occ_[q][t][k] >= 0));
                snprintf(constr_name, sizeof(constr_name), "sb_ing_pos_ub_q%u_t%u_k%u", q, t, k);
                // Ingress pool ledger is bounded by configured ingress pool capacity.
                new_constr_map.insert(
                    named_constr(constr_name, ingress_pool_occ_[q][t][k] <= (int) ingress_pool_cap_cnt));
                snprintf(constr_name, sizeof(constr_name), "sb_hdr_pos_lb_q%u_t%u_k%u", q, t, k);
                // Headroom pool ledger must be non-negative.
                new_constr_map.insert(named_constr(constr_name, headroom_pool_occ_[q][t][k] >= 0));
                snprintf(constr_name, sizeof(constr_name), "sb_hdr_pos_ub_q%u_t%u_k%u", q, t, k);
                // Headroom pool ledger is bounded by configured headroom pool capacity.
                new_constr_map.insert(
                    named_constr(constr_name, headroom_pool_occ_[q][t][k] <= (int) headroom_pool_cap_cnt));

                if (k < micro_steps) {
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_release_in_requires_credit_q%u_t%u_k%u",
                             q,
                             t,
                             k);
                    // Ingress release requires available ingress-pool credit.
                    new_constr_map.insert(named_constr(
                        constr_name,
                        implies(release_in_[q][t][k], ingress_pool_occ_[q][t][k] > 0)));
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_release_hdr_requires_credit_q%u_t%u_k%u",
                             q,
                             t,
                             k);
                    // Headroom release requires available headroom credit.
                    new_constr_map.insert(named_constr(
                        constr_name,
                        implies(release_hdr_[q][t][k], headroom_pool_occ_[q][t][k] > 0)));
                }
            }

            for (unsigned int e = 0; e < eg_cnt; e++) {
                eg_sum = eg_sum + egress_lossless_pool_occ_[e][t][k];

                snprintf(constr_name, sizeof(constr_name), "sb_eg_pos_lb_e%u_t%u_k%u", e, t, k);
                // Egress-lossless ledger must be non-negative.
                new_constr_map.insert(named_constr(constr_name, egress_lossless_pool_occ_[e][t][k] >= 0));
                snprintf(constr_name, sizeof(constr_name), "sb_eg_pos_ub_e%u_t%u_k%u", e, t, k);
                // Egress-lossless ledger is bounded by configured egress-lossless pool capacity.
                new_constr_map.insert(
                    named_constr(constr_name,
                                 egress_lossless_pool_occ_[e][t][k] <=
                                     (int) egress_lossless_pool_cap_cnt));

                if (k < micro_steps) {
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_release_e_requires_credit_e%u_t%u_k%u",
                             e,
                             t,
                             k);
                    // Egress release requires available egress-lossless credit.
                    new_constr_map.insert(named_constr(
                        constr_name,
                        implies(release_e_[e][t][k], egress_lossless_pool_occ_[e][t][k] > 0)));
                }
            }

            snprintf(constr_name, sizeof(constr_name), "sb_ing_used_sum_t%u_k%u", t, k);
            // Aggregate ingress-pool usage equals sum of all per-queue ingress ledgers.
            new_constr_map.insert(named_constr(constr_name, ingress_pool_total_occ_[t][k] == ing_sum));
            snprintf(constr_name, sizeof(constr_name), "sb_hdr_used_sum_t%u_k%u", t, k);
            // Aggregate headroom usage equals sum of all per-queue headroom ledgers.
            new_constr_map.insert(named_constr(constr_name, headroom_pool_total_occ_[t][k] == hdr_sum));
            snprintf(constr_name, sizeof(constr_name), "sb_eg_used_sum_t%u_k%u", t, k);
            // Aggregate egress-lossless usage equals sum of all per-egress ledgers.
            new_constr_map.insert(named_constr(constr_name, egress_lossless_pool_total_occ_[t][k] == eg_sum));

            snprintf(constr_name, sizeof(constr_name), "sb_ing_used_lb_t%u_k%u", t, k);
            // Aggregate ingress usage must be non-negative.
            new_constr_map.insert(named_constr(constr_name, ingress_pool_total_occ_[t][k] >= 0));
            snprintf(constr_name, sizeof(constr_name), "sb_ing_used_ub_t%u_k%u", t, k);
            // Aggregate ingress usage is bounded by configured ingress pool capacity.
            new_constr_map.insert(
                named_constr(constr_name, ingress_pool_total_occ_[t][k] <= (int) ingress_pool_cap_cnt));
            snprintf(constr_name, sizeof(constr_name), "sb_hdr_used_lb_t%u_k%u", t, k);
            // Aggregate headroom usage must be non-negative.
            new_constr_map.insert(named_constr(constr_name, headroom_pool_total_occ_[t][k] >= 0));
            snprintf(constr_name, sizeof(constr_name), "sb_hdr_used_ub_t%u_k%u", t, k);
            // Aggregate headroom usage is bounded by configured headroom pool capacity.
            new_constr_map.insert(
                named_constr(constr_name, headroom_pool_total_occ_[t][k] <= (int) headroom_pool_cap_cnt));
            snprintf(constr_name, sizeof(constr_name), "sb_eg_used_lb_t%u_k%u", t, k);
            // Aggregate egress-lossless usage must be non-negative.
            new_constr_map.insert(named_constr(constr_name, egress_lossless_pool_total_occ_[t][k] >= 0));
            snprintf(constr_name, sizeof(constr_name), "sb_eg_used_ub_t%u_k%u", t, k);
            // Aggregate egress-lossless usage is bounded by configured egress-lossless pool capacity.
            new_constr_map.insert(
                named_constr(constr_name,
                             egress_lossless_pool_total_occ_[t][k] <=
                                 (int) egress_lossless_pool_cap_cnt));
        }
    }

    // Intra-slot state transitions: [t][k] -> [t][k+1].
    for (unsigned int t = 0; t < total_time; t++) {
        for (unsigned int k = 0; k < micro_steps; k++) {
            for (unsigned int q = 0; q < in_cnt; q++) {
                snprintf(constr_name, sizeof(constr_name), "sb_ing_pos_step_q%u_t%u_k%u", q, t, k);
                // Ingress-pool ledger transition applies alloc/release delta across one micro-step.
                new_constr_map.insert(named_constr(
                    constr_name,
                    ingress_pool_occ_[q][t][k + 1] ==
                        ingress_pool_occ_[q][t][k] + b2i(alloc_in_[q][t][k]) -
                            b2i(release_in_[q][t][k])));

                snprintf(constr_name, sizeof(constr_name), "sb_hdr_pos_step_q%u_t%u_k%u", q, t, k);
                // Headroom ledger transition applies alloc/release delta across one micro-step.
                new_constr_map.insert(named_constr(
                    constr_name,
                    headroom_pool_occ_[q][t][k + 1] ==
                        headroom_pool_occ_[q][t][k] + b2i(alloc_hdr_[q][t][k]) -
                            b2i(release_hdr_[q][t][k])));
            }

            for (unsigned int e = 0; e < eg_cnt; e++) {
                snprintf(constr_name, sizeof(constr_name), "sb_eg_pos_step_e%u_t%u_k%u", e, t, k);
                // Egress-lossless ledger transition applies alloc/release delta across one micro-step.
                new_constr_map.insert(named_constr(
                    constr_name,
                    egress_lossless_pool_occ_[e][t][k + 1] ==
                        egress_lossless_pool_occ_[e][t][k] + b2i(alloc_eg_lossless_[e][t][k]) -
                            b2i(release_e_[e][t][k])));
            }

            for (unsigned int b = 0; b < slot_cnt; b++) {
                expr any_write = bool0;
                expr any_read = bool0;
                expr write_stamp = net_ctx.int_val((int) (t * micro_steps + k));

                for (unsigned int q = 0; q < in_cnt; q++) {
                    any_write = any_write || write_slot_[q][b][t][k];
                }
                for (unsigned int e = 0; e < eg_cnt; e++) {
                    any_read = any_read || read_slot_[e][b][t][k];
                }

                for (unsigned int q = 0; q < in_cnt; q++) {
                    expr pkt = in_queues[q]->enqs(0)[t];
                    expr dst_eg = net_ctx.pkt2meta1(pkt) % net_ctx.int_val((int) eg_cnt);

                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_buf_pkt_write_bind_q%u_b%u_t%u_k%u",
                             q,
                             b,
                             t,
                             k);
                    // Writing queue q to slot b copies packet identity into the next micro-state.
                    new_constr_map.insert(named_constr(
                        constr_name,
                        implies(write_slot_[q][b][t][k], buf_pkt_[b][t][k + 1] == pkt)));

                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_buf_in_write_bind_q%u_b%u_t%u_k%u",
                             q,
                             b,
                             t,
                             k);
                    // Writing queue q to slot b records ingress owner index q.
                    new_constr_map.insert(named_constr(
                        constr_name,
                        implies(write_slot_[q][b][t][k], buf_in_[b][t][k + 1] == (int) q)));

                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_buf_e_write_bind_q%u_b%u_t%u_k%u",
                             q,
                             b,
                             t,
                             k);
                    // Writing queue q to slot b records routed egress owner index.
                    new_constr_map.insert(named_constr(
                        constr_name,
                        implies(write_slot_[q][b][t][k], buf_e_[b][t][k + 1] == dst_eg)));
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_buf_stamp_write_bind_q%u_b%u_t%u_k%u",
                             q,
                             b,
                             t,
                             k);
                    // Writing queue q to slot b records this micro-step timestamp.
                    new_constr_map.insert(named_constr(
                        constr_name,
                        implies(write_slot_[q][b][t][k], buf_stamp_[b][t][k + 1] == write_stamp)));
                }

                snprintf(constr_name, sizeof(constr_name), "sb_buf_pkt_read_clear_b%u_t%u_k%u", b, t, k);
                // Any read clears slot packet identity in the next micro-state.
                new_constr_map.insert(
                    named_constr(constr_name, implies(any_read, buf_pkt_[b][t][k + 1] == net_ctx.null_pkt())));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_in_read_clear_b%u_t%u_k%u", b, t, k);
                // Any read clears ingress owner metadata to -1 in the next micro-state.
                new_constr_map.insert(named_constr(constr_name, implies(any_read, buf_in_[b][t][k + 1] == -1)));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_e_read_clear_b%u_t%u_k%u", b, t, k);
                // Any read clears egress owner metadata to -1 in the next micro-state.
                new_constr_map.insert(named_constr(constr_name, implies(any_read, buf_e_[b][t][k + 1] == -1)));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_stamp_read_clear_b%u_t%u_k%u", b, t, k);
                // Any read clears timestamp to -1 in the next micro-state.
                new_constr_map.insert(
                    named_constr(constr_name, implies(any_read, buf_stamp_[b][t][k + 1] == -1)));

                snprintf(constr_name, sizeof(constr_name), "sb_buf_pkt_keep_b%u_t%u_k%u", b, t, k);
                // Without read/write action, packet identity carries over to the next micro-state.
                new_constr_map.insert(named_constr(
                    constr_name,
                    implies(!any_write && !any_read, buf_pkt_[b][t][k + 1] == buf_pkt_[b][t][k])));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_in_keep_b%u_t%u_k%u", b, t, k);
                // Without read/write action, ingress owner metadata carries over.
                new_constr_map.insert(named_constr(
                    constr_name,
                    implies(!any_write && !any_read, buf_in_[b][t][k + 1] == buf_in_[b][t][k])));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_e_keep_b%u_t%u_k%u", b, t, k);
                // Without read/write action, egress owner metadata carries over.
                new_constr_map.insert(named_constr(
                    constr_name,
                    implies(!any_write && !any_read, buf_e_[b][t][k + 1] == buf_e_[b][t][k])));
                snprintf(constr_name, sizeof(constr_name), "sb_buf_stamp_keep_b%u_t%u_k%u", b, t, k);
                // Without read/write action, timestamp carries over.
                new_constr_map.insert(named_constr(
                    constr_name,
                    implies(!any_write && !any_read, buf_stamp_[b][t][k + 1] == buf_stamp_[b][t][k])));
            }
        }
    }

    // Accounting closure at deq micro-step.
    for (unsigned int t = 0; t < total_time; t++) {
        expr read_sum = int0;
        for (unsigned int e = 0; e < eg_cnt; e++) {
            for (unsigned int b = 0; b < slot_cnt; b++) {
                read_sum = read_sum + b2i(read_slot_[e][b][t][deq_k]);
            }
        }

        expr release_in_sum = int0;
        expr release_hdr_sum = int0;
        expr release_e_sum = int0;
        for (unsigned int q = 0; q < in_cnt; q++) {
            expr reads_from_q_sum = int0;
            for (unsigned int e = 0; e < eg_cnt; e++) {
                for (unsigned int b = 0; b < slot_cnt; b++) {
                    reads_from_q_sum = reads_from_q_sum +
                                       b2i(read_slot_[e][b][t][deq_k] &&
                                           buf_in_[b][t][deq_k] == (int) q);
                }
            }
            expr has_read_from_q = reads_from_q_sum == int1;
            snprintf(constr_name, sizeof(constr_name), "sb_release_hdr_from_reads_q%u_t%u_k%u", q, t, deq_k);
            // On dequeue, queue q releases headroom first when a read occurs and headroom is non-empty.
            new_constr_map.insert(named_constr(
                constr_name,
                release_hdr_[q][t][deq_k] == (has_read_from_q && headroom_pool_occ_[q][t][deq_k] > 0)));
            snprintf(constr_name, sizeof(constr_name), "sb_release_in_from_reads_q%u_t%u_k%u", q, t, deq_k);
            // On dequeue, queue q releases ingress only when a read occurs and headroom is already empty.
            new_constr_map.insert(named_constr(
                constr_name,
                release_in_[q][t][deq_k] == (has_read_from_q && headroom_pool_occ_[q][t][deq_k] == 0)));

            release_in_sum = release_in_sum + b2i(release_in_[q][t][deq_k]);
            release_hdr_sum = release_hdr_sum + b2i(release_hdr_[q][t][deq_k]);
        }
        for (unsigned int e = 0; e < eg_cnt; e++) {
            release_e_sum = release_e_sum + b2i(release_e_[e][t][deq_k]);
        }

        snprintf(constr_name, sizeof(constr_name), "sb_release_in_hdr_match_read_t%u_k%u", t, deq_k);
        // Total (ingress + headroom) releases must match total reads in the deq micro-step.
        new_constr_map.insert(named_constr(constr_name, release_in_sum + release_hdr_sum == read_sum));
        snprintf(constr_name, sizeof(constr_name), "sb_release_e_match_read_t%u_k%u", t, deq_k);
        // Total egress releases must match total reads in the deq micro-step.
        new_constr_map.insert(named_constr(constr_name, release_e_sum == read_sum));
    }
}

void SharedBufferPoint::add_cross_time_step_constrs(map<string, expr>& new_constr_map) {
    const unsigned int in_cnt = (unsigned int) in_queues.size();
    const unsigned int eg_cnt = (unsigned int) out_queues.size();
    const unsigned int slot_cnt = sb_info.buffer_slot_cnt;
    const unsigned int ingress_pool_cap_cnt = ingress_pool_cap();
    const unsigned int micro_steps = micro_step_cnt();

    if (total_time == 0) return;

    char constr_name[160];

    // Initial state: empty shared-buffer and zero per-pool ledgers.
    for (unsigned int q = 0; q < in_cnt; q++) {
        snprintf(constr_name, sizeof(constr_name), "sb_ing_pos_init_q%u", q);
        // Initial ingress-pool ledger starts at zero.
        new_constr_map.insert(named_constr(constr_name, ingress_pool_occ_[q][0][0] == 0));
        snprintf(constr_name, sizeof(constr_name), "sb_hdr_pos_init_q%u", q);
        // Initial headroom ledger starts at zero.
        new_constr_map.insert(named_constr(constr_name, headroom_pool_occ_[q][0][0] == 0));
        snprintf(constr_name, sizeof(constr_name), "sb_paused_init_q%u", q);
        // Initial pause state is resumed (not paused).
        new_constr_map.insert(named_constr(constr_name, !paused_[q][0]));
    }
    for (unsigned int e = 0; e < eg_cnt; e++) {
        snprintf(constr_name, sizeof(constr_name), "sb_eg_pos_init_e%u", e);
        // Initial egress-lossless ledger starts at zero.
        new_constr_map.insert(named_constr(constr_name, egress_lossless_pool_occ_[e][0][0] == 0));
    }
    for (unsigned int b = 0; b < slot_cnt; b++) {
        snprintf(constr_name, sizeof(constr_name), "sb_buf_pkt_init_null_b%u", b);
        // Initial slot packet identity is null.
        new_constr_map.insert(named_constr(constr_name, buf_pkt_[b][0][0] == net_ctx.null_pkt()));
        snprintf(constr_name, sizeof(constr_name), "sb_buf_in_init_neg1_b%u", b);
        // Initial ingress owner metadata is -1 (no owner).
        new_constr_map.insert(named_constr(constr_name, buf_in_[b][0][0] == -1));
        snprintf(constr_name, sizeof(constr_name), "sb_buf_e_init_neg1_b%u", b);
        // Initial egress owner metadata is -1 (no owner).
        new_constr_map.insert(named_constr(constr_name, buf_e_[b][0][0] == -1));
        snprintf(constr_name, sizeof(constr_name), "sb_buf_stamp_init_neg1_b%u", b);
        // Initial slot timestamp is -1 (empty).
        new_constr_map.insert(named_constr(constr_name, buf_stamp_[b][0][0] == -1));
    }

    // Cross-slot state handoff: [t][last_k] -> [t+1][0].
    for (unsigned int t = 0; t + 1 < total_time; t++) {
        for (unsigned int q = 0; q < in_cnt; q++) {
            snprintf(constr_name, sizeof(constr_name), "sb_ing_handoff_q%u_t%u", q, t);
            // End-of-slot ingress ledger becomes next slot's initial ingress ledger.
            new_constr_map.insert(named_constr(
                constr_name,
                ingress_pool_occ_[q][t + 1][0] == ingress_pool_occ_[q][t][micro_steps]));

            snprintf(constr_name, sizeof(constr_name), "sb_hdr_handoff_q%u_t%u", q, t);
            // End-of-slot headroom ledger becomes next slot's initial headroom ledger.
            new_constr_map.insert(named_constr(
                constr_name,
                headroom_pool_occ_[q][t + 1][0] == headroom_pool_occ_[q][t][micro_steps]));

            expr in_end = ingress_pool_occ_[q][t][micro_steps];
            expr in_total_end = ingress_pool_total_occ_[t][micro_steps];
            expr hdr_end = headroom_pool_occ_[q][t][micro_steps];
            // TODO(shared-buffer): Parameterize per-queue alpha and use
            // gamma_q_end = alpha_q * remaining_ingress_pool_end.
            // Current model fixes alpha = 1.
            expr gamma_q_end = net_ctx.int_val((int) ingress_pool_cap_cnt) - in_total_end;
            expr xoff_trig = in_end >= gamma_q_end;
            expr xon_trig = paused_[q][t] && (hdr_end == 0) && (in_end < gamma_q_end);
            expr paused_next = (paused_[q][t] || xoff_trig) && !xon_trig;

            snprintf(constr_name, sizeof(constr_name), "sb_paused_handoff_q%u_t%u", q, t);
            // Pause state has one-slot delay: triggers at t take effect at t+1.
            new_constr_map.insert(named_constr(constr_name, paused_[q][t + 1] == paused_next));
        }

        for (unsigned int e = 0; e < eg_cnt; e++) {
            snprintf(constr_name, sizeof(constr_name), "sb_eg_handoff_e%u_t%u", e, t);
            // End-of-slot egress-lossless ledger becomes next slot's initial egress ledger.
            new_constr_map.insert(named_constr(
                constr_name,
                egress_lossless_pool_occ_[e][t + 1][0] == egress_lossless_pool_occ_[e][t][micro_steps]));
        }

        for (unsigned int b = 0; b < slot_cnt; b++) {
            snprintf(constr_name, sizeof(constr_name), "sb_buf_pkt_handoff_b%u_t%u", b, t);
            // End-of-slot slot packet identity becomes next slot's initial packet identity.
            new_constr_map.insert(named_constr(
                constr_name,
                buf_pkt_[b][t + 1][0] == buf_pkt_[b][t][micro_steps]));
            snprintf(constr_name, sizeof(constr_name), "sb_buf_in_handoff_b%u_t%u", b, t);
            // End-of-slot ingress owner metadata becomes next slot's initial ingress owner metadata.
            new_constr_map.insert(named_constr(
                constr_name,
                buf_in_[b][t + 1][0] == buf_in_[b][t][micro_steps]));
            snprintf(constr_name, sizeof(constr_name), "sb_buf_e_handoff_b%u_t%u", b, t);
            // End-of-slot egress owner metadata becomes next slot's initial egress owner metadata.
            new_constr_map.insert(named_constr(
                constr_name,
                buf_e_[b][t + 1][0] == buf_e_[b][t][micro_steps]));
            snprintf(constr_name, sizeof(constr_name), "sb_buf_stamp_handoff_b%u_t%u", b, t);
            // End-of-slot slot timestamp becomes next slot's initial timestamp.
            new_constr_map.insert(named_constr(
                constr_name,
                buf_stamp_[b][t + 1][0] == buf_stamp_[b][t][micro_steps]));
        }

        snprintf(constr_name, sizeof(constr_name), "sb_ing_used_handoff_t%u", t);
        // End-of-slot aggregate ingress usage is handed off to next slot initial state.
        new_constr_map.insert(
            named_constr(constr_name, ingress_pool_total_occ_[t + 1][0] == ingress_pool_total_occ_[t][micro_steps]));
        snprintf(constr_name, sizeof(constr_name), "sb_hdr_used_handoff_t%u", t);
        // End-of-slot aggregate headroom usage is handed off to next slot initial state.
        new_constr_map.insert(
            named_constr(constr_name, headroom_pool_total_occ_[t + 1][0] == headroom_pool_total_occ_[t][micro_steps]));
        snprintf(constr_name, sizeof(constr_name), "sb_eg_used_handoff_t%u", t);
        // End-of-slot aggregate egress usage is handed off to next slot initial state.
        new_constr_map.insert(named_constr(
            constr_name,
            egress_lossless_pool_total_occ_[t + 1][0] == egress_lossless_pool_total_occ_[t][micro_steps]));
    }
}

void SharedBufferPoint::add_in_queue_constrs() {
    ContentionPoint::add_in_queue_constrs();

    const unsigned int in_cnt = (unsigned int) in_queues.size();
    const unsigned int eg_cnt = (unsigned int) out_queues.size();
    if (in_cnt == 0 || eg_cnt == 0) return;

    if (!shared_vars_initialized_) init_shared_vars();

    map<string, expr> new_constr_map;
    add_micro_step_constrs(new_constr_map);
    add_cross_time_step_constrs(new_constr_map);
    add_constr_from_map(new_constr_map);
}

void SharedBufferPoint::add_out_queue_constrs() {
    ContentionPoint::add_out_queue_constrs();

    const unsigned int eg_cnt = (unsigned int) out_queues.size();
    const unsigned int slot_cnt = sb_info.buffer_slot_cnt;
    const unsigned int deq_k = deq_step_idx();
    if (eg_cnt == 0 || slot_cnt == 0) return;

    const expr int0 = net_ctx.int_val(0);
    const expr int1 = net_ctx.int_val(1);
    auto b2i = [&](const expr& b) { return ite(b, int1, int0); };

    char constr_name[160];
    map<string, expr> new_constr_map;

    for (unsigned int e = 0; e < eg_cnt; e++) {
        Queue* outq = out_queues[e];
        for (unsigned int t = 0; t < total_time; t++) {
            expr reads_this_e_t_sum = int0;
            expr_vector cand_vec(net_ctx.z3_ctx());
            expr_vector min_witness_vec(net_ctx.z3_ctx());

            for (unsigned int b = 0; b < slot_cnt; b++) {
                expr cand = net_ctx.pkt2val(buf_pkt_[b][t][deq_k]) &&
                            (buf_e_[b][t][deq_k] == (int) e);
                cand_vec.push_back(cand);
                min_witness_vec.push_back(cand && (buf_stamp_[b][t][deq_k] == min_stamp_e_[e][t]));

                snprintf(constr_name,
                         sizeof(constr_name),
                         "sb_read_requires_nonempty_e%u_b%u_t%u_k%u",
                         e,
                         b,
                         t,
                         deq_k);
                // A read action can only target a slot that is currently occupied.
                new_constr_map.insert(
                    named_constr(constr_name,
                                 implies(read_slot_[e][b][t][deq_k],
                                         net_ctx.pkt2val(buf_pkt_[b][t][deq_k]))));
                snprintf(constr_name,
                         sizeof(constr_name),
                         "sb_read_requires_match_egress_e%u_b%u_t%u_k%u",
                         e,
                         b,
                         t,
                         deq_k);
                // A read action by egress e can only target a slot owned by egress e.
                new_constr_map.insert(
                    named_constr(constr_name,
                                 implies(read_slot_[e][b][t][deq_k],
                                         buf_e_[b][t][deq_k] == (int) e)));
                snprintf(constr_name,
                         sizeof(constr_name),
                         "sb_read_requires_hol_stamp_e%u_b%u_t%u_k%u",
                         e,
                         b,
                         t,
                         deq_k);
                // A read action by egress e must target an HOL packet (minimum timestamp among e's candidates).
                new_constr_map.insert(
                    named_constr(constr_name,
                                 implies(read_slot_[e][b][t][deq_k],
                                         buf_stamp_[b][t][deq_k] == min_stamp_e_[e][t])));
                snprintf(constr_name,
                         sizeof(constr_name),
                         "sb_min_stamp_lb_e%u_b%u_t%u_k%u",
                         e,
                         b,
                         t,
                         deq_k);
                // Minimum timestamp is a lower bound of each readable candidate's timestamp.
                new_constr_map.insert(
                    named_constr(constr_name,
                                 implies(cand, buf_stamp_[b][t][deq_k] >= min_stamp_e_[e][t])));
                reads_this_e_t_sum = reads_this_e_t_sum + b2i(read_slot_[e][b][t][deq_k]);
            }

            snprintf(constr_name, sizeof(constr_name), "sb_has_pkt_e_def_e%u_t%u", e, t);
            // has_pkt_e is true iff egress e has at least one readable candidate at the deq micro-step.
            new_constr_map.insert(
                named_constr(constr_name, has_pkt_e_[e][t] == mk_or(cand_vec)));
            snprintf(constr_name, sizeof(constr_name), "sb_min_stamp_empty_e%u_t%u", e, t);
            // If no candidate exists for egress e, min_stamp is set to -1.
            new_constr_map.insert(
                named_constr(constr_name, implies(!has_pkt_e_[e][t], min_stamp_e_[e][t] == -1)));
            snprintf(constr_name, sizeof(constr_name), "sb_min_stamp_lb_e%u_t%u", e, t);
            // min_stamp lower bound.
            // TODO: this may be redundant
            new_constr_map.insert(named_constr(constr_name, min_stamp_e_[e][t] >= -1));
            snprintf(constr_name, sizeof(constr_name), "sb_min_stamp_witness_e%u_t%u", e, t);
            // If candidates exist, at least one candidate has timestamp equal to min_stamp.
            new_constr_map.insert(
                named_constr(constr_name, implies(has_pkt_e_[e][t], mk_or(min_witness_vec))));

            snprintf(constr_name, sizeof(constr_name), "sb_release_e_def_e%u_t%u_k%u", e, t, deq_k);
            // Egress release flag is true iff this egress reads at least one slot in deq micro-step.
            new_constr_map.insert(
                named_constr(constr_name, release_e_[e][t][deq_k] == (reads_this_e_t_sum > 0)));
            snprintf(constr_name, sizeof(constr_name), "sb_read_deq_eq_e%u_t%u_k%u", e, t, deq_k);
            // Read count for this egress equals out-queue deq count at time t.
            new_constr_map.insert(named_constr(constr_name, reads_this_e_t_sum == outq->deq_cnt(t)));
        }
    }

    for (unsigned int b = 0; b < slot_cnt; b++) {
        for (unsigned int t = 0; t < total_time; t++) {
            expr slot_readers_sum = int0;
            for (unsigned int e = 0; e < eg_cnt; e++) {
                slot_readers_sum = slot_readers_sum + b2i(read_slot_[e][b][t][deq_k]);
            }
            snprintf(constr_name,
                     sizeof(constr_name),
                     "sb_slot_single_reader_b%u_t%u_k%u",
                     b,
                     t,
                     deq_k);
            // A slot can be read by at most one egress in the deq micro-step.
            new_constr_map.insert(named_constr(constr_name, slot_readers_sum <= 1));
        }
    }

    add_constr_from_map(new_constr_map);
}

m_val_expr_t SharedBufferPoint::metric_val_expr(metric_t metric, unsigned int q, unsigned t) {
    return ContentionPoint::metric_val_expr(metric, q, t);
}
