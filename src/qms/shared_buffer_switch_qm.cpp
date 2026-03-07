//
//  shared_buffer_switch_qm.cpp
//  FSB
//
//  Created by Wenkai Li on 3/7/26.
//

#include "shared_buffer_switch_qm.hpp"

#include <cstdio>
#include <stdexcept>

SharedBufferSwitchQM::SharedBufferSwitchQM(cid_t id,
                                           unsigned int total_time,
                                           vector<QueueInfo> in_queue_info,
                                           vector<QueueInfo> out_queue_info,
                                           SharedBufferSwitchInfo info,
                                           NetContext& net_ctx):
QueuingModule(id, total_time, in_queue_info, out_queue_info, net_ctx),
sb_info_(info) {
    init(net_ctx);
}

unsigned int SharedBufferSwitchQM::micro_step_cnt() const {
    return (unsigned int) in_queues.size() + 1;
}

unsigned int SharedBufferSwitchQM::deq_step_idx() const {
    if (sb_info_.timing_mode == SharedBufferSwitchInfo::TimingMode::DEQ_FIRST) return 0;
    return (unsigned int) in_queues.size();
}

int SharedBufferSwitchQM::ingress_queue_for_step(unsigned int k) const {
    const unsigned int in_cnt = (unsigned int) in_queues.size();
    if (sb_info_.timing_mode == SharedBufferSwitchInfo::TimingMode::DEQ_FIRST) {
        if (k == 0 || k > in_cnt) return -1;
        return (int) (k - 1);
    }

    if (k >= in_cnt) return -1;
    return (int) k;
}

unsigned int SharedBufferSwitchQM::ingress_pool_cap() const {
    if (sb_info_.ingress_pool_size > 0) return sb_info_.ingress_pool_size;
    return sb_info_.buffer_slot_cnt;
}

unsigned int SharedBufferSwitchQM::headroom_pool_cap() const {
    if (sb_info_.headroom_pool_size > 0) return sb_info_.headroom_pool_size;
    return sb_info_.buffer_slot_cnt;
}

unsigned int SharedBufferSwitchQM::egress_lossless_pool_cap() const {
    if (sb_info_.egress_lossless_pool_size > 0) return sb_info_.egress_lossless_pool_size;
    return sb_info_.buffer_slot_cnt;
}

unsigned int SharedBufferSwitchQM::ingress_serialization_factor(unsigned int q) const {
    if (q >= sb_info_.ingress_ser_factor.size()) return 1;
    unsigned int f = sb_info_.ingress_ser_factor[q];
    if (f == 0) return 1;
    return f;
}

unsigned int SharedBufferSwitchQM::egress_serialization_factor(unsigned int e) const {
    if (e >= sb_info_.egress_ser_factor.size()) return 1;
    unsigned int f = sb_info_.egress_ser_factor[e];
    if (f == 0) return 1;
    return f;
}

const expr&
SharedBufferSwitchQM::ingress_paused(unsigned int q, unsigned int t) const {
    return ingress_paused_[q][t];
}

const expr&
SharedBufferSwitchQM::slot_pkt(unsigned int b, unsigned int t, unsigned int k) const {
    return buf_pkt_[b][t][k];
}

SBQueue*
SharedBufferSwitchQM::ingress_sb_queue(unsigned int q) {
    SBQueue* sbq = dynamic_cast<SBQueue*>(in_queues[q]);
    if (sbq == nullptr) {
        throw runtime_error("SharedBufferSwitchQM ingress queue is not an SBQueue");
    }
    return sbq;
}

SBQueue*
SharedBufferSwitchQM::egress_sb_queue(unsigned int e) {
    SBQueue* sbq = dynamic_cast<SBQueue*>(out_queues[e]);
    if (sbq == nullptr) {
        throw runtime_error("SharedBufferSwitchQM egress queue is not an SBQueue");
    }
    return sbq;
}

void SharedBufferSwitchQM::add_proc_vars(NetContext& net_ctx) {
    const unsigned int in_cnt = (unsigned int) in_queues.size();
    const unsigned int eg_cnt = (unsigned int) out_queues.size();
    const unsigned int slot_cnt = sb_info_.buffer_slot_cnt;
    const unsigned int micro_steps = micro_step_cnt();
    const unsigned int k_states = micro_steps + 1;

    buf_pkt_.clear();
    buf_in_.clear();
    buf_e_.clear();
    buf_is_hdr_.clear();
    buf_stamp_.clear();
    ingress_paused_.clear();
    ingress_pool_occ_.clear();
    ingress_pool_total_occ_.clear();
    headroom_pool_occ_.clear();
    headroom_pool_total_occ_.clear();
    egress_lossless_pool_occ_.clear();
    egress_lossless_pool_total_occ_.clear();
    admit_in_.clear();
    admit_e_.clear();
    admit_.clear();
    alloc_in_.clear();
    alloc_hdr_.clear();
    alloc_eg_lossless_.clear();
    write_slot_.clear();
    read_slot_.clear();
    min_stamp_e_.clear();
    has_pkt_e_.clear();

    buf_pkt_.resize(slot_cnt);
    buf_in_.resize(slot_cnt);
    buf_e_.resize(slot_cnt);
    buf_is_hdr_.resize(slot_cnt);
    buf_stamp_.resize(slot_cnt);
    ingress_paused_.resize(in_cnt);
    ingress_pool_occ_.resize(in_cnt);
    ingress_pool_total_occ_.resize(total_time);
    headroom_pool_occ_.resize(in_cnt);
    headroom_pool_total_occ_.resize(total_time);
    egress_lossless_pool_occ_.resize(eg_cnt);
    egress_lossless_pool_total_occ_.resize(total_time);
    admit_in_.resize(in_cnt);
    admit_e_.resize(eg_cnt);
    admit_.resize(in_cnt);
    alloc_in_.resize(in_cnt);
    alloc_hdr_.resize(in_cnt);
    alloc_eg_lossless_.resize(eg_cnt);
    write_slot_.resize(in_cnt);
    read_slot_.resize(slot_cnt);
    min_stamp_e_.resize(eg_cnt);
    has_pkt_e_.resize(eg_cnt);

    char vname[160];

    for (unsigned int b = 0; b < slot_cnt; b++) {
        buf_pkt_[b].resize(total_time);
        buf_in_[b].resize(total_time);
        buf_e_[b].resize(total_time);
        buf_is_hdr_[b].resize(total_time);
        buf_stamp_[b].resize(total_time);
        for (unsigned int t = 0; t < total_time; t++) {
            for (unsigned int k = 0; k < k_states; k++) {
                snprintf(vname, sizeof(vname), "%s_buf_pkt[%u][%u][%u]", id.c_str(), b, t, k);
                buf_pkt_[b][t].push_back(net_ctx.pkt_const(vname));
                snprintf(vname, sizeof(vname), "%s_buf_in[%u][%u][%u]", id.c_str(), b, t, k);
                buf_in_[b][t].push_back(net_ctx.int_const(vname));
                snprintf(vname, sizeof(vname), "%s_buf_e[%u][%u][%u]", id.c_str(), b, t, k);
                buf_e_[b][t].push_back(net_ctx.int_const(vname));
                snprintf(vname, sizeof(vname), "%s_buf_is_hdr[%u][%u][%u]", id.c_str(), b, t, k);
                buf_is_hdr_[b][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "%s_buf_stamp[%u][%u][%u]", id.c_str(), b, t, k);
                buf_stamp_[b][t].push_back(net_ctx.int_const(vname));
            }
        }
    }

    for (unsigned int q = 0; q < in_cnt; q++) {
        ingress_paused_[q].reserve(total_time);
        ingress_pool_occ_[q].resize(total_time);
        headroom_pool_occ_[q].resize(total_time);
        admit_in_[q].resize(total_time);
        admit_[q].resize(total_time);
        alloc_in_[q].resize(total_time);
        alloc_hdr_[q].resize(total_time);
        write_slot_[q].resize(slot_cnt);

        for (unsigned int t = 0; t < total_time; t++) {
            snprintf(vname, sizeof(vname), "%s_ingress_paused[%u][%u]", id.c_str(), q, t);
            ingress_paused_[q].push_back(net_ctx.bool_const(vname));

            for (unsigned int k = 0; k < k_states; k++) {
                snprintf(vname,
                         sizeof(vname),
                         "%s_ingress_pool_occ[%u][%u][%u]",
                         id.c_str(),
                         q,
                         t,
                         k);
                ingress_pool_occ_[q][t].push_back(net_ctx.int_const(vname));
                snprintf(vname,
                         sizeof(vname),
                         "%s_headroom_pool_occ[%u][%u][%u]",
                         id.c_str(),
                         q,
                         t,
                         k);
                headroom_pool_occ_[q][t].push_back(net_ctx.int_const(vname));
            }

            for (unsigned int k = 0; k < micro_steps; k++) {
                snprintf(vname, sizeof(vname), "%s_admit_in[%u][%u][%u]", id.c_str(), q, t, k);
                admit_in_[q][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "%s_admit[%u][%u][%u]", id.c_str(), q, t, k);
                admit_[q][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "%s_alloc_in[%u][%u][%u]", id.c_str(), q, t, k);
                alloc_in_[q][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "%s_alloc_hdr[%u][%u][%u]", id.c_str(), q, t, k);
                alloc_hdr_[q][t].push_back(net_ctx.bool_const(vname));
            }
        }

        for (unsigned int b = 0; b < slot_cnt; b++) {
            write_slot_[q][b].resize(total_time);
            for (unsigned int t = 0; t < total_time; t++) {
                for (unsigned int k = 0; k < micro_steps; k++) {
                    snprintf(
                        vname, sizeof(vname), "%s_write_slot[%u][%u][%u][%u]", id.c_str(), q, b, t, k);
                    write_slot_[q][b][t].push_back(net_ctx.bool_const(vname));
                }
            }
        }
    }

    for (unsigned int e = 0; e < eg_cnt; e++) {
        egress_lossless_pool_occ_[e].resize(total_time);
        admit_e_[e].resize(total_time);
        alloc_eg_lossless_[e].resize(total_time);
        min_stamp_e_[e].reserve(total_time);
        has_pkt_e_[e].reserve(total_time);

        for (unsigned int t = 0; t < total_time; t++) {
            snprintf(vname, sizeof(vname), "%s_min_stamp_e[%u][%u]", id.c_str(), e, t);
            min_stamp_e_[e].push_back(net_ctx.int_const(vname));
            snprintf(vname, sizeof(vname), "%s_has_pkt_e[%u][%u]", id.c_str(), e, t);
            has_pkt_e_[e].push_back(net_ctx.bool_const(vname));

            for (unsigned int k = 0; k < k_states; k++) {
                snprintf(vname,
                         sizeof(vname),
                         "%s_egress_lossless_pool_occ[%u][%u][%u]",
                         id.c_str(),
                         e,
                         t,
                         k);
                egress_lossless_pool_occ_[e][t].push_back(net_ctx.int_const(vname));
            }

            for (unsigned int k = 0; k < micro_steps; k++) {
                snprintf(vname, sizeof(vname), "%s_admit_e[%u][%u][%u]", id.c_str(), e, t, k);
                admit_e_[e][t].push_back(net_ctx.bool_const(vname));
                snprintf(vname,
                         sizeof(vname),
                         "%s_alloc_eg_lossless[%u][%u][%u]",
                         id.c_str(),
                         e,
                         t,
                         k);
                alloc_eg_lossless_[e][t].push_back(net_ctx.bool_const(vname));
            }
        }
    }

    for (unsigned int b = 0; b < slot_cnt; b++) {
        read_slot_[b].resize(total_time);
        for (unsigned int t = 0; t < total_time; t++) {
            for (unsigned int k = 0; k < micro_steps; k++) {
                snprintf(vname, sizeof(vname), "%s_read_slot[%u][%u][%u]", id.c_str(), b, t, k);
                read_slot_[b][t].push_back(net_ctx.bool_const(vname));
            }
        }
    }

    for (unsigned int t = 0; t < total_time; t++) {
        for (unsigned int k = 0; k < k_states; k++) {
            snprintf(vname, sizeof(vname), "%s_ingress_pool_total_occ[%u][%u]", id.c_str(), t, k);
            ingress_pool_total_occ_[t].push_back(net_ctx.int_const(vname));
            snprintf(vname, sizeof(vname), "%s_headroom_pool_total_occ[%u][%u]", id.c_str(), t, k);
            headroom_pool_total_occ_[t].push_back(net_ctx.int_const(vname));
            snprintf(vname,
                     sizeof(vname),
                     "%s_egress_lossless_pool_total_occ[%u][%u]",
                     id.c_str(),
                     t,
                     k);
            egress_lossless_pool_total_occ_[t].push_back(net_ctx.int_const(vname));
        }
    }
}

void SharedBufferSwitchQM::add_constrs(NetContext& net_ctx, map<string, expr>& constr_map) {
    add_scaffold_constrs(net_ctx, constr_map);
}

void SharedBufferSwitchQM::add_scaffold_constrs(NetContext& net_ctx,
                                                map<string, expr>& constr_map) {
    const unsigned int in_cnt = (unsigned int) in_queues.size();
    const unsigned int eg_cnt = (unsigned int) out_queues.size();
    const unsigned int slot_cnt = sb_info_.buffer_slot_cnt;
    const unsigned int ingress_pool_cap_cnt = ingress_pool_cap();
    const unsigned int headroom_pool_cap_cnt = headroom_pool_cap();
    const unsigned int micro_steps = micro_step_cnt();
    const unsigned int k_states = micro_steps + 1;
    const unsigned int deq_k = deq_step_idx();
    char constr_name[180];
    const expr int0 = net_ctx.int_val(0);
    const expr int1 = net_ctx.int_val(1);

    auto b2i = [&](const expr& b) { return ite(b, int1, int0); };
    auto reads_from_q = [&](unsigned int q, unsigned int t, unsigned int k) -> expr {
        expr res = int0;
        for (unsigned int b = 0; b < slot_cnt; b++) {
            res = res + b2i(read_slot_[b][t][k] && buf_in_[b][t][k] == (int) q);
        }
        return res;
    };

    // Scaffold only: keep the new QM safe and deterministic until shared-buffer
    // semantics are migrated out of SharedBufferPoint.
    if (total_time > 0) {
        // Initial pool ledgers start at zero before the first micro-step.
        for (unsigned int q = 0; q < in_cnt; q++) {
            snprintf(constr_name, sizeof(constr_name), "%s_ingress_pool_init_q%u", id.c_str(), q);
            constr_map.insert(named_constr(constr_name, ingress_pool_occ_[q][0][0] == 0));
            snprintf(constr_name, sizeof(constr_name), "%s_headroom_pool_init_q%u", id.c_str(), q);
            constr_map.insert(named_constr(constr_name, headroom_pool_occ_[q][0][0] == 0));
        }
        snprintf(constr_name, sizeof(constr_name), "%s_ingress_pool_total_init", id.c_str());
        constr_map.insert(named_constr(constr_name, ingress_pool_total_occ_[0][0] == 0));
        snprintf(constr_name, sizeof(constr_name), "%s_headroom_pool_total_init", id.c_str());
        constr_map.insert(named_constr(constr_name, headroom_pool_total_occ_[0][0] == 0));
    }

    for (unsigned int q = 0; q < in_cnt; q++) {
        if (total_time > 0) {
            SBQueue* inq = ingress_sb_queue(q);

            snprintf(
                constr_name, sizeof(constr_name), "%s_ingress_paused_init_q%u", id.c_str(), q);
            // Shared-buffer ingress starts resumed.
            constr_map.insert(named_constr(constr_name, !ingress_paused_[q][0]));
            snprintf(constr_name, sizeof(constr_name), "%s_ingress_pause_tx_tail_q%u", id.c_str(), q);
            // The last slot has no modeled next slot inside this horizon, so we suppress new
            // pause emission here.
            constr_map.insert(named_constr(constr_name, !inq->pause_tx(total_time - 1)));
            snprintf(
                constr_name, sizeof(constr_name), "%s_ingress_resume_tx_tail_q%u", id.c_str(), q);
            // Likewise suppress resume emission in the terminal slot of the horizon.
            constr_map.insert(named_constr(constr_name, !inq->resume_tx(total_time - 1)));
        }
    }

    for (unsigned int t = 0; t < total_time; t++) {
        for (unsigned int q = 0; q < in_cnt; q++) {
            SBQueue* inq = ingress_sb_queue(q);

            snprintf(
                constr_name, sizeof(constr_name), "%s_input_deq_zero_q%u_t%u", id.c_str(), q, t);
            constr_map.insert(named_constr(constr_name, in_queues[q]->deq_cnt(t) == 0));

            for (unsigned int k = 0; k < micro_steps; k++) {
                const int active_q = ingress_queue_for_step(k);
                if (active_q < 0 || (unsigned int) active_q != q) {
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "%s_admit_in_false_q%u_t%u_k%u",
                             id.c_str(),
                             q,
                             t,
                             k);
                    // Only the scheduled ingress can issue an ingress-side request in this
                    // micro-step.
                    constr_map.insert(named_constr(constr_name, !admit_in_[q][t][k]));
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "%s_admit_false_q%u_t%u_k%u",
                             id.c_str(),
                             q,
                             t,
                             k);
                    // Only the scheduled ingress can be admitted in this micro-step.
                    constr_map.insert(named_constr(constr_name, !admit_[q][t][k]));
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "%s_alloc_in_false_q%u_t%u_k%u",
                             id.c_str(),
                             q,
                             t,
                             k);
                    // Inactive ingress queues cannot allocate ingress-pool credits.
                    constr_map.insert(named_constr(constr_name, !alloc_in_[q][t][k]));
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "%s_alloc_hdr_false_q%u_t%u_k%u",
                             id.c_str(),
                             q,
                             t,
                             k);
                    // Inactive ingress queues cannot allocate headroom credits.
                    constr_map.insert(named_constr(constr_name, !alloc_hdr_[q][t][k]));
                    continue;
                }

                expr pkt = inq->enqs(0)[t];
                expr has_enq = net_ctx.pkt2val(pkt);
                expr dst_eg = net_ctx.pkt2meta1(pkt) % net_ctx.int_val((int) eg_cnt);
                expr has_ing_credit = ingress_pool_total_occ_[t][k] < (int) ingress_pool_cap_cnt;
                expr has_hdr_credit = headroom_pool_occ_[q][t][k] < (int) headroom_pool_cap_cnt;
                expr ingress_can_admit =
                    (!ingress_paused_[q][t] && has_ing_credit) ||
                    (ingress_paused_[q][t] && has_hdr_credit);
                expr any_admit_e = net_ctx.bool_val(false);
                expr_vector route_cases(net_ctx.z3_ctx());

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_admit_in_from_has_enq_q%u_t%u_k%u",
                         id.c_str(),
                         q,
                         t,
                         k);
                // Ingress-side admit is granted exactly when a packet is present and the local
                // ingress/headroom accounting path has available credit.
                constr_map.insert(
                    named_constr(constr_name, admit_in_[q][t][k] == (has_enq && ingress_can_admit)));

                for (unsigned int e = 0; e < eg_cnt; e++) {
                    expr route_to_e = dst_eg == net_ctx.int_val((int) e);
                    any_admit_e = any_admit_e || admit_e_[e][t][k];
                    route_cases.push_back(
                        implies(has_enq && route_to_e,
                                admit_[q][t][k] == (admit_in_[q][t][k] && admit_e_[e][t][k])));

                    snprintf(constr_name,
                             sizeof(constr_name),
                             "%s_admit_e_non_route_false_q%u_e%u_t%u_k%u",
                             id.c_str(),
                             q,
                             e,
                             t,
                             k);
                    // Only the routed egress may keep its admission gate enabled for this packet.
                    constr_map.insert(named_constr(
                        constr_name, implies(!has_enq || !route_to_e, !admit_e_[e][t][k])));
                }

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_admit_route_q%u_t%u_k%u",
                         id.c_str(),
                         q,
                         t,
                         k);
                // On the routed egress, final admit follows the ingress request and that routed
                // egress gate.
                constr_map.insert(named_constr(constr_name, mk_and(route_cases)));

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_admit_requires_in_and_eg_q%u_t%u_k%u",
                         id.c_str(),
                         q,
                         t,
                         k);
                // Final admit requires both ingress-side admit and an enabled routed egress gate.
                constr_map.insert(
                    named_constr(constr_name, admit_[q][t][k] == (admit_in_[q][t][k] && any_admit_e)));

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_alloc_in_by_paused_q%u_t%u_k%u",
                         id.c_str(),
                         q,
                         t,
                         k);
                // Admitted packets use ingress-pool accounting exactly when this ingress is not
                // paused in slot t.
                constr_map.insert(named_constr(
                    constr_name, alloc_in_[q][t][k] == (admit_[q][t][k] && !ingress_paused_[q][t])));

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_alloc_hdr_by_paused_q%u_t%u_k%u",
                         id.c_str(),
                         q,
                         t,
                         k);
                // Admitted packets use headroom accounting exactly when this ingress is paused in
                // slot t.
                constr_map.insert(named_constr(
                    constr_name, alloc_hdr_[q][t][k] == (admit_[q][t][k] && ingress_paused_[q][t])));

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_alloc_in_requires_credit_q%u_t%u_k%u",
                         id.c_str(),
                         q,
                         t,
                         k);
                // Ingress-pool allocation requires remaining global ingress-pool credit at the
                // current micro-state.
                constr_map.insert(named_constr(
                    constr_name,
                    implies(alloc_in_[q][t][k], ingress_pool_total_occ_[t][k] < (int) ingress_pool_cap_cnt)));

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_alloc_hdr_requires_credit_q%u_t%u_k%u",
                         id.c_str(),
                         q,
                         t,
                         k);
                // Headroom allocation requires remaining per-ingress headroom credit at the
                // current micro-state.
                constr_map.insert(named_constr(
                    constr_name,
                    implies(alloc_hdr_[q][t][k], headroom_pool_occ_[q][t][k] < (int) headroom_pool_cap_cnt)));

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_alloc_cover_admit_q%u_t%u_k%u",
                         id.c_str(),
                         q,
                         t,
                         k);
                // Every admitted packet must be accounted by exactly one of ingress/headroom
                // allocation paths.
                constr_map.insert(named_constr(
                    constr_name, admit_[q][t][k] == (alloc_in_[q][t][k] || alloc_hdr_[q][t][k])));
            }

            if (t + 1 < total_time) {
                expr in_end = ingress_pool_occ_[q][t][micro_steps];
                expr in_total_end = ingress_pool_total_occ_[t][micro_steps];
                expr hdr_end = headroom_pool_occ_[q][t][micro_steps];
                // TODO(shared-buffer-switch): Parameterize per-ingress alpha. Current scaffold
                // uses alpha=1 so threshold equals remaining ingress-pool credits.
                expr gamma_q_end = net_ctx.int_val((int) ingress_pool_cap_cnt) - in_total_end;
                expr xoff_trig = in_end >= gamma_q_end;
                expr xon_trig = ingress_paused_[q][t] && (hdr_end == 0) && (in_end < gamma_q_end);
                expr paused_next = (ingress_paused_[q][t] || xoff_trig) && !xon_trig;

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_ingress_paused_handoff_q%u_t%u",
                         id.c_str(),
                         q,
                         t);
                // Pause state for the next slot is derived from the end-of-slot occupancies.
                constr_map.insert(named_constr(constr_name, ingress_paused_[q][t + 1] == paused_next));

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_ingress_pause_tx_evt_q%u_t%u",
                         id.c_str(),
                         q,
                         t);
                // Emit XOFF exactly on a resumed->paused transition.
                constr_map.insert(
                    named_constr(constr_name, inq->pause_tx(t) == (!ingress_paused_[q][t] && paused_next)));

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_ingress_resume_tx_evt_q%u_t%u",
                         id.c_str(),
                         q,
                         t);
                // Emit XON exactly on a paused->resumed transition.
                constr_map.insert(
                    named_constr(constr_name, inq->resume_tx(t) == (ingress_paused_[q][t] && !paused_next)));
            }
        }

        for (unsigned int e = 0; e < eg_cnt; e++) {
            SBQueue* out_sbq = egress_sb_queue(e);

            for (unsigned int k = 0; k < micro_steps; k++) {
                if (k == deq_k) {
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "%s_admit_e_false_e%u_t%u_k%u",
                             id.c_str(),
                             e,
                             t,
                             k);
                    // The dedicated dequeue micro-step does not admit new arrivals.
                    constr_map.insert(named_constr(constr_name, !admit_e_[e][t][k]));
                }
                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_alloc_eg_lossless_false_e%u_t%u_k%u",
                         id.c_str(),
                         e,
                         t,
                         k);
                constr_map.insert(named_constr(constr_name, !alloc_eg_lossless_[e][t][k]));
            }

            snprintf(constr_name, sizeof(constr_name), "%s_min_stamp_e_empty_e%u_t%u", id.c_str(), e, t);
            constr_map.insert(named_constr(constr_name, min_stamp_e_[e][t] == -1));
            snprintf(constr_name, sizeof(constr_name), "%s_has_pkt_e_false_e%u_t%u", id.c_str(), e, t);
            constr_map.insert(named_constr(constr_name, !has_pkt_e_[e][t]));

            for (unsigned int k = 0; k < k_states; k++) {
                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_egress_lossless_pool_zero_e%u_t%u_k%u",
                         id.c_str(),
                         e,
                         t,
                         k);
                constr_map.insert(named_constr(constr_name, egress_lossless_pool_occ_[e][t][k] == 0));
            }

            Queue* outq = out_queues[e];
            snprintf(constr_name, sizeof(constr_name), "%s_egress_read_sender_paused_e%u_t%u", id.c_str(), e, t);
            // Scaffold path: if the downstream-facing SBQueue reports paused, this egress cannot
            // perform any dequeue/send action in slot t.
            constr_map.insert(
                named_constr(constr_name, implies(out_sbq->sender_paused(t), outq->deq_cnt(t) == 0)));
            snprintf(constr_name, sizeof(constr_name), "%s_egress_deq_zero_e%u_t%u", id.c_str(), e, t);
            // Scaffold path currently emits no packet even when the egress-facing SBQueue is open.
            constr_map.insert(named_constr(constr_name, outq->deq_cnt(t) == 0));
            for (unsigned int i = 0; i < outq->max_enq(); i++) {
                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_output_null_e%u_i%u_t%u",
                         id.c_str(),
                         e,
                         i,
                         t);
                constr_map.insert(named_constr(constr_name, outq->enqs(i)[t] == net_ctx.null_pkt()));
            }
        }

        for (unsigned int k = 0; k < k_states; k++) {
            snprintf(
                constr_name, sizeof(constr_name), "%s_egress_lossless_pool_total_zero_t%u_k%u", id.c_str(), t, k);
            constr_map.insert(named_constr(constr_name, egress_lossless_pool_total_occ_[t][k] == 0));
        }
    }

    // Pool bounds and aggregate totals hold at every micro-state snapshot.
    for (unsigned int t = 0; t < total_time; t++) {
        for (unsigned int k = 0; k < k_states; k++) {
            expr ingress_sum = int0;
            expr headroom_sum = int0;

            for (unsigned int q = 0; q < in_cnt; q++) {
                ingress_sum = ingress_sum + ingress_pool_occ_[q][t][k];
                headroom_sum = headroom_sum + headroom_pool_occ_[q][t][k];

                snprintf(
                    constr_name, sizeof(constr_name), "%s_ingress_pool_lb_q%u_t%u_k%u", id.c_str(), q, t, k);
                constr_map.insert(named_constr(constr_name, ingress_pool_occ_[q][t][k] >= 0));
                snprintf(
                    constr_name, sizeof(constr_name), "%s_ingress_pool_ub_q%u_t%u_k%u", id.c_str(), q, t, k);
                constr_map.insert(named_constr(
                    constr_name, ingress_pool_occ_[q][t][k] <= (int) ingress_pool_cap_cnt));

                snprintf(
                    constr_name, sizeof(constr_name), "%s_headroom_pool_lb_q%u_t%u_k%u", id.c_str(), q, t, k);
                constr_map.insert(named_constr(constr_name, headroom_pool_occ_[q][t][k] >= 0));
                snprintf(
                    constr_name, sizeof(constr_name), "%s_headroom_pool_ub_q%u_t%u_k%u", id.c_str(), q, t, k);
                constr_map.insert(named_constr(
                    constr_name, headroom_pool_occ_[q][t][k] <= (int) headroom_pool_cap_cnt));
            }

            snprintf(
                constr_name, sizeof(constr_name), "%s_ingress_pool_total_sum_t%u_k%u", id.c_str(), t, k);
            constr_map.insert(
                named_constr(constr_name, ingress_pool_total_occ_[t][k] == ingress_sum));
            snprintf(
                constr_name, sizeof(constr_name), "%s_headroom_pool_total_sum_t%u_k%u", id.c_str(), t, k);
            constr_map.insert(
                named_constr(constr_name, headroom_pool_total_occ_[t][k] == headroom_sum));

            snprintf(
                constr_name, sizeof(constr_name), "%s_ingress_pool_total_lb_t%u_k%u", id.c_str(), t, k);
            constr_map.insert(named_constr(constr_name, ingress_pool_total_occ_[t][k] >= 0));
            snprintf(
                constr_name, sizeof(constr_name), "%s_ingress_pool_total_ub_t%u_k%u", id.c_str(), t, k);
            constr_map.insert(named_constr(
                constr_name, ingress_pool_total_occ_[t][k] <= (int) ingress_pool_cap_cnt));

            snprintf(
                constr_name, sizeof(constr_name), "%s_headroom_pool_total_lb_t%u_k%u", id.c_str(), t, k);
            constr_map.insert(named_constr(constr_name, headroom_pool_total_occ_[t][k] >= 0));
            snprintf(
                constr_name, sizeof(constr_name), "%s_headroom_pool_total_ub_t%u_k%u", id.c_str(), t, k);
            constr_map.insert(named_constr(
                constr_name, headroom_pool_total_occ_[t][k] <= (int) headroom_pool_cap_cnt));
        }
    }

    // Intra-slot pool-ledger updates mirror per-micro-step alloc/release events.
    for (unsigned int t = 0; t < total_time; t++) {
        for (unsigned int k = 0; k < micro_steps; k++) {
            for (unsigned int q = 0; q < in_cnt; q++) {
                expr release_in_delta = int0;
                expr release_hdr_delta = int0;
                if (k == deq_k) {
                    expr reads = reads_from_q(q, t, k);
                    release_hdr_delta = ite(reads <= headroom_pool_occ_[q][t][k],
                                            reads,
                                            headroom_pool_occ_[q][t][k]);
                    release_in_delta = reads - release_hdr_delta;
                }

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_ingress_pool_step_q%u_t%u_k%u",
                         id.c_str(),
                         q,
                         t,
                         k);
                constr_map.insert(named_constr(
                    constr_name,
                    ingress_pool_occ_[q][t][k + 1] ==
                        ingress_pool_occ_[q][t][k] + b2i(alloc_in_[q][t][k]) - release_in_delta));

                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_headroom_pool_step_q%u_t%u_k%u",
                         id.c_str(),
                         q,
                         t,
                         k);
                constr_map.insert(named_constr(
                    constr_name,
                    headroom_pool_occ_[q][t][k + 1] ==
                        headroom_pool_occ_[q][t][k] + b2i(alloc_hdr_[q][t][k]) - release_hdr_delta));
            }
        }
    }

    // End-of-slot pool ledgers seed the next slot's initial micro-state.
    for (unsigned int t = 0; t + 1 < total_time; t++) {
        for (unsigned int q = 0; q < in_cnt; q++) {
            snprintf(constr_name,
                     sizeof(constr_name),
                     "%s_ingress_pool_handoff_q%u_t%u",
                     id.c_str(),
                     q,
                     t);
            constr_map.insert(named_constr(
                constr_name, ingress_pool_occ_[q][t + 1][0] == ingress_pool_occ_[q][t][micro_steps]));

            snprintf(constr_name,
                     sizeof(constr_name),
                     "%s_headroom_pool_handoff_q%u_t%u",
                     id.c_str(),
                     q,
                     t);
            constr_map.insert(named_constr(
                constr_name, headroom_pool_occ_[q][t + 1][0] == headroom_pool_occ_[q][t][micro_steps]));
        }

        snprintf(
            constr_name, sizeof(constr_name), "%s_ingress_pool_total_handoff_t%u", id.c_str(), t);
        constr_map.insert(named_constr(
            constr_name, ingress_pool_total_occ_[t + 1][0] == ingress_pool_total_occ_[t][micro_steps]));
        snprintf(
            constr_name, sizeof(constr_name), "%s_headroom_pool_total_handoff_t%u", id.c_str(), t);
        constr_map.insert(named_constr(
            constr_name, headroom_pool_total_occ_[t + 1][0] == headroom_pool_total_occ_[t][micro_steps]));
    }

    for (unsigned int b = 0; b < slot_cnt; b++) {
        for (unsigned int t = 0; t < total_time; t++) {
            for (unsigned int k = 0; k < k_states; k++) {
                snprintf(constr_name, sizeof(constr_name), "%s_buf_pkt_null_b%u_t%u_k%u", id.c_str(), b, t, k);
                constr_map.insert(named_constr(constr_name, buf_pkt_[b][t][k] == net_ctx.null_pkt()));
                snprintf(constr_name, sizeof(constr_name), "%s_buf_in_empty_b%u_t%u_k%u", id.c_str(), b, t, k);
                constr_map.insert(named_constr(constr_name, buf_in_[b][t][k] == -1));
                snprintf(constr_name, sizeof(constr_name), "%s_buf_e_empty_b%u_t%u_k%u", id.c_str(), b, t, k);
                constr_map.insert(named_constr(constr_name, buf_e_[b][t][k] == -1));
                snprintf(constr_name, sizeof(constr_name), "%s_buf_is_hdr_false_b%u_t%u_k%u", id.c_str(), b, t, k);
                constr_map.insert(named_constr(constr_name, !buf_is_hdr_[b][t][k]));
                snprintf(constr_name, sizeof(constr_name), "%s_buf_stamp_empty_b%u_t%u_k%u", id.c_str(), b, t, k);
                constr_map.insert(named_constr(constr_name, buf_stamp_[b][t][k] == -1));
            }
        }
    }

    for (unsigned int q = 0; q < in_cnt; q++) {
        for (unsigned int b = 0; b < slot_cnt; b++) {
            for (unsigned int t = 0; t < total_time; t++) {
                for (unsigned int k = 0; k < micro_steps; k++) {
                    snprintf(constr_name,
                             sizeof(constr_name),
                             "%s_write_slot_false_q%u_b%u_t%u_k%u",
                             id.c_str(),
                             q,
                             b,
                             t,
                             k);
                    constr_map.insert(named_constr(constr_name, !write_slot_[q][b][t][k]));
                }
            }
        }
    }

    for (unsigned int b = 0; b < slot_cnt; b++) {
        for (unsigned int t = 0; t < total_time; t++) {
            for (unsigned int k = 0; k < micro_steps; k++) {
                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_read_slot_false_b%u_t%u_k%u",
                         id.c_str(),
                         b,
                         t,
                         k);
                constr_map.insert(named_constr(constr_name, !read_slot_[b][t][k]));
            }
        }
    }
}
