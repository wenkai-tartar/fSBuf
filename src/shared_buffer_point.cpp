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

void SharedBufferPoint::add_in_queue_constrs() {
    ContentionPoint::add_in_queue_constrs();

    const unsigned int in_cnt = (unsigned int) in_queues.size();
    const unsigned int eg_cnt = (unsigned int) out_queues.size();
    const unsigned int slot_cnt = sb_info.buffer_slot_cnt;

    if (in_cnt == 0 || eg_cnt == 0) return;

    // Initialize admission vars
    if (!shared_vars_initialized_) {
        admit_in_.clear();
        admit_e_.clear();
        admit_.clear();
        buf_used_.clear();
        write_slot_.clear();
        read_slot_.clear();

        admit_in_.resize(in_cnt);
        admit_e_.resize(eg_cnt);
        admit_.resize(in_cnt);
        buf_used_.resize(slot_cnt);
        write_slot_.resize(in_cnt);
        read_slot_.resize(eg_cnt);

        char vname[100];
        // Shared-buffer slot occupancy vars
        for (unsigned int b = 0; b < slot_cnt; b++) {
            for (unsigned int t = 0; t < total_time; t++) {
                snprintf(vname, sizeof(vname), "sb_buf_used[%u][%u]", b, t);
                buf_used_[b].push_back(net_ctx.bool_const(vname));
            }
        }

        // Ingress queue vars
        for (unsigned int q = 0; q < in_cnt; q++) {
            write_slot_[q].resize(slot_cnt);
            for (unsigned int t = 0; t < total_time; t++) {
                snprintf(vname, sizeof(vname), "sb_admit_in[%u][%u]", q, t);
                admit_in_[q].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "sb_admit[%u][%u]", q, t);
                admit_[q].push_back(net_ctx.bool_const(vname));
            }

            for (unsigned int b = 0; b < slot_cnt; b++) {
                for (unsigned int t = 0; t < total_time; t++) {
                    snprintf(vname, sizeof(vname), "sb_write_slot[%u][%u][%u]", q, b, t);
                    write_slot_[q][b].push_back(net_ctx.bool_const(vname));
                }
            }
        }

        // Egress queue vars
        for (unsigned int e = 0; e < eg_cnt; e++) {
            read_slot_[e].resize(slot_cnt);
            for (unsigned int t = 0; t < total_time; t++) {
                snprintf(vname, sizeof(vname), "sb_admit_e[%u][%u]", e, t);
                admit_e_[e].push_back(net_ctx.bool_const(vname));
            }
            for (unsigned int b = 0; b < slot_cnt; b++) {
                for (unsigned int t = 0; t < total_time; t++) {
                    snprintf(vname, sizeof(vname), "sb_read_slot[%u][%u][%u]", e, b, t);
                    read_slot_[e][b].push_back(net_ctx.bool_const(vname));
                }
            }
        }

        shared_vars_initialized_ = true;
    }

    char constr_name[100];
    map<string, expr> new_constr_map;
    for (unsigned int q = 0; q < in_cnt; q++) {
        Queue* inq = in_queues[q];
        for (unsigned int t = 0; t < total_time; t++) {
            expr dst = net_ctx.pkt2meta1(inq->enqs(0)[t]);

            expr egress_cnt = net_ctx.int_val((int) eg_cnt);
            expr dst_eg = dst % egress_cnt;

            // Admission is route-aware: for the egress selected by dst_eg, admit is true
            // iff both ingress-side and egress-side admission gates are true.
            expr_vector route_cases(net_ctx.z3_ctx());
            for (unsigned int e = 0; e < eg_cnt; e++) {
                expr route_to_e = dst_eg == net_ctx.int_val((int) e);
                expr admit_def = admit_[q][t] == (admit_in_[q][t] && admit_e_[e][t]);
                route_cases.push_back(implies(route_to_e, admit_def));
            }

            snprintf(constr_name, sizeof(constr_name), "sb_admit_def_q%u_t%u", q, t);
            new_constr_map.insert(named_constr(constr_name, mk_and(route_cases)));

            if (slot_cnt > 0) {
                // Per (q,t): admitted traffic writes exactly one slot; non-admitted traffic writes none.
                // Also enforce write_slot -> admit to prevent writes without admission.
                expr writes_this_q_t_sum = net_ctx.int_val(0);
                for (unsigned int b = 0; b < slot_cnt; b++) {
                    writes_this_q_t_sum =
                        writes_this_q_t_sum + ite(write_slot_[q][b][t],
                                                  net_ctx.int_val(1),
                                                  net_ctx.int_val(0));

                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_write_implies_admit_q%u_b%u_t%u",
                             q,
                             b,
                             t);
                    new_constr_map.insert(
                        named_constr(constr_name, implies(write_slot_[q][b][t], admit_[q][t])));

                    snprintf(constr_name,
                             sizeof(constr_name),
                             "sb_write_requires_empty_q%u_b%u_t%u",
                             q,
                             b,
                             t);
                    // TODO(shared-buffer): once buf_pkt_ init/transition constraints are implemented,
                    // switch this guard to !net_ctx.pkt2val(buf_pkt_[b][t]).
                    new_constr_map.insert(
                        named_constr(constr_name, implies(write_slot_[q][b][t], !buf_used_[b][t])));
                }

                snprintf(constr_name, sizeof(constr_name), "sb_admit_write_eq_q%u_t%u", q, t);
                new_constr_map.insert(named_constr(
                    constr_name,
                    writes_this_q_t_sum ==
                        ite(admit_[q][t], net_ctx.int_val(1), net_ctx.int_val(0))));
            }
        }
    }

    if (slot_cnt > 0) {
        for (unsigned int b = 0; b < slot_cnt; b++) {
            for (unsigned int t = 0; t < total_time; t++) {
                // Per (b,t): at most one ingress queue can write to the same physical slot.
                expr slot_writers_sum = net_ctx.int_val(0);
                for (unsigned int q = 0; q < in_cnt; q++) {
                    slot_writers_sum = slot_writers_sum + ite(write_slot_[q][b][t],
                                                              net_ctx.int_val(1),
                                                              net_ctx.int_val(0));
                }
                snprintf(constr_name, sizeof(constr_name), "sb_slot_single_writer_b%u_t%u", b, t);
                new_constr_map.insert(named_constr(constr_name, slot_writers_sum <= 1));
            }
        }
    }

    add_constr_from_map(new_constr_map);
}

void SharedBufferPoint::add_out_queue_constrs() {
    ContentionPoint::add_out_queue_constrs();

    const unsigned int eg_cnt = (unsigned int) out_queues.size();
    const unsigned int slot_cnt = sb_info.buffer_slot_cnt;
    if (eg_cnt == 0 || slot_cnt == 0) return;

    char constr_name[100];
    map<string, expr> new_constr_map;

    // For strict egress service, each egress reads exactly deq_cnt(e,t) slots at time t.
    // With current queue config (max_deq=1), this enforces a 0/1 read action per egress per slot.
    for (unsigned int e = 0; e < eg_cnt; e++) {
        Queue* outq = out_queues[e];
        for (unsigned int t = 0; t < total_time; t++) {
            expr reads_this_e_t_sum = net_ctx.int_val(0);
            for (unsigned int b = 0; b < slot_cnt; b++) {
                snprintf(constr_name,
                         sizeof(constr_name),
                         "sb_read_requires_nonempty_e%u_b%u_t%u",
                         e,
                         b,
                         t);
                // TODO(shared-buffer): once buf_pkt_ init/transition constraints are implemented,
                // switch this guard to net_ctx.pkt2val(buf_pkt_[b][t]).
                new_constr_map.insert(
                    named_constr(constr_name, implies(read_slot_[e][b][t], buf_used_[b][t])));

                reads_this_e_t_sum = reads_this_e_t_sum + ite(read_slot_[e][b][t],
                                                              net_ctx.int_val(1),
                                                              net_ctx.int_val(0));
            }
            snprintf(constr_name, sizeof(constr_name), "sb_read_deq_eq_e%u_t%u", e, t);
            new_constr_map.insert(named_constr(constr_name, reads_this_e_t_sum == outq->deq_cnt(t)));
        }
    }

    // A physical slot can be consumed by at most one egress in the same time step.
    for (unsigned int b = 0; b < slot_cnt; b++) {
        for (unsigned int t = 0; t < total_time; t++) {
            expr slot_readers_sum = net_ctx.int_val(0);
            for (unsigned int e = 0; e < eg_cnt; e++) {
                slot_readers_sum = slot_readers_sum + ite(read_slot_[e][b][t],
                                                          net_ctx.int_val(1),
                                                          net_ctx.int_val(0));
            }
            snprintf(constr_name, sizeof(constr_name), "sb_slot_single_reader_b%u_t%u", b, t);
            new_constr_map.insert(named_constr(constr_name, slot_readers_sum <= 1));
        }
    }

    add_constr_from_map(new_constr_map);
}

m_val_expr_t SharedBufferPoint::metric_val_expr(metric_t metric, unsigned int q, unsigned t) {
    return ContentionPoint::metric_val_expr(metric, q, t);
}
