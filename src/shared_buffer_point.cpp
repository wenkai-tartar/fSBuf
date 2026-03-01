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

    if (in_cnt == 0 || eg_cnt == 0) return;

    // Initialize admission vars
    if (!shared_vars_initialized_) {
        admit_in_.clear();
        admit_e_.clear();
        admit_.clear();

        admit_in_.resize(in_cnt);
        admit_e_.resize(eg_cnt);
        admit_.resize(in_cnt);

        char vname[100];
        for (unsigned int q = 0; q < in_cnt; q++) {
            for (unsigned int t = 0; t < total_time; t++) {
                snprintf(vname, sizeof(vname), "sb_admit_in[%u][%u]", q, t);
                admit_in_[q].push_back(net_ctx.bool_const(vname));
                snprintf(vname, sizeof(vname), "sb_admit[%u][%u]", q, t);
                admit_[q].push_back(net_ctx.bool_const(vname));
            }
        }

        for (unsigned int e = 0; e < eg_cnt; e++) {
            for (unsigned int t = 0; t < total_time; t++) {
                snprintf(vname, sizeof(vname), "sb_admit_e[%u][%u]", e, t);
                admit_e_[e].push_back(net_ctx.bool_const(vname));
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

            expr_vector route_cases(net_ctx.z3_ctx());
            for (unsigned int e = 0; e < eg_cnt; e++) {
                expr route_to_e = dst_eg == net_ctx.int_val((int) e);
                expr admit_def = admit_[q][t] == (admit_in_[q][t] && admit_e_[e][t]);
                route_cases.push_back(implies(route_to_e, admit_def));
            }

            snprintf(constr_name, sizeof(constr_name), "sb_admit_def_q%u_t%u", q, t);
            new_constr_map.insert(named_constr(constr_name, mk_and(route_cases)));
        }
    }

    add_constr_from_map(new_constr_map);
}

void SharedBufferPoint::add_out_queue_constrs() {
    ContentionPoint::add_out_queue_constrs();
}

m_val_expr_t SharedBufferPoint::metric_val_expr(metric_t metric, unsigned int q, unsigned t) {
    return ContentionPoint::metric_val_expr(metric, q, t);
}
