//  shared_buffer_point.cpp
//  FSB
//
//  Created by Wenkai Li on 2/27/26.
//  Copyright © 2026 Wenkai Li. All rights reserved.
//

#include "shared_buffer_point.hpp"

#include <cstdio>
#include <stdexcept>

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
