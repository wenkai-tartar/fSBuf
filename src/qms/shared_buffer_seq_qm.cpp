//
//  shared_buffer_seq_qm.cpp
//  FSB
//
//  Created by Wenkai Li on 3/5/26.
//

#include "shared_buffer_seq_qm.hpp"

#include <cstdio>

SharedBufferSeqQM::SharedBufferSeqQM(cid_t id,
                                     unsigned int total_time,
                                     vector<QueueInfo> in_queue_info,
                                     vector<QueueInfo> out_queue_info,
                                     NetContext& net_ctx):
QueuingModule(id, total_time, in_queue_info, out_queue_info, net_ctx) {
    init(net_ctx);
}

void SharedBufferSeqQM::add_proc_vars(NetContext& net_ctx) {
    const unsigned int in_cnt = (unsigned int) in_queues.size();
    ingress_active_.clear();
    ingress_active_.resize(in_cnt);

    char vname[120];
    for (unsigned int q = 0; q < in_cnt; q++) {
        ingress_active_[q].resize(total_time);
        for (unsigned int t = 0; t < total_time; t++) {
            for (unsigned int k = 0; k < in_cnt; k++) {
                snprintf(
                    vname, sizeof(vname), "%s_ingress_active[%u][%u][%u]", id.c_str(), q, t, k);
                ingress_active_[q][t].push_back(net_ctx.bool_const(vname));
            }
        }
    }
}

void SharedBufferSeqQM::add_constrs(NetContext& net_ctx, map<string, expr>& constr_map) {
    const unsigned int in_cnt = (unsigned int) in_queues.size();
    char constr_name[140];

    for (unsigned int t = 0; t < total_time; t++) {
        for (unsigned int k = 0; k < in_cnt; k++) {
            expr sum = net_ctx.int_val(0);
            for (unsigned int q = 0; q < in_cnt; q++) {
                const bool is_expected = (q == k);
                snprintf(constr_name,
                         sizeof(constr_name),
                         "%s_seq_order_q%u_t%u_k%u",
                         id.c_str(),
                         q,
                         t,
                         k);
                // Deterministic schedule: ingress micro-step k always selects queue k.
                constr_map.insert(
                    named_constr(constr_name,
                                 ingress_active_[q][t][k] == net_ctx.bool_val(is_expected)));
                sum = sum + ite(ingress_active_[q][t][k], net_ctx.int_val(1), net_ctx.int_val(0));
            }

            snprintf(constr_name, sizeof(constr_name), "%s_seq_onehot_t%u_k%u", id.c_str(), t, k);
            // One-hot sanity check for each ingress micro-step.
            constr_map.insert(named_constr(constr_name, sum == 1));
        }
    }
}

const expr&
SharedBufferSeqQM::ingress_active(unsigned int q, unsigned int t, unsigned int k) const {
    return ingress_active_[q][t][k];
}

unsigned int SharedBufferSeqQM::ingress_step_cnt() const {
    return (unsigned int) in_queues.size();
}
