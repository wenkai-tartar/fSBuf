//
//  pfc_blocked.cpp
//  FSB
//
//  Created by Wenkai Li on 3/10/26.
//

#include "pfc_blocked.hpp"

PFCBlocked::PFCBlocked(Queue* queue,
                       unsigned int total_time,
                       const vector<expr>& blocked_exprs,
                       NetContext& net_ctx):
Metric(metric_t::PFC_BLOCKED, queue, total_time, net_ctx),
blocked_exprs_(blocked_exprs) {
    init(net_ctx);
}

void PFCBlocked::eval(const IndexedExample* eg,
                      unsigned int time,
                      unsigned int qind,
                      metric_val& res) {
    (void) eg;
    (void) time;
    (void) qind;
    // This metric is derived from shared-buffer internal state that is not serialized into
    // IndexedExample today. Symbolic queries use populate_val_exprs below.
    res.valid = false;
    res.value = 0;
}

void PFCBlocked::populate_val_exprs(NetContext& net_ctx) {
    for (unsigned int t = 0; t < total_time; t++) {
        if (t < blocked_exprs_.size()) {
            value_[t] = ite(blocked_exprs_[t], net_ctx.int_val(1), net_ctx.int_val(0));
        } else {
            value_[t] = net_ctx.int_val(0);
        }
        valid_[t] = net_ctx.bool_val(true);
    }
}
