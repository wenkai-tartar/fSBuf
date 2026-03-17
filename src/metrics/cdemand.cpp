//  cdemand.cpp
//  FSB
//
//  Created by Wenkai Li on 3/15/26.
//

#include "cdemand.hpp"

CDemand::CDemand(Queue* queue, Demand* demand, unsigned int total_time, NetContext& net_ctx):
Metric(metric_t::CDEMAND, queue, total_time, net_ctx),
demand_(demand) {
    init(net_ctx);
}

void CDemand::eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) {
    res.valid = true;
    res.value = 0;
    for (unsigned int t = 0; t <= time; t++) {
        res.value += eg->demands[qind][t];
    }
}

void CDemand::populate_val_exprs(NetContext& net_ctx) {
    value_[0] = demand_->demand_cnt(0);
    for (unsigned int t = 1; t < total_time; t++) {
        value_[t] = value_[t - 1] + demand_->demand_cnt(t);
    }

    for (unsigned int t = 0; t < total_time; t++) {
        valid_[t] = net_ctx.bool_val(true);
    }
}
