//  demand.cpp
//  FSB
//
//  Created by Wenkai Li on 2/28/26.
//  Copyright © 2026 Wenkai Li. All rights reserved.
//

#include "demand.hpp"

Demand::Demand(Queue* queue, unsigned int total_time, NetContext& net_ctx):
Metric(metric_t::DEMAND, queue, total_time, net_ctx) {
    init(net_ctx);
}

expr& Demand::demand_cnt(unsigned int t) {
    return demand_cnt_[t];
}

const expr& Demand::demand_cnt(unsigned int t) const {
    return demand_cnt_[t];
}

void Demand::eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) {
    res.valid = true;
    res.value = 0;
    for (unsigned int t = 0; t <= time; t++) {
        res.value += eg->demands[qind][t];
    }
}

void Demand::populate_val_exprs(NetContext& net_ctx) {
    // Add SMT vars
    demand_cnt_.clear();
    for (unsigned int t = 0; t < total_time; t++) {
        char vname[100];
        snprintf(vname, 100, "%s_demand_cnt[%d]", id.c_str(), t);
        demand_cnt_.push_back(net_ctx.int_const(vname));
    }

    // value
    value_[0] = demand_cnt_[0];
    for (unsigned int t = 1; t < total_time; t++) {
        value_[t] = value_[t-1] + demand_cnt_[t];
    }

    // valid
    for (unsigned int t = 0; t < total_time; t++) {
        valid_[t] = net_ctx.bool_val(true);
    }
}