//
//  demand_dst.cpp
//  FSB
//

#include "demand_dst.hpp"

DemandDst::DemandDst(Queue* queue, unsigned int total_time, NetContext& net_ctx):
Metric(metric_t::DEMAND_DST, queue, total_time, net_ctx) {
    init(net_ctx);
}

expr& DemandDst::demand_dst(unsigned int t) {
    return demand_dst_[t];
}

const expr& DemandDst::demand_dst(unsigned int t) const {
    return demand_dst_[t];
}

void DemandDst::eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) {
    (void)eg;
    (void)time;
    (void)qind;
    res.valid = false;
}

void DemandDst::populate_val_exprs(NetContext& net_ctx) {
    demand_dst_.clear();
    for (unsigned int t = 0; t < total_time; t++) {
        char vname[100];
        snprintf(vname, sizeof(vname), "%s_demand_dst[%u]", id.c_str(), t);
        demand_dst_.push_back(net_ctx.int_const(vname));
    }

    for (unsigned int t = 0; t < total_time; t++) {
        value_[t] = demand_dst_[t];
        valid_[t] = net_ctx.bool_val(true);
    }
}
