#include "enq.hpp"

Enq::Enq(Queue* queue, unsigned int total_time, NetContext& net_ctx):
Metric(metric_t::ENQ, queue, total_time, net_ctx) {
    init(net_ctx);
}

void Enq::populate_val_exprs(NetContext& net_ctx) {
    for (unsigned int t = 0; t < total_time; t++) {
        valid_[t] = net_ctx.bool_val(true);
        value_[t] = queue->enq_cnt(t);
    }
}

void Enq::eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) {
    res.valid = true;
    res.value = eg->enqs[qind][time];
}
