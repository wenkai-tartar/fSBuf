#include "cbd_wait_edge.hpp"

CBDWaitEdge::CBDWaitEdge(Queue* queue,
                         unsigned int total_time,
                         const std::vector<expr>& active_exprs,
                         NetContext& net_ctx):
Metric(metric_t::CBD_WAIT_EDGE, queue, total_time, net_ctx),
active_exprs_(active_exprs) {
    init(net_ctx);
}

void CBDWaitEdge::populate_val_exprs(NetContext& net_ctx) {
    for (unsigned int t = 0; t < total_time; t++) {
        valid_[t] = net_ctx.bool_val(true);
        value_[t] = z3::ite(active_exprs_[t], net_ctx.int_val(1), net_ctx.int_val(0));
    }
}

void CBDWaitEdge::eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) {
    (void)eg;
    (void)time;
    (void)qind;
    res.valid = false;
}
