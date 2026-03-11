#include "sender_paused.hpp"

SenderPaused::SenderPaused(Queue* queue,
                           unsigned int total_time,
                           const vector<expr>& paused_exprs,
                           NetContext& net_ctx):
Metric(metric_t::SENDER_PAUSED, queue, total_time, net_ctx),
paused_exprs_(paused_exprs) {
    init(net_ctx);
}

void SenderPaused::populate_val_exprs(NetContext& net_ctx) {
    for (unsigned int t = 0; t < total_time; t++) {
        valid_[t] = net_ctx.bool_val(true);
        value_[t] = z3::ite(paused_exprs_[t], net_ctx.int_val(1), net_ctx.int_val(0));
    }
}

void SenderPaused::eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) {
    (void)eg;
    (void)time;
    (void)qind;
    res.valid = false;
}
