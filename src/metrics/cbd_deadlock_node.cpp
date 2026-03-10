#include "cbd_deadlock_node.hpp"

CBDDeadlockNode::CBDDeadlockNode(Queue* queue,
                                 unsigned int total_time,
                                 const std::vector<expr>& blocked_exprs,
                                 NetContext& net_ctx):
Metric(metric_t::CBD_DEADLOCK_NODE, queue, total_time, net_ctx),
blocked_exprs_(blocked_exprs) {
    init(net_ctx);
}

void CBDDeadlockNode::populate_val_exprs(NetContext& net_ctx) {
    for (unsigned int t = 0; t < total_time; t++) {
        valid_[t] = net_ctx.bool_val(true);
        value_[t] = z3::ite(blocked_exprs_[t], net_ctx.int_val(1), net_ctx.int_val(0));
    }
}

void CBDDeadlockNode::eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) {
    (void)eg;
    (void)time;
    (void)qind;
    res.valid = false;
}
