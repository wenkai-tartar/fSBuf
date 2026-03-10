#ifndef cbd_deadlock_node_hpp
#define cbd_deadlock_node_hpp

#include "metric.hpp"

class CBDDeadlockNode : public Metric {
public:
    CBDDeadlockNode(Queue* queue,
                    unsigned int total_time,
                    const std::vector<expr>& blocked_exprs,
                    NetContext& net_ctx);

    void populate_val_exprs(NetContext& net_ctx) override;
    void eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) override;

private:
    std::vector<expr> blocked_exprs_;
};

#endif
