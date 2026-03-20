#ifndef cbd_blocked_edge_hpp
#define cbd_blocked_edge_hpp

#include "metric.hpp"

class CBDBlockedEdge : public Metric {
public:
    CBDBlockedEdge(Queue* queue,
                   unsigned int total_time,
                   const std::vector<expr>& active_exprs,
                   NetContext& net_ctx);

    void populate_val_exprs(NetContext& net_ctx) override;
    void eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) override;

private:
    std::vector<expr> active_exprs_;
};

#endif
