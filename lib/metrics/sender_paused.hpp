#ifndef sender_paused_hpp
#define sender_paused_hpp

#include "metric.hpp"

class SenderPaused : public Metric {
public:
    SenderPaused(Queue* queue,
                 unsigned int total_time,
                 const vector<expr>& paused_exprs,
                 NetContext& net_ctx);

    void populate_val_exprs(NetContext& net_ctx) override;
    void
    eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) override;

private:
    vector<expr> paused_exprs_;
};

#endif
