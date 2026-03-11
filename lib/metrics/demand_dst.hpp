//
//  demand_dst.hpp
//  FSB
//

#ifndef demand_dst_hpp
#define demand_dst_hpp

#include "metric.hpp"

class DemandDst : public Metric {
public:
    DemandDst(Queue* queue, unsigned int total_time, NetContext& net_ctx);

    expr& demand_dst(unsigned int t);
    const expr& demand_dst(unsigned int t) const;

    void populate_val_exprs(NetContext& net_ctx) override;

    void
    eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) override;

private:
    vector<expr> demand_dst_;
};

#endif /* demand_dst_hpp */
