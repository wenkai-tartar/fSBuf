//  demand.hpp
//  FSB
//
//  Created by Wenkai Li on 2/27/26.
//  Copyright © 2026 Wenkai Li. All rights reserved.
//

#ifndef demand_hpp
#define demand_hpp

#include "metric.hpp"
#include "shared_buffer_point.hpp"

class Demand : public Metric {
public:
    Demand(Queue* queue, unsigned int total_time, NetContext& net_ctx);

    expr& demand_cnt(unsigned int t);
    const expr& demand_cnt(unsigned int t) const;

    void populate_val_exprs(NetContext& net_ctx) override;

    void
    eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) override;

private:
    vector<expr> demand_cnt_;
};

#endif /* demand_hpp */
