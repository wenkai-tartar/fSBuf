//  cdemand.hpp
//  FSB
//
//  Created by Wenkai Li on 3/15/26.
//

#ifndef cdemand_hpp
#define cdemand_hpp

#include "demand.hpp"

class CDemand : public Metric {
public:
    CDemand(Queue* queue, Demand* demand, unsigned int total_time, NetContext& net_ctx);

    void populate_val_exprs(NetContext& net_ctx) override;

    void
    eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) override;

private:
    Demand* demand_;
};

#endif /* cdemand_hpp */
