//
//  pfc_blocked.hpp
//  FSB
//
//  Created by Wenkai Li on 3/10/26.
//

#ifndef pfc_blocked_hpp
#define pfc_blocked_hpp

#include "metric.hpp"

class PFCBlocked : public Metric {
public:
    PFCBlocked(Queue* queue,
               unsigned int total_time,
               const vector<expr>& blocked_exprs,
               NetContext& net_ctx);

    void eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) override;
    void populate_val_exprs(NetContext& net_ctx) override;

private:
    vector<expr> blocked_exprs_;
};

#endif /* pfc_blocked_hpp */
