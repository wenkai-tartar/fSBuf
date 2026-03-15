//
//  enq.hpp
//  FPerf
//
//  Created by OpenAI Codex on 3/14/26.
//

#ifndef enq_hpp
#define enq_hpp

#include "metric.hpp"
#include "queue.hpp"

class Enq : public Metric {
public:
    Enq(Queue* queue, unsigned int total_time, NetContext& net_ctx);

    void populate_val_exprs(NetContext& net_ctx) override;

    void eval(const IndexedExample* eg, unsigned int time, unsigned int qind, metric_val& res) override;
};

#endif /* enq_hpp */
