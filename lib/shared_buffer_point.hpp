//  shared_buffer_point.hpp
//  FSB
//
//  Created by Wenkai Li on 2/27/26.
//  Copyright © 2026 Wenkai Li. All rights reserved.
//

#ifndef shared_buffer_point_hpp
#define shared_buffer_point_hpp

#include "contention_point.hpp"

using namespace std;

struct SharedBufferPointInfo {
    enum class TimingMode {
        DEQ_FIRST = 0,
        ENQ_FIRST = 1,
    };

    unsigned int max_demand_per_t = 1;
    // Per-ingress serialization factor S_q (in fastest-timeslot units).
    // S_q = 1 means can inject every slot; S_q = 2 means at most once every 2 slots, etc.
    // If empty or out-of-range for queue q, default to 1.
    vector<unsigned int> ingress_ser_factor;
    // Per-egress serialization factor S_e (in fastest-timeslot units).
    // S_e = 1 means can dequeue every slot; S_e = 2 means at most once every 2 slots, etc.
    // If empty or out-of-range for egress e, default to 1.
    vector<unsigned int> egress_ser_factor;
    // If true, boundary-egress pause/resume inputs are constrained to false. If false,
    // they remain unconstrained environment inputs.
    bool boundary_egress_ctrl_default_false = false;
    unsigned int buffer_slot_cnt = MAX_QUEUE_SIZE;
    // Pool capacities. If 0, default to buffer_slot_cnt.
    unsigned int ingress_pool_size = 0;
    unsigned int headroom_pool_size = 0;
    unsigned int egress_lossless_pool_size = 0;
    TimingMode timing_mode = TimingMode::DEQ_FIRST;
};

class SharedBufferPoint : public ContentionPoint {
public:
    SharedBufferPoint(unsigned int total_time, SharedBufferPointInfo info);

protected:
    void add_in_queue_constrs() override;
    void add_out_queue_constrs() override;
    m_val_expr_t metric_val_expr(metric_t metric, unsigned int q, unsigned t) override;

    unsigned int ingress_serialization_factor(unsigned int q) const;

    SharedBufferPointInfo sb_info;
};

#endif /* shared_buffer_point_hpp */
