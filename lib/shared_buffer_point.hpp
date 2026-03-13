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

struct SharedBufferEnvInfo {
    unsigned int max_demand_per_t = 1;
    // If true, boundary-egress pause/resume inputs are constrained to false. If false,
    // they remain unconstrained environment inputs.
    bool boundary_egress_ctrl_default_false = false;
};

class SharedBufferPoint : public ContentionPoint {
public:
    SharedBufferPoint(unsigned int total_time, SharedBufferEnvInfo info);
    solver_res_t satisfy_query_with_model(string& model_str);

protected:
    void add_in_queue_constrs() override;
    void add_out_queue_constrs() override;
    m_val_expr_t metric_val_expr(metric_t metric, unsigned int q, unsigned t) override;

    SharedBufferEnvInfo env_info;
};

#endif /* shared_buffer_point_hpp */
