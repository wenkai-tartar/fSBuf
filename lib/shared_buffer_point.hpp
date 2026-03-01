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
    unsigned int max_demand_per_t = 1;
};

class SharedBufferPoint : public ContentionPoint {
public:
    SharedBufferPoint(unsigned int total_time, SharedBufferPointInfo info);

protected:
    void add_in_queue_constrs() override;
    void add_out_queue_constrs() override;
    m_val_expr_t metric_val_expr(metric_t metric, unsigned int q, unsigned t) override;

    SharedBufferPointInfo sb_info;
    bool shared_vars_initialized_ = false;

    vector<vector<expr>> buf_pkt_;
    vector<vector<expr>> buf_ing_;
    vector<vector<expr>> buf_eg_;
    vector<expr> buf_used_;
    vector<vector<expr>> ingress_pool_pos_;
    vector<expr> ingress_pool_used_;
    vector<vector<expr>> headroom_pool_pos_;
    vector<expr> headroom_pool_used_;
    vector<vector<expr>> egress_lossless_pool_pos_;
    vector<expr> egress_lossless_pool_used_;

    vector<vector<expr>> admit_in_; // [q][t] bool
    vector<vector<expr>> admit_e_; // [e][t] bool
    vector<vector<expr>> admit_; // [q][t]
    vector<vector<expr>> drop_or_block_; // [q][t]
    vector<vector<expr>> alloc_ing_; // [q][t]
    vector<vector<expr>> release_ing_;
    vector<vector<expr>> alloc_hdr_; // [q][t]
    vector<vector<expr>> release_hdr_;
    vector<vector<expr>> alloc_eg_lossless_; // [q][t]
    vector<vector<expr>> release_eg_lossless_;
};

#endif /* shared_buffer_point_hpp */
