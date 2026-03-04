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

    SharedBufferPointInfo sb_info;
    bool shared_vars_initialized_ = false;

    // Per-slot identity state at micro-step granularity [b][t][k].
    vector<vector<vector<expr>>> buf_pkt_; // [b][t][k] pkt
    vector<vector<vector<expr>>> buf_in_; // [b][t][k] int
    vector<vector<vector<expr>>> buf_e_; // [b][t][k] int
    // k indexes micro-state snapshots within a coarse slot t.
    vector<vector<vector<expr>>> ingress_pool_occ_; // [q][t][k] int
    vector<vector<expr>> ingress_pool_total_occ_; // [t][k] int
    vector<vector<vector<expr>>> headroom_pool_occ_; // [q][t][k] int
    vector<vector<expr>> headroom_pool_total_occ_; // [t][k] int
    vector<vector<vector<expr>>> egress_lossless_pool_occ_; // [e][t][k] int
    vector<vector<expr>> egress_lossless_pool_total_occ_; // [t][k] int

    // Action variables upgraded to micro-step granularity [..][t][k].
    vector<vector<vector<expr>>> admit_in_; // [q][t][k] bool
    vector<vector<vector<expr>>> admit_e_; // [e][t][k] bool
    vector<vector<vector<expr>>> admit_; // [q][t][k] bool
    // vector<vector<vector<expr>>> drop_or_block_; // [q][t][k] bool
    vector<vector<vector<expr>>> alloc_ing_; // [q][t][k] bool
    vector<vector<vector<expr>>> release_ing_; // [q][t][k] bool
    vector<vector<vector<expr>>> alloc_hdr_; // [q][t][k] bool
    vector<vector<vector<expr>>> release_hdr_; // [q][t][k] bool
    vector<vector<vector<expr>>> alloc_eg_lossless_; // [e][t][k] bool
    vector<vector<vector<expr>>> release_eg_lossless_; // [e][t][k] bool
    vector<vector<vector<vector<expr>>>> write_slot_; // [q][b][t][k] bool
    vector<vector<vector<vector<expr>>>> read_slot_; // [e][b][t][k] bool

    unsigned int micro_step_cnt() const;
    unsigned int deq_step_idx() const;
    int ingress_queue_for_step(unsigned int k) const;
    unsigned int ingress_pool_cap() const;
    unsigned int headroom_pool_cap() const;
    unsigned int egress_lossless_pool_cap() const;

    void init_shared_vars();
    void add_micro_step_constrs(map<string, expr>& new_constr_map);
    void add_cross_time_step_constrs(map<string, expr>& new_constr_map);
};

#endif /* shared_buffer_point_hpp */
