//
//  shared_buffer_seq_qm.hpp
//  FSB
//
//  Created by Wenkai Li on 3/5/26.
//

#ifndef shared_buffer_seq_qm_hpp
#define shared_buffer_seq_qm_hpp

#include "queuing_module.hpp"

class SharedBufferSeqQM : public QueuingModule {
public:
    SharedBufferSeqQM(cid_t id,
                      unsigned int total_time,
                      vector<QueueInfo> in_queue_info,
                      vector<QueueInfo> out_queue_info,
                      NetContext& net_ctx);

    void add_constrs(NetContext& net_ctx, map<string, expr>& constr_map) override;

    // [q][t][k]: queue q is scheduled at ingress micro-step k of slot t.
    const expr& ingress_active(unsigned int q, unsigned int t, unsigned int k) const;
    unsigned int ingress_step_cnt() const;

private:
    void add_proc_vars(NetContext& net_ctx) override;

    vector<vector<vector<expr>>> ingress_active_;
};

#endif /* shared_buffer_seq_qm_hpp */
