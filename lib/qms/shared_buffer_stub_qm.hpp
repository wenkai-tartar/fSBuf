//
//  shared_buffer_stub_qm.hpp
//  FSB
//
//  Created by Wenkai Li on 3/5/26.
//

#ifndef shared_buffer_stub_qm_hpp
#define shared_buffer_stub_qm_hpp

#include "queuing_module.hpp"

class SharedBufferStubQM : public QueuingModule {
public:
    SharedBufferStubQM(cid_t id,
                       unsigned int total_time,
                       vector<QueueInfo> in_queue_info,
                       vector<QueueInfo> out_queue_info,
                       NetContext& net_ctx);

    void add_constrs(NetContext& net_ctx, map<string, expr>& constr_map) override;

private:
    void add_proc_vars(NetContext& net_ctx) override;
};

#endif /* shared_buffer_stub_qm_hpp */
