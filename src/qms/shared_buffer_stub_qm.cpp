//
//  shared_buffer_stub_qm.cpp
//  FSB
//
//  Created by Wenkai Li on 3/5/26.
//

#include "shared_buffer_stub_qm.hpp"

SharedBufferStubQM::SharedBufferStubQM(cid_t id,
                                       unsigned int total_time,
                                       vector<QueueInfo> in_queue_info,
                                       vector<QueueInfo> out_queue_info,
                                       NetContext& net_ctx):
QueuingModule(id, total_time, in_queue_info, out_queue_info, net_ctx) {
    init(net_ctx);
}

void SharedBufferStubQM::add_proc_vars(NetContext& net_ctx) {
    (void) net_ctx;
}

void SharedBufferStubQM::add_constrs(NetContext& net_ctx, map<string, expr>& constr_map) {
    (void) net_ctx;
    (void) constr_map;
}
