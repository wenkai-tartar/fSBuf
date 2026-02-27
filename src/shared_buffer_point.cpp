//  shared_buffer_point.cpp
//  FSB
//
//  Created by Wenkai Li on 2/27/26.
//  Copyright © 2026 Wenkai Li. All rights reserved.
//

#include "shared_buffer_point.hpp"

SharedBufferPoint::SharedBufferPoint(unsigned int total_time, SharedBufferPointInfo info):
ContentionPoint(total_time), _sb_info(info) {
}

void SharedBufferPoint::add_in_queue_constrs() {
    ContentionPoint::add_in_queue_constrs();
}

void SharedBufferPoint::add_out_queue_constrs() {
    ContentionPoint::add_out_queue_constrs();
}

m_val_expr_t SharedBufferPoint::metric_val_expr(metric_t metric, unsigned int q, unsigned t) {
    return ContentionPoint::metric_val_expr(metric, q, t);
}
