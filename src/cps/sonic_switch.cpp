//
//  sonic_switch.cpp
//  FSB
//
//  Created by Wenkai Li on 2/28/26.
//

#include "sonic_switch.hpp"

#include "shared_buffer_stub_qm.hpp"

#include <sstream>

SONiCSwitch::SONiCSwitch(unsigned int port_cnt,
                         unsigned int ingress_queue_cnt,
                         unsigned int total_time,
                         SharedBufferPointInfo info):
SharedBufferPoint(total_time, info),
port_cnt_(port_cnt),
ingress_queue_cnt_(ingress_queue_cnt) {
    init();
}

void SONiCSwitch::add_nodes() {
    QueueInfo in_info{};
    in_info.size = MAX_QUEUE_SIZE;
    in_info.max_enq = 1;
    in_info.max_deq = 1;
    in_info.meta = 0;
    in_info.type = queue_t::SB_INGRESS;

    QueueInfo out_info{};
    out_info.size = MAX_QUEUE_SIZE;
    out_info.max_enq = 1;
    out_info.max_deq = 1;
    out_info.meta = 0;
    out_info.type = queue_t::SB_EGRESS;

    cid_t m_id = "SB_CORE";
    SharedBufferStubQM* sb_qm = new SharedBufferStubQM(
        m_id,
        total_time,
        vector<QueueInfo>(ingress_queue_cnt_, in_info),
        vector<QueueInfo>(port_cnt_, out_info),
        net_ctx);
    nodes.push_back(m_id);
    id_to_qm[m_id] = sb_qm;
}

void SONiCSwitch::add_edges() {
}

void SONiCSwitch::add_metrics() {
    // CEnq
    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        CEnq* ce = new CEnq(queue, total_time, net_ctx);
        cenq_.push_back(ce);
        metrics[metric_t::CENQ][queue->get_id()] = ce;
        queue->add_metric(metric_t::CENQ, ce);
    }

    // Demand
    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        Demand* d = new Demand(queue, total_time, net_ctx);
        demand_.push_back(d);
        metrics[metric_t::DEMAND][queue->get_id()] = d;
        queue->add_metric(metric_t::DEMAND, d);        
    }
}

string SONiCSwitch::cp_model_str(model& m, NetContext& net_ctx, unsigned int t) {
    (void) net_ctx;

    stringstream ss;
    ss << "ports=" << port_cnt_ << ", ingress_queues=" << ingress_queue_cnt_ << endl;
    ss << "qid: cenq, demand" << endl;
    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        ss << queue->get_id() << ": " << m.eval(cenq_[q]->val(t).second).get_numeral_int();
        ss << ", " << m.eval(demand_[q]->val(t).second).get_numeral_int();
        ss << endl;
    }
    return ss.str();
}
