//
//  sonic_switch.cpp
//  FSB
//
//  Created by Wenkai Li on 2/28/26.
//

#include "sonic_switch.hpp"

#include "shared_buffer_switch_qm.hpp"

#include <sstream>
#include <stdexcept>

SONiCSwitch::SONiCSwitch(unsigned int total_time,
                         SharedBufferEnvInfo env_info,
                         SharedBufferSwitchInfo switch_info,
                         unique_ptr<SharedBufferRoutePolicy> route_policy):
SharedBufferPoint(total_time, env_info),
switch_info_(switch_info),
route_policy_(std::move(route_policy)) {
    if (route_policy_ == nullptr) {
        route_policy_ = make_unique<ModuloMeta1RoutePolicy>();
    }
    init();
}

void SONiCSwitch::add_nodes() {
    if (switch_info_.ingress_queue_cnt == 0 || switch_info_.egress_queue_cnt == 0) {
        throw runtime_error("SONiCSwitch requires non-zero ingress_queue_cnt and egress_queue_cnt");
    }

    QueueInfo in_info{};
    in_info.size = 1;
    in_info.max_enq = 1;
    in_info.max_deq = 1;
    in_info.meta = 0;
    in_info.type = queue_t::SB_INGRESS;

    QueueInfo out_info{};
    out_info.size = 1;
    out_info.max_enq = 1;
    out_info.max_deq = 1;
    out_info.meta = 0;
    out_info.type = queue_t::SB_EGRESS;

    cid_t m_id = "SB_CORE";
    SharedBufferSwitchQM* sb_qm = new SharedBufferSwitchQM(
        m_id,
        total_time,
        vector<QueueInfo>(switch_info_.ingress_queue_cnt, in_info),
        vector<QueueInfo>(switch_info_.egress_queue_cnt, out_info),
        switch_info_,
        route_policy_.get(),
        net_ctx);
    nodes.push_back(m_id);
    id_to_qm[m_id] = sb_qm;
}

void SONiCSwitch::add_edges() {
}

void SONiCSwitch::add_in_queue_constrs() {
    SharedBufferPoint::add_in_queue_constrs();

    map<string, expr> new_constr_map;
    char constr_name[160];

    for (unsigned int q = 0; q < in_queues.size(); q++) {
        SBQueue* inq = dynamic_cast<SBQueue*>(in_queues[q]);
        if (inq == nullptr) {
            throw runtime_error("SONiCSwitch boundary ingress queue is not an SBQueue");
        }

        Metric* demand_metric = inq->get_metric(metric_t::DEMAND);
        if (demand_metric == nullptr) continue;
        Metric* demand_dst_metric = inq->get_metric(metric_t::DEMAND_DST);
        if (demand_dst_metric == nullptr) {
            throw runtime_error("SONiCSwitch boundary ingress missing DEMAND_DST metric");
        }

        unsigned int ser_factor = 1;
        if (q < switch_info_.ingress_ser_factor.size() && switch_info_.ingress_ser_factor[q] > 0) {
            ser_factor = switch_info_.ingress_ser_factor[q];
        }

        for (unsigned int t = 0; t < total_time; t++) {
            expr demand_t = demand_metric->val(t).second;
            if (t > 0) {
                demand_t = demand_t - demand_metric->val(t - 1).second;
            }

            expr can_inject = net_ctx.bool_val(true);
            for (unsigned int dt = 1; dt < ser_factor && dt <= t; dt++) {
                can_inject = can_inject && !net_ctx.pkt2val(inq->enqs(0)[t - dt]);
            }

            snprintf(constr_name, sizeof(constr_name), "sb_boundary_enq_requires_demand_q%u_t%u", q, t);
            new_constr_map.insert(named_constr(
                constr_name,
                net_ctx.pkt2val(inq->enqs(0)[t]) ==
                    (demand_t > 0 && !inq->sender_paused(t) && can_inject)));

            snprintf(
                constr_name, sizeof(constr_name), "sb_boundary_meta1_from_demand_dst_q%u_t%u", q, t);
            new_constr_map.insert(named_constr(
                constr_name,
                implies(net_ctx.pkt2val(inq->enqs(0)[t]),
                        net_ctx.pkt2meta1(inq->enqs(0)[t]) == demand_dst_metric->val(t).second)));
        }
    }

    add_constr_from_map(new_constr_map);
}

void SONiCSwitch::add_metrics() {
    SharedBufferSwitchQM* sb_qm = dynamic_cast<SharedBufferSwitchQM*>(id_to_qm["SB_CORE"]);
    if (sb_qm == nullptr) {
        throw runtime_error("SONiCSwitch SB_CORE is not a SharedBufferSwitchQM");
    }

    // Input CEnq
    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        CEnq* ce = new CEnq(queue, total_time, net_ctx);
        cenq_.push_back(ce);
        metrics[metric_t::CENQ][queue->get_id()] = ce;
        queue->add_metric(metric_t::CENQ, ce);
    }

    // Output CEnq
    for (unsigned int q = 0; q < out_queues.size(); q++) {
        Queue* queue = out_queues[q];
        CEnq* ce = new CEnq(queue, total_time, net_ctx);
        out_cenq_.push_back(ce);
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

    // DemandDst
    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        DemandDst* d = new DemandDst(queue, total_time, net_ctx);
        demand_dst_.push_back(d);
        metrics[metric_t::DEMAND_DST][queue->get_id()] = d;
        queue->add_metric(metric_t::DEMAND_DST, d);
    }

    // Dst
    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        Dst* d = new Dst(queue, total_time, net_ctx);
        dst_.push_back(d);
        metrics[metric_t::DST][queue->get_id()] = d;
        queue->add_metric(metric_t::DST, d);
    }
}

string SONiCSwitch::cp_model_str(model& m, NetContext& net_ctx, unsigned int t) {
    stringstream ss;
    ss << "egress_queues=" << switch_info_.egress_queue_cnt
       << ", ingress_queues=" << switch_info_.ingress_queue_cnt << endl;
    ss << "ingress qid: cenq, demand" << endl;
    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        ss << queue->get_id() << ": " << m.eval(cenq_[q]->val(t).second).get_numeral_int();
        ss << ", " << m.eval(demand_[q]->val(t).second).get_numeral_int();
        ss << endl;
    }

    ss << "egress qid: cenq" << endl;
    for (unsigned int e = 0; e < out_queues.size(); e++) {
        Queue* queue = out_queues[e];
        ss << queue->get_id() << ": " << m.eval(out_cenq_[e]->val(t).second).get_numeral_int();
        ss << endl;
    }

    SharedBufferSwitchQM* sb_qm = dynamic_cast<SharedBufferSwitchQM*>(id_to_qm["SB_CORE"]);
    if (sb_qm != nullptr) {
        ss << sb_qm->debug_model_str(m, net_ctx, t);
    }
    return ss.str();
}
