//
//  sonic_switch.cpp
//  FSB
//
//  Created by Wenkai Li on 2/28/26.
//

#include "sonic_switch.hpp"

#include "shared_buffer_switch_qm.hpp"

#include <sstream>

SONiCSwitch::SONiCSwitch(unsigned int port_cnt,
                         unsigned int ingress_queue_cnt,
                         unsigned int total_time,
                         SharedBufferEnvInfo env_info,
                         SharedBufferSwitchInfo switch_info):
SharedBufferPoint(total_time, env_info),
port_cnt_(port_cnt),
ingress_queue_cnt_(ingress_queue_cnt),
switch_info_(switch_info) {
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
    SharedBufferSwitchQM* sb_qm = new SharedBufferSwitchQM(m_id,
                                                           total_time,
                                                           vector<QueueInfo>(ingress_queue_cnt_,
                                                                             in_info),
                                                           vector<QueueInfo>(port_cnt_, out_info),
                                                           switch_info_,
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
        Metric* demand_metric = in_queues[q]->get_metric(metric_t::DEMAND);
        if (demand_metric == nullptr) continue;

        unsigned int ser_factor = 1;
        if (q < switch_info_.ingress_ser_factor.size() && switch_info_.ingress_ser_factor[q] > 0) {
            ser_factor = switch_info_.ingress_ser_factor[q];
        }

        for (unsigned int t = 0; t < total_time; t++) {
            expr demand_t = demand_metric->val(t).second;
            if (t > 0) {
                demand_t = demand_t - demand_metric->val(t - 1).second;
            }

            for (unsigned int dt = 1; dt < ser_factor && t + dt < total_time; dt++) {
                expr demand_next = demand_metric->val(t + dt).second;
                if (t + dt > 0) {
                    demand_next = demand_next - demand_metric->val(t + dt - 1).second;
                }

                snprintf(constr_name,
                         sizeof(constr_name),
                         "sb_boundary_demand_cooldown_q%u_t%u_dt%u",
                         q,
                         t,
                         dt);
                // Boundary ingress demand cooldown for this SONiC switch follows the switch-local
                // ingress serialization factor.
                new_constr_map.insert(
                    named_constr(constr_name, implies(demand_t > 0, demand_next == 0)));
            }
        }
    }

    add_constr_from_map(new_constr_map);
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
    stringstream ss;
    ss << "ports=" << port_cnt_ << ", ingress_queues=" << ingress_queue_cnt_ << endl;
    ss << "qid: cenq, demand" << endl;
    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        ss << queue->get_id() << ": " << m.eval(cenq_[q]->val(t).second).get_numeral_int();
        ss << ", " << m.eval(demand_[q]->val(t).second).get_numeral_int();
        ss << endl;
    }

    SharedBufferSwitchQM* sb_qm = dynamic_cast<SharedBufferSwitchQM*>(id_to_qm["SB_CORE"]);
    if (sb_qm != nullptr) {
        ss << sb_qm->debug_model_str(m, net_ctx, t);
    }
    return ss.str();
}
