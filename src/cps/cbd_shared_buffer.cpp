#include "cbd_shared_buffer.hpp"

#include <sstream>
#include <stdexcept>

using namespace std;

CBDSharedBufferCP::CBDSharedBufferCP(unsigned int total_time,
                                     SharedBufferEnvInfo env_info,
                                     vector<CBDSharedBufferSwitchConfig> switch_configs,
                                     vector<CBDRingEdge> ring_edges):
SharedBufferPoint(total_time, env_info),
switch_configs_(std::move(switch_configs)),
ring_edges_(std::move(ring_edges)) {
    init();
}

const vector<CBDRingEdge>& CBDSharedBufferCP::ring_edges() const {
    return ring_edges_;
}

void CBDSharedBufferCP::add_nodes() {
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

    for (CBDSharedBufferSwitchConfig& switch_cfg : switch_configs_) {
        if (switch_cfg.switch_info.ingress_queue_cnt == 0 || switch_cfg.switch_info.egress_queue_cnt == 0) {
            throw runtime_error("CBDSharedBufferCP requires non-zero ingress and egress queue counts");
        }

        if (switch_cfg.route_policy == nullptr) {
            switch_cfg.route_policy = make_unique<ModuloMeta1RoutePolicy>();
        }

        SharedBufferSwitchQM* sb_qm = new SharedBufferSwitchQM(
            switch_cfg.switch_id,
            total_time,
            vector<QueueInfo>(switch_cfg.switch_info.ingress_queue_cnt, in_info),
            vector<QueueInfo>(switch_cfg.switch_info.egress_queue_cnt, out_info),
            switch_cfg.switch_info,
            switch_cfg.route_policy.get(),
            net_ctx);

        nodes.push_back(switch_cfg.switch_id);
        switch_order_.push_back(switch_cfg.switch_id);
        id_to_qm[switch_cfg.switch_id] = sb_qm;
        switch_qms_[switch_cfg.switch_id] = sb_qm;

        for (unsigned int q = 0; q < switch_cfg.switch_info.ingress_queue_cnt; q++) {
            boundary_ingress_ser_factor_[sb_qm->get_in_queue(q)->get_id()] =
                sb_qm->ingress_serialization_factor(q);
        }
    }
}

void CBDSharedBufferCP::add_edges() {
    for (const CBDRingEdge& edge : ring_edges_) {
        auto src_it = switch_qms_.find(edge.src_switch);
        auto dst_it = switch_qms_.find(edge.dst_switch);
        if (src_it == switch_qms_.end()) {
            throw runtime_error("CBDSharedBufferCP ring edge references unknown src switch");
        }
        if (dst_it == switch_qms_.end()) {
            throw runtime_error("CBDSharedBufferCP ring edge references unknown dst switch");
        }
        if (edge.priority != 0) {
            throw runtime_error("CBDSharedBufferCP currently only supports priority=0");
        }

        SharedBufferSwitchQM* src_qm = src_it->second;
        SharedBufferSwitchQM* dst_qm = dst_it->second;
        if (edge.src_egress >= src_qm->out_queue_cnt()) {
            throw runtime_error("CBDSharedBufferCP ring edge src_egress out of range");
        }
        if (edge.src_ingress >= src_qm->in_queue_cnt()) {
            throw runtime_error("CBDSharedBufferCP ring edge src_ingress out of range");
        }
        if (edge.dst_ingress >= dst_qm->in_queue_cnt()) {
            throw runtime_error("CBDSharedBufferCP ring edge dst_ingress out of range");
        }

        module_edges[edge.src_switch].push_back(edge.dst_switch);
        queue_edges[cid_pair(edge.src_switch, edge.dst_switch)].push_back(
            qpair(edge.src_egress, edge.dst_ingress));
    }
}

void CBDSharedBufferCP::add_metrics() {
    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        CEnq* ce = new CEnq(queue, total_time, net_ctx);
        cenq_.push_back(ce);
        metrics[metric_t::CENQ][queue->get_id()] = ce;
        queue->add_metric(metric_t::CENQ, ce);
    }

    for (unsigned int q = 0; q < out_queues.size(); q++) {
        Queue* queue = out_queues[q];
        CEnq* ce = new CEnq(queue, total_time, net_ctx);
        out_cenq_.push_back(ce);
        metrics[metric_t::CENQ][queue->get_id()] = ce;
        queue->add_metric(metric_t::CENQ, ce);
    }

    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        Demand* d = new Demand(queue, total_time, net_ctx);
        demand_.push_back(d);
        metrics[metric_t::DEMAND][queue->get_id()] = d;
        queue->add_metric(metric_t::DEMAND, d);
    }

    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        Dst* d = new Dst(queue, total_time, net_ctx);
        dst_.push_back(d);
        metrics[metric_t::DST][queue->get_id()] = d;
        queue->add_metric(metric_t::DST, d);
    }
}

void CBDSharedBufferCP::add_in_queue_constrs() {
    SharedBufferPoint::add_in_queue_constrs();

    map<string, expr> new_constr_map;
    char constr_name[160];

    for (unsigned int q = 0; q < in_queues.size(); q++) {
        SBQueue* inq = dynamic_cast<SBQueue*>(in_queues[q]);
        if (inq == nullptr) {
            throw runtime_error("CBDSharedBufferCP boundary ingress queue is not an SBQueue");
        }

        Metric* demand_metric = inq->get_metric(metric_t::DEMAND);
        if (demand_metric == nullptr) continue;

        unsigned int ser_factor = 1;
        auto sf_it = boundary_ingress_ser_factor_.find(inq->get_id());
        if (sf_it != boundary_ingress_ser_factor_.end() && sf_it->second > 0) {
            ser_factor = sf_it->second;
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

            snprintf(constr_name, sizeof(constr_name), "cbd_boundary_enq_from_demand_q%u_t%u", q, t);
            new_constr_map.insert(
                named_constr(constr_name,
                             net_ctx.pkt2val(inq->enqs(0)[t]) ==
                                 (demand_t > 0 && !inq->sender_paused(t) && can_inject)));
        }
    }

    add_constr_from_map(new_constr_map);
}

string CBDSharedBufferCP::cp_model_str(model& m, NetContext& net_ctx, unsigned int t) {
    stringstream ss;
    ss << "cbd_shared_buffer_cp"
       << " switches=" << switch_order_.size()
       << ", ring_edges=" << ring_edges_.size() << endl;

    ss << "boundary ingress qid: cenq, demand" << endl;
    for (unsigned int q = 0; q < in_queues.size(); q++) {
        Queue* queue = in_queues[q];
        ss << queue->get_id() << ": " << m.eval(cenq_[q]->val(t).second).get_numeral_int();
        ss << ", " << m.eval(demand_[q]->val(t).second).get_numeral_int();
        ss << endl;
    }

    ss << "boundary egress qid: cenq" << endl;
    for (unsigned int e = 0; e < out_queues.size(); e++) {
        Queue* queue = out_queues[e];
        ss << queue->get_id() << ": " << m.eval(out_cenq_[e]->val(t).second).get_numeral_int();
        ss << endl;
    }

    for (const cid_t& switch_id : switch_order_) {
        auto it = switch_qms_.find(switch_id);
        if (it != switch_qms_.end()) {
            ss << it->second->debug_model_str(m, net_ctx, t);
        }
    }
    return ss.str();
}
