#include "multi_shared_buffer.hpp"

#include <sstream>
#include <stdexcept>

using namespace std;

MultiSharedBufferCP::MultiSharedBufferCP(unsigned int total_time,
                                         SharedBufferEnvInfo env_info,
                                         vector<MultiSharedBufferSwitchConfig> switch_configs,
                                         vector<MultiSharedBufferEdge> internal_edges):
SharedBufferPoint(total_time, env_info),
switch_configs_(std::move(switch_configs)),
internal_edges_(std::move(internal_edges)) {
    init();
}

const vector<MultiSharedBufferEdge>& MultiSharedBufferCP::internal_edges() const {
    return internal_edges_;
}

SharedBufferSwitchQM* MultiSharedBufferCP::get_switch_qm(const cid_t& switch_id) const {
    auto it = id_to_qm.find(switch_id);
    if (it == id_to_qm.end()) {
        throw runtime_error("MultiSharedBufferCP references unknown switch id");
    }

    SharedBufferSwitchQM* qm = dynamic_cast<SharedBufferSwitchQM*>(it->second);
    if (qm == nullptr) {
        throw runtime_error("MultiSharedBufferCP switch id does not map to a SharedBufferSwitchQM");
    }
    return qm;
}

void MultiSharedBufferCP::populate_boundary_ingress_ser_factors() {
    boundary_ingress_ser_factor_.clear();

    for (Queue* queue : in_queues) {
        bool found = false;
        for (const cid_t& switch_id : switch_order_) {
            SharedBufferSwitchQM* qm = get_switch_qm(switch_id);
            for (unsigned int q = 0; q < qm->in_queue_cnt(); q++) {
                if (qm->get_in_queue(q)->get_id() == queue->get_id()) {
                    boundary_ingress_ser_factor_[queue->get_id()] = qm->ingress_serialization_factor(q);
                    found = true;
                    break;
                }
            }
            if (found) break;
        }

        if (!found) {
            throw runtime_error("MultiSharedBufferCP failed to resolve boundary ingress serialization factor");
        }
    }
}

void MultiSharedBufferCP::add_nodes() {
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

    for (MultiSharedBufferSwitchConfig& switch_cfg : switch_configs_) {
        if (switch_cfg.switch_info.ingress_queue_cnt == 0 || switch_cfg.switch_info.egress_queue_cnt == 0) {
            throw runtime_error("MultiSharedBufferCP requires non-zero ingress and egress queue counts");
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
    }
}

void MultiSharedBufferCP::add_edges() {
    for (const MultiSharedBufferEdge& edge : internal_edges_) {
        SharedBufferSwitchQM* src_qm = get_switch_qm(edge.src_switch);
        SharedBufferSwitchQM* dst_qm = get_switch_qm(edge.dst_switch);
        if (edge.src_egress >= src_qm->out_queue_cnt()) {
            throw runtime_error("MultiSharedBufferCP internal edge src_egress out of range");
        }
        if (edge.dst_ingress >= dst_qm->in_queue_cnt()) {
            throw runtime_error("MultiSharedBufferCP internal edge dst_ingress out of range");
        }

        module_edges[edge.src_switch].push_back(edge.dst_switch);
        queue_edges[cid_pair(edge.src_switch, edge.dst_switch)].push_back(
            qpair(edge.src_egress, edge.dst_ingress));
    }
}

void MultiSharedBufferCP::add_metrics() {
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

    for (const cid_t& switch_id : switch_order_) {
        SharedBufferSwitchQM* qm = get_switch_qm(switch_id);
        for (unsigned int q = 0; q < qm->in_queue_cnt(); q++) {
            Queue* queue = qm->get_in_queue(q);
            if (metrics[metric_t::CENQ].find(queue->get_id()) == metrics[metric_t::CENQ].end()) {
                CEnq* ce = new CEnq(queue, total_time, net_ctx);
                switch_in_cenq_.push_back(ce);
                metrics[metric_t::CENQ][queue->get_id()] = ce;
                queue->add_metric(metric_t::CENQ, ce);
            }
        }

        for (unsigned int e = 0; e < qm->out_queue_cnt(); e++) {
            Queue* queue = qm->get_out_queue(e);
            if (metrics[metric_t::CENQ].find(queue->get_id()) == metrics[metric_t::CENQ].end()) {
                CEnq* ce = new CEnq(queue, total_time, net_ctx);
                switch_out_cenq_.push_back(ce);
                metrics[metric_t::CENQ][queue->get_id()] = ce;
                queue->add_metric(metric_t::CENQ, ce);
            }
        }
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

    for (unsigned int q = 0; q < in_queues.size(); q++) {
        SBQueue* queue = dynamic_cast<SBQueue*>(in_queues[q]);
        if (queue == nullptr) {
            throw runtime_error("MultiSharedBufferCP boundary ingress queue is not an SBQueue");
        }
        vector<expr> paused_exprs;
        paused_exprs.reserve(total_time);
        for (unsigned int t = 0; t < total_time; t++) {
            paused_exprs.push_back(queue->sender_paused(t));
        }
        SenderPaused* sp = new SenderPaused(queue, total_time, paused_exprs, net_ctx);
        sender_paused_.push_back(sp);
        metrics[metric_t::SENDER_PAUSED][queue->get_id()] = sp;
        queue->add_metric(metric_t::SENDER_PAUSED, sp);
    }

    for (unsigned int e = 0; e < out_queues.size(); e++) {
        SBQueue* queue = dynamic_cast<SBQueue*>(out_queues[e]);
        if (queue == nullptr) {
            throw runtime_error("MultiSharedBufferCP boundary egress queue is not an SBQueue");
        }
        vector<expr> paused_exprs;
        paused_exprs.reserve(total_time);
        for (unsigned int t = 0; t < total_time; t++) {
            paused_exprs.push_back(queue->sender_paused(t));
        }
        SenderPaused* sp = new SenderPaused(queue, total_time, paused_exprs, net_ctx);
        sender_paused_.push_back(sp);
        metrics[metric_t::SENDER_PAUSED][queue->get_id()] = sp;
        queue->add_metric(metric_t::SENDER_PAUSED, sp);
    }
}

void MultiSharedBufferCP::add_in_queue_constrs() {
    SharedBufferPoint::add_in_queue_constrs();
    populate_boundary_ingress_ser_factors();

    map<string, expr> new_constr_map;
    char constr_name[160];

    for (unsigned int q = 0; q < in_queues.size(); q++) {
        SBQueue* inq = dynamic_cast<SBQueue*>(in_queues[q]);
        if (inq == nullptr) {
            throw runtime_error("MultiSharedBufferCP boundary ingress queue is not an SBQueue");
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

            snprintf(constr_name, sizeof(constr_name), "multi_sb_boundary_enq_requires_demand_q%u_t%u", q, t);
            new_constr_map.insert(named_constr(
                constr_name,
                net_ctx.pkt2val(inq->enqs(0)[t]) ==
                    (demand_t > 0 && !inq->sender_paused(t) && can_inject)));
        }
    }

    add_constr_from_map(new_constr_map);
}

string MultiSharedBufferCP::cp_model_str(model& m, NetContext& net_ctx, unsigned int t) {
    stringstream ss;
    ss << "multi_shared_buffer_cp"
       << " switches=" << switch_order_.size()
       << ", internal_edges=" << internal_edges_.size() << endl;

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
        ss << get_switch_qm(switch_id)->debug_model_str(m, net_ctx, t);
    }

    return ss.str();
}
