#include "cbd_shared_buffer.hpp"

#include <set>
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

const vector<CBDNodeRef>& CBDSharedBufferCP::ring_nodes() const {
    return ring_nodes_;
}

const CBDRingEdge& CBDSharedBufferCP::incoming_ring_edge(const CBDNodeRef& node) const {
    auto it = incoming_ring_edge_idx_.find(node);
    if (it == incoming_ring_edge_idx_.end()) {
        throw runtime_error("CBDSharedBufferCP ring node has no incoming ring edge");
    }
    return ring_edges_[it->second];
}

const CBDRingEdge& CBDSharedBufferCP::outgoing_ring_edge(const CBDNodeRef& node) const {
    auto it = outgoing_ring_edge_idx_.find(node);
    if (it == outgoing_ring_edge_idx_.end()) {
        throw runtime_error("CBDSharedBufferCP ring node has no outgoing ring edge");
    }
    return ring_edges_[it->second];
}

const CBDRingEdge& CBDSharedBufferCP::ring_edge_for_egress(const CBDEgressRef& egress) const {
    auto it = ring_edge_idx_by_egress_.find(egress);
    if (it == ring_edge_idx_by_egress_.end()) {
        throw runtime_error("CBDSharedBufferCP egress is not part of the CBD ring");
    }
    return ring_edges_[it->second];
}

bool CBDSharedBufferCP::is_ring_ingress(const CBDNodeRef& node) const {
    return incoming_ring_edge_idx_.find(node) != incoming_ring_edge_idx_.end();
}

bool CBDSharedBufferCP::is_ring_egress(const CBDEgressRef& egress) const {
    return ring_edge_idx_by_egress_.find(egress) != ring_edge_idx_by_egress_.end();
}

CBDNodeRef CBDSharedBufferCP::src_node(const CBDRingEdge& edge) const {
    return CBDNodeRef{edge.src_switch, edge.src_ingress, edge.priority};
}

CBDNodeRef CBDSharedBufferCP::dst_node(const CBDRingEdge& edge) const {
    return CBDNodeRef{edge.dst_switch, edge.dst_ingress, edge.priority};
}

CBDEgressRef CBDSharedBufferCP::src_egress(const CBDRingEdge& edge) const {
    return CBDEgressRef{edge.src_switch, edge.src_egress, edge.priority};
}

SharedBufferSwitchQM* CBDSharedBufferCP::get_switch_qm(const cid_t& switch_id) const {
    auto it = id_to_qm.find(switch_id);
    if (it == id_to_qm.end()) {
        throw runtime_error("CBDSharedBufferCP references unknown switch id");
    }

    SharedBufferSwitchQM* qm = dynamic_cast<SharedBufferSwitchQM*>(it->second);
    if (qm == nullptr) {
        throw runtime_error("CBDSharedBufferCP switch id does not map to a SharedBufferSwitchQM");
    }
    return qm;
}

void CBDSharedBufferCP::populate_boundary_ingress_ser_factors() {
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
            throw runtime_error("CBDSharedBufferCP failed to resolve boundary ingress serialization factor");
        }
    }
}

expr CBDSharedBufferCP::ring_edge_pending_to_egress(const CBDRingEdge& edge, unsigned int t) const {
    SharedBufferSwitchQM* src_qm = get_switch_qm(edge.src_switch);
    return src_qm->cbd_ingress_pending_to_egress(
        edge.src_ingress, edge.src_egress, t, edge.priority);
}

expr CBDSharedBufferCP::ring_edge_active(const CBDRingEdge& edge, unsigned int t) const {
    SharedBufferSwitchQM* src_qm = get_switch_qm(edge.src_switch);
    // CBD topology already chooses only ingress-to-ingress edges that carry a flow. At runtime,
    // the wait edge is active exactly when the corresponding src->dst physical link is PFC-blocked.
    return src_qm->egress_blocked(edge.src_egress, t, edge.priority);
}

expr CBDSharedBufferCP::ring_node_deadlock(const CBDNodeRef& node, unsigned int t) const {
    SharedBufferSwitchQM* qm = get_switch_qm(node.switch_id);
    const CBDRingEdge& in_edge = incoming_ring_edge(node);
    const CBDRingEdge& out_edge = outgoing_ring_edge(node);

    return qm->cbd_ingress_has_pkt(node.ingress, t, node.priority) &&
           qm->cbd_ingress_paused(node.ingress, t, node.priority) &&
           qm->cbd_ingress_no_progress(node.ingress, t, node.priority) &&
           ring_edge_active(in_edge, t) && ring_edge_active(out_edge, t);
}

void CBDSharedBufferCP::validate_and_index_ring() {
    ring_nodes_.clear();
    incoming_ring_edge_idx_.clear();
    outgoing_ring_edge_idx_.clear();
    ring_edge_idx_by_egress_.clear();

    if (ring_edges_.empty()) {
        throw runtime_error("CBDSharedBufferCP requires at least one CBD ring edge");
    }

    set<CBDNodeRef> ring_node_set;
    for (unsigned int edge_idx = 0; edge_idx < ring_edges_.size(); edge_idx++) {
        const CBDRingEdge& edge = ring_edges_[edge_idx];
        CBDNodeRef src = src_node(edge);
        CBDNodeRef dst = dst_node(edge);
        CBDEgressRef eg = src_egress(edge);

        if (!outgoing_ring_edge_idx_.emplace(src, edge_idx).second) {
            throw runtime_error("CBDSharedBufferCP ring source node has multiple outgoing edges");
        }
        if (!incoming_ring_edge_idx_.emplace(dst, edge_idx).second) {
            throw runtime_error("CBDSharedBufferCP ring destination node has multiple incoming edges");
        }
        if (!ring_edge_idx_by_egress_.emplace(eg, edge_idx).second) {
            throw runtime_error("CBDSharedBufferCP ring source egress is reused by multiple edges");
        }

        ring_node_set.insert(src);
        ring_node_set.insert(dst);
    }

    if (incoming_ring_edge_idx_.size() != ring_node_set.size() ||
        outgoing_ring_edge_idx_.size() != ring_node_set.size()) {
        throw runtime_error("CBDSharedBufferCP ring nodes must each have exactly one incoming and one outgoing edge");
    }

    CBDNodeRef start = src_node(ring_edges_.front());
    CBDNodeRef cur = start;
    set<CBDNodeRef> visited;
    vector<CBDNodeRef> visit_order;
    for (unsigned int step = 0; step < ring_edges_.size(); step++) {
        auto out_it = outgoing_ring_edge_idx_.find(cur);
        if (out_it == outgoing_ring_edge_idx_.end()) {
            throw runtime_error("CBDSharedBufferCP ring traversal reached a node without outgoing edge");
        }
        if (!visited.insert(cur).second) {
            throw runtime_error("CBDSharedBufferCP ring contains a repeated node before closing the cycle");
        }
        visit_order.push_back(cur);
        cur = dst_node(ring_edges_[out_it->second]);
    }

    if (cur.switch_id != start.switch_id || cur.ingress != start.ingress || cur.priority != start.priority) {
        throw runtime_error("CBDSharedBufferCP ring edges do not close into a single cycle");
    }
    if (visited.size() != ring_node_set.size()) {
        throw runtime_error("CBDSharedBufferCP ring edges form multiple disconnected cycles");
    }

    ring_nodes_ = std::move(visit_order);
}

void CBDSharedBufferCP::add_nodes() {
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
    }
}

void CBDSharedBufferCP::add_edges() {
    for (const CBDRingEdge& edge : ring_edges_) {
        if (edge.priority != 0) {
            throw runtime_error("CBDSharedBufferCP currently only supports priority=0");
        }

        SharedBufferSwitchQM* src_qm = get_switch_qm(edge.src_switch);
        SharedBufferSwitchQM* dst_qm = get_switch_qm(edge.dst_switch);
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

    validate_and_index_ring();
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

    for (const cid_t& switch_id : switch_order_) {
        SharedBufferSwitchQM* qm = get_switch_qm(switch_id);
        for (unsigned int q = 0; q < qm->in_queue_cnt(); q++) {
            Queue* queue = qm->get_in_queue(q);
            if (metrics[metric_t::CENQ].find(queue->get_id()) == metrics[metric_t::CENQ].end()) {
                CEnq* ce = new CEnq(queue, total_time, net_ctx);
                metrics[metric_t::CENQ][queue->get_id()] = ce;
                queue->add_metric(metric_t::CENQ, ce);
            }
        }

        for (unsigned int q = 0; q < qm->out_queue_cnt(); q++) {
            Queue* queue = qm->get_out_queue(q);
            if (metrics[metric_t::CENQ].find(queue->get_id()) == metrics[metric_t::CENQ].end()) {
                CEnq* ce = new CEnq(queue, total_time, net_ctx);
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

    for (const CBDRingEdge& edge : ring_edges_) {
        Queue* queue = get_switch_qm(edge.dst_switch)->get_in_queue(edge.dst_ingress);
        vector<expr> active_exprs;
        active_exprs.reserve(total_time);
        for (unsigned int t = 0; t < total_time; t++) {
            active_exprs.push_back(ring_edge_active(edge, t));
        }

        CBDWaitEdge* wait_edge = new CBDWaitEdge(queue, total_time, active_exprs, net_ctx);
        cbd_wait_edge_.push_back(wait_edge);
        metrics[metric_t::CBD_WAIT_EDGE][queue->get_id()] = wait_edge;
        queue->add_metric(metric_t::CBD_WAIT_EDGE, wait_edge);
    }

    for (const CBDNodeRef& node : ring_nodes_) {
        Queue* queue = get_switch_qm(node.switch_id)->get_in_queue(node.ingress);
        vector<expr> blocked_exprs;
        blocked_exprs.reserve(total_time);
        for (unsigned int t = 0; t < total_time; t++) {
            blocked_exprs.push_back(ring_node_deadlock(node, t));
        }

        CBDDeadlockNode* deadlock_node = new CBDDeadlockNode(queue, total_time, blocked_exprs, net_ctx);
        cbd_deadlock_node_.push_back(deadlock_node);
        metrics[metric_t::CBD_DEADLOCK_NODE][queue->get_id()] = deadlock_node;
        queue->add_metric(metric_t::CBD_DEADLOCK_NODE, deadlock_node);
    }
}

void CBDSharedBufferCP::add_in_queue_constrs() {
    SharedBufferPoint::add_in_queue_constrs();
    populate_boundary_ingress_ser_factors();

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

            snprintf(
                constr_name, sizeof(constr_name), "cbd_boundary_enq_requires_demand_q%u_t%u", q, t);
            new_constr_map.insert(named_constr(
                constr_name,
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

    ss << "ring nodes:" << endl;
    for (const CBDNodeRef& node : ring_nodes_) {
        const CBDRingEdge& in_edge = incoming_ring_edge(node);
        const CBDRingEdge& out_edge = outgoing_ring_edge(node);
        ss << "(" << node.switch_id << ", q" << node.ingress << ", p" << node.priority << ")"
           << " in<-(" << in_edge.src_switch << ", e" << in_edge.src_egress << ")"
           << " out->(" << out_edge.dst_switch << ", q" << out_edge.dst_ingress << ")" << endl;
    }

    ss << "ring paused links: active" << endl;
    for (unsigned int edge_idx = 0; edge_idx < ring_edges_.size(); edge_idx++) {
        const CBDRingEdge& edge = ring_edges_[edge_idx];
        ss << "(" << edge.src_switch << ", q" << edge.src_ingress << ", e" << edge.src_egress
           << ") -> (" << edge.dst_switch << ", q" << edge.dst_ingress << ", p" << edge.priority
           << "): " << m.eval(cbd_wait_edge_[edge_idx]->val(t).second).get_numeral_int() << endl;
    }

    ss << "ring nodes: deadlock" << endl;
    for (unsigned int node_idx = 0; node_idx < ring_nodes_.size(); node_idx++) {
        const CBDNodeRef& node = ring_nodes_[node_idx];
        ss << "(" << node.switch_id << ", q" << node.ingress << ", p" << node.priority
           << "): " << m.eval(cbd_deadlock_node_[node_idx]->val(t).second).get_numeral_int() << endl;
    }

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
