//
//  shared_buffer_route_policy.cpp
//  FSB
//
//  Created by Wenkai Li on 3/9/26.
//

#include "shared_buffer_route_policy.hpp"

#include <stdexcept>

void SharedBufferRoutePolicy::add_proc_vars(
    cid_t, unsigned int, const vector<Queue*>&, unsigned int, NetContext&) {
}

void SharedBufferRoutePolicy::add_constrs(
    cid_t, unsigned int, const vector<Queue*>&, unsigned int, NetContext&, map<string, expr>&) {
}

expr ModuloMeta1RoutePolicy::routed_egress(cid_t,
                                           unsigned int,
                                           unsigned int,
                                           expr pkt,
                                           unsigned int egress_count,
                                           NetContext& net_ctx) const {
    return z3::mod(net_ctx.pkt2meta1(pkt), net_ctx.int_val((int) egress_count));
}

FixedIngressRoutePolicy::FixedIngressRoutePolicy(vector<unsigned int> ingress_to_egress):
ingress_to_egress_(std::move(ingress_to_egress)) {
}

void FixedIngressRoutePolicy::add_constrs(cid_t qm_id,
                                          unsigned int,
                                          const vector<Queue*>& in_queues,
                                          unsigned int egress_count,
                                          NetContext& net_ctx,
                                          map<string, expr>& constr_map) {
    if (ingress_to_egress_.size() != in_queues.size()) {
        throw runtime_error("FixedIngressRoutePolicy requires one egress per ingress queue");
    }

    char constr_name[160];
    for (unsigned int q = 0; q < ingress_to_egress_.size(); q++) {
        unsigned int routed_e = ingress_to_egress_[q];
        if (routed_e >= egress_count) {
            throw runtime_error("FixedIngressRoutePolicy egress index out of range");
        }

        snprintf(constr_name,
                 sizeof(constr_name),
                 "%s_fixed_route_egress_range_q%u",
                 qm_id.c_str(),
                 q);
        constr_map.insert(named_constr(constr_name, net_ctx.bool_val(routed_e < egress_count)));
    }
}

expr FixedIngressRoutePolicy::routed_egress(
    cid_t, unsigned int ingress, unsigned int, expr, unsigned int, NetContext& net_ctx) const {
    if (ingress >= ingress_to_egress_.size()) {
        throw runtime_error("FixedIngressRoutePolicy ingress index out of range");
    }
    return net_ctx.int_val((int) ingress_to_egress_[ingress]);
}

TableRoutePolicy::TableRoutePolicy(vector<vector<unsigned int>> route_table):
route_table_(std::move(route_table)) {
}

void TableRoutePolicy::add_constrs(cid_t qm_id,
                                   unsigned int total_time,
                                   const vector<Queue*>& in_queues,
                                   unsigned int egress_count,
                                   NetContext& net_ctx,
                                   map<string, expr>& constr_map) {
    if (route_table_.size() != in_queues.size()) {
        throw runtime_error("TableRoutePolicy requires one table row per ingress queue");
    }

    char constr_name[160];
    for (unsigned int q = 0; q < route_table_.size(); q++) {
        const vector<unsigned int>& row = route_table_[q];
        if (row.empty()) {
            throw runtime_error("TableRoutePolicy rows must be non-empty");
        }
        for (unsigned int routed_e : row) {
            if (routed_e >= egress_count) {
                throw runtime_error("TableRoutePolicy egress index out of range");
            }
        }

        for (unsigned int t = 0; t < total_time; t++) {
            expr pkt = in_queues[q]->enqs(0)[t];
            expr has_pkt = net_ctx.pkt2val(pkt);
            expr meta1 = net_ctx.pkt2meta1(pkt);

            snprintf(constr_name,
                     sizeof(constr_name),
                     "%s_table_route_meta1_range_q%u_t%u",
                     qm_id.c_str(),
                     q,
                     t);
            constr_map.insert(
                named_constr(constr_name,
                             implies(has_pkt, meta1 >= 0 && meta1 < (int) row.size())));
        }
    }
}

expr TableRoutePolicy::routed_egress(
    cid_t, unsigned int ingress, unsigned int, expr pkt, unsigned int, NetContext& net_ctx) const {
    if (ingress >= route_table_.size()) {
        throw runtime_error("TableRoutePolicy ingress index out of range");
    }

    const vector<unsigned int>& row = route_table_[ingress];
    expr meta1 = net_ctx.pkt2meta1(pkt);
    expr routed_e = net_ctx.int_val((int) row[0]);
    for (int meta = (int) row.size() - 1; meta >= 0; meta--) {
        routed_e = ite(meta1 == meta, net_ctx.int_val((int) row[meta]), routed_e);
    }
    return routed_e;
}
