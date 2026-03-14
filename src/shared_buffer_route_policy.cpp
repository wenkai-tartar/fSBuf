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
                                           expr pkt,
                                           unsigned int egress_count,
                                           NetContext& net_ctx) const {
    return z3::mod(net_ctx.pkt2meta1(pkt), net_ctx.int_val((int) egress_count));
}

TableRoutePolicy::TableRoutePolicy(vector<unsigned int> route_table):
route_table_(std::move(route_table)) {
}

void TableRoutePolicy::add_constrs(cid_t qm_id,
                                   unsigned int total_time,
                                   const vector<Queue*>& in_queues,
                                   unsigned int egress_count,
                                   NetContext& net_ctx,
                                   map<string, expr>& constr_map) {
    if (route_table_.empty()) {
        throw runtime_error("TableRoutePolicy route table must be non-empty");
    }

    for (unsigned int routed_e : route_table_) {
        if (routed_e >= egress_count) {
            throw runtime_error("TableRoutePolicy egress index out of range");
        }
    }

    char constr_name[160];
    for (unsigned int q = 0; q < in_queues.size(); q++) {
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
                             implies(has_pkt, meta1 >= 0 && meta1 < (int) route_table_.size())));
        }
    }
}

expr TableRoutePolicy::routed_egress(
    cid_t, expr pkt, unsigned int, NetContext& net_ctx) const {
    expr meta1 = net_ctx.pkt2meta1(pkt);
    expr routed_e = net_ctx.int_val((int) route_table_[0]);
    for (int meta = (int) route_table_.size() - 1; meta >= 0; meta--) {
        routed_e = ite(meta1 == meta, net_ctx.int_val((int) route_table_[meta]), routed_e);
    }
    return routed_e;
}
