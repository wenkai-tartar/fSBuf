//
//  shared_buffer_route_policy.hpp
//  FSB
//
//  Created by Wenkai Li on 3/9/26.
//

#ifndef shared_buffer_route_policy_hpp
#define shared_buffer_route_policy_hpp

#include <map>
#include <vector>

#include "queue.hpp"

using namespace std;

class SharedBufferRoutePolicy {
public:
    virtual ~SharedBufferRoutePolicy() = default;

    virtual void add_proc_vars(cid_t qm_id,
                               unsigned int total_time,
                               const vector<Queue*>& in_queues,
                               unsigned int egress_count,
                               NetContext& net_ctx);

    virtual void add_constrs(cid_t qm_id,
                             unsigned int total_time,
                             const vector<Queue*>& in_queues,
                             unsigned int egress_count,
                             NetContext& net_ctx,
                             map<string, expr>& constr_map);

    virtual expr routed_egress(cid_t qm_id,
                               expr pkt,
                               unsigned int egress_count,
                               NetContext& net_ctx) const = 0;
};

class ModuloMeta1RoutePolicy : public SharedBufferRoutePolicy {
public:
    expr routed_egress(cid_t qm_id,
                       expr pkt,
                       unsigned int egress_count,
                       NetContext& net_ctx) const override;
};

class TableRoutePolicy : public SharedBufferRoutePolicy {
public:
    explicit TableRoutePolicy(vector<unsigned int> route_table);

    void add_constrs(cid_t qm_id,
                     unsigned int total_time,
                     const vector<Queue*>& in_queues,
                     unsigned int egress_count,
                     NetContext& net_ctx,
                     map<string, expr>& constr_map) override;

    expr routed_egress(cid_t qm_id,
                       expr pkt,
                       unsigned int egress_count,
                       NetContext& net_ctx) const override;

private:
    vector<unsigned int> route_table_;
};

#endif /* shared_buffer_route_policy_hpp */
