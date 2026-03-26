#ifndef CBD_SHARED_BUFFER_HPP
#define CBD_SHARED_BUFFER_HPP

#include <memory>
#include <tuple>

#include "cenq.hpp"
#include "cbd_blocked_edge.hpp"
#include "cdemand.hpp"
#include "demand.hpp"
#include "dst.hpp"
#include "shared_buffer_point.hpp"
#include "shared_buffer_route_policy.hpp"
#include "shared_buffer_switch_qm.hpp"

struct CBDRingEdge {
    // CBD edges are modeled at ingress granularity: src ingress is connected to dst ingress.
    // The edge itself is selected from a topology/path analysis that already establishes that a
    // flow can traverse src ingress -> dst ingress. src_egress remains only as an implementation
    // witness used to resolve whether the corresponding physical paused link is active.
    cid_t src_switch;
    unsigned int src_ingress;
    unsigned int src_egress;
    cid_t dst_switch;
    unsigned int dst_ingress;
    unsigned int priority = 0;
};

struct CBDNodeRef {
    cid_t switch_id;
    unsigned int ingress;
    unsigned int priority = 0;

    bool operator<(const CBDNodeRef& other) const {
        return std::tie(switch_id, ingress, priority) <
               std::tie(other.switch_id, other.ingress, other.priority);
    }
};

struct CBDEgressRef {
    cid_t switch_id;
    unsigned int egress;
    unsigned int priority = 0;

    bool operator<(const CBDEgressRef& other) const {
        return std::tie(switch_id, egress, priority) <
               std::tie(other.switch_id, other.egress, other.priority);
    }
};

struct CBDSharedBufferSwitchConfig {
    cid_t switch_id;
    SharedBufferSwitchInfo switch_info;
    std::unique_ptr<SharedBufferRoutePolicy> route_policy;
};

class CBDSharedBufferCP : public SharedBufferPoint {
public:
    CBDSharedBufferCP(unsigned int total_time,
                      SharedBufferEnvInfo env_info,
                      std::vector<CBDSharedBufferSwitchConfig> switch_configs,
                      std::vector<CBDRingEdge> ring_edges);

    const std::vector<CBDRingEdge>& ring_edges() const;
    const std::vector<CBDNodeRef>& ring_nodes() const;
    const CBDRingEdge& incoming_ring_edge(const CBDNodeRef& node) const;
    const CBDRingEdge& outgoing_ring_edge(const CBDNodeRef& node) const;
    const CBDRingEdge& ring_edge_for_egress(const CBDEgressRef& egress) const;
    bool is_ring_ingress(const CBDNodeRef& node) const;
    bool is_ring_egress(const CBDEgressRef& egress) const;

private:
    std::vector<CBDSharedBufferSwitchConfig> switch_configs_;
    std::vector<CBDRingEdge> ring_edges_;
    std::vector<CBDNodeRef> ring_nodes_;
    std::vector<cid_t> switch_order_;
    std::map<cid_t, unsigned int> boundary_ingress_ser_factor_;
    std::map<CBDNodeRef, unsigned int> incoming_ring_edge_idx_;
    std::map<CBDNodeRef, unsigned int> outgoing_ring_edge_idx_;
    std::map<CBDEgressRef, unsigned int> ring_edge_idx_by_egress_;

    std::vector<CEnq*> cenq_;
    std::vector<CEnq*> out_cenq_;
    std::vector<CBDBlockedEdge*> cbd_blocked_edge_;
    std::vector<Demand*> demand_;
    std::vector<CDemand*> cdemand_;
    std::vector<Dst*> dst_;

    CBDNodeRef src_node(const CBDRingEdge& edge) const;
    CBDNodeRef dst_node(const CBDRingEdge& edge) const;
    CBDEgressRef src_egress(const CBDRingEdge& edge) const;
    SharedBufferSwitchQM* get_switch_qm(const cid_t& switch_id) const;
    void populate_boundary_ingress_ser_factors();
    expr blocked_edge_active(const CBDRingEdge& incoming_edge, unsigned int t) const;
    void validate_and_index_ring();
    void add_nodes() override;
    void add_edges() override;
    void add_metrics() override;
    void add_in_queue_constrs() override;
    std::string cp_model_str(model& m, NetContext& net_ctx, unsigned int t) override;
};

#endif
