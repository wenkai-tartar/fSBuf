#ifndef CBD_SHARED_BUFFER_HPP
#define CBD_SHARED_BUFFER_HPP

#include <memory>

#include "cenq.hpp"
#include "demand.hpp"
#include "dst.hpp"
#include "shared_buffer_point.hpp"
#include "shared_buffer_route_policy.hpp"
#include "shared_buffer_switch_qm.hpp"

struct CBDRingEdge {
    cid_t src_switch;
    unsigned int src_ingress;
    unsigned int src_egress;
    cid_t dst_switch;
    unsigned int dst_ingress;
    unsigned int priority = 0;
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

private:
    std::vector<CBDSharedBufferSwitchConfig> switch_configs_;
    std::vector<CBDRingEdge> ring_edges_;
    std::vector<cid_t> switch_order_;
    std::map<cid_t, SharedBufferSwitchQM*> switch_qms_;
    std::map<cid_t, unsigned int> boundary_ingress_ser_factor_;

    std::vector<CEnq*> cenq_;
    std::vector<CEnq*> out_cenq_;
    std::vector<Demand*> demand_;
    std::vector<Dst*> dst_;

    void add_nodes() override;
    void add_edges() override;
    void add_metrics() override;
    void add_in_queue_constrs() override;
    std::string cp_model_str(model& m, NetContext& net_ctx, unsigned int t) override;
};

#endif
