#ifndef MULTI_SHARED_BUFFER_CP_HPP
#define MULTI_SHARED_BUFFER_CP_HPP

#include <memory>

#include "cenq.hpp"
#include "demand.hpp"
#include "dst.hpp"
#include "sender_paused.hpp"
#include "shared_buffer_point.hpp"
#include "shared_buffer_route_policy.hpp"
#include "shared_buffer_switch_qm.hpp"

struct MultiSharedBufferEdge {
    cid_t src_switch;
    unsigned int src_egress;
    cid_t dst_switch;
    unsigned int dst_ingress;
};

struct MultiSharedBufferSwitchConfig {
    cid_t switch_id;
    SharedBufferSwitchInfo switch_info;
    std::unique_ptr<SharedBufferRoutePolicy> route_policy;
};

class MultiSharedBufferCP : public SharedBufferPoint {
public:
    MultiSharedBufferCP(unsigned int total_time,
                        SharedBufferEnvInfo env_info,
                        std::vector<MultiSharedBufferSwitchConfig> switch_configs,
                        std::vector<MultiSharedBufferEdge> internal_edges);

    const std::vector<MultiSharedBufferEdge>& internal_edges() const;

private:
    std::vector<MultiSharedBufferSwitchConfig> switch_configs_;
    std::vector<MultiSharedBufferEdge> internal_edges_;
    std::vector<cid_t> switch_order_;
    std::map<cid_t, unsigned int> boundary_ingress_ser_factor_;

    std::vector<CEnq*> cenq_;
    std::vector<CEnq*> out_cenq_;
    std::vector<CEnq*> switch_in_cenq_;
    std::vector<CEnq*> switch_out_cenq_;
    std::vector<Demand*> demand_;
    std::vector<Dst*> dst_;
    std::vector<SenderPaused*> sender_paused_;

    SharedBufferSwitchQM* get_switch_qm(const cid_t& switch_id) const;
    void populate_boundary_ingress_ser_factors();
    void add_nodes() override;
    void add_edges() override;
    void add_metrics() override;
    void add_in_queue_constrs() override;
    std::string cp_model_str(model& m, NetContext& net_ctx, unsigned int t) override;
};

#endif
