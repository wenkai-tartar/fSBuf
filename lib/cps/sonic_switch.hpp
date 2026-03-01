//
//  sonic_switch.hpp
//  FSB
//
//  Created by Wenkai Li on 2/28/26.
//

#ifndef sonic_switch_hpp
#define sonic_switch_hpp

#include "cenq.hpp"
#include "demand.hpp"
#include "shared_buffer_point.hpp"

using namespace std;

class SONiCSwitch : public SharedBufferPoint {
public:
    SONiCSwitch(unsigned int port_cnt,
                unsigned int ingress_queue_cnt,
                unsigned int total_time,
                SharedBufferPointInfo info);

private:
    unsigned int port_cnt_;
    unsigned int ingress_queue_cnt_;

    vector<CEnq*> cenq_;
    vector<Demand*> demand_;

    void add_nodes() override;
    void add_edges() override;
    void add_metrics() override;

    string cp_model_str(model& m, NetContext& net_ctx, unsigned int t) override;
};

#endif /* sonic_switch_hpp */
