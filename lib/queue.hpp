//
//  Queue.hpp
//  FPerf
//
//  Created by Mina Tahmasbi Arashloo on 11/5/20.
//  Copyright © 2020 Mina Tahmasbi Arashloo. All rights reserved.
//

#ifndef Queue_hpp
#define Queue_hpp

#include <map>
#include <string>
#include <vector>

#include "metric.hpp"
#include "net_context.hpp"
#include "util.hpp"

class Metric;
enum class metric_t;

enum class queue_t {
    QUEUE = 0,
    IMM_QUEUE,
    LINK,
    SB_INGRESS,
    SB_EGRESS,
    IMM_SB_INGRESS,
    IMM_SB_EGRESS,
};

struct QueueInfo {
    unsigned int size;
    unsigned int max_enq;
    unsigned int max_deq;
    unsigned int meta;
    queue_t type;
};

/* *********************** Queue ********************** */

class Queue {
public:
    Queue(cid_t module_id,
          cid_t queue_id,
          unsigned int size,
          unsigned int max_enq,
          unsigned int max_deq,
          unsigned int total_time,
          NetContext& net_ctx);

    unsigned int size();
    unsigned int max_enq();
    unsigned int max_deq();

    vector<expr>& operator[](int ind);
    const vector<expr>& operator[](int ind) const;
    vector<expr>& elem(int ind);
    const vector<expr>& elem(int ind) const;

    vector<expr>& enqs(unsigned int ind);
    const vector<expr>& enqs(unsigned int ind) const;
    expr& enq_cnt(unsigned int t);
    expr& deq_cnt(unsigned int t);

    void add_metric(metric_t metric_type, Metric* m);
    Metric* get_metric(metric_t metric_type);

    cid_t get_id() const;

    virtual void add_constrs(NetContext& net_ctx, map<string, expr>& constr_map);

    virtual string get_model_str(model& m, NetContext& net_ctx, unsigned int t);

    friend ostream& operator<<(ostream& os, const Queue& q);

protected:
    cid_t id;
    unsigned int size_;
    unsigned int max_enq_;
    unsigned int max_deq_;

    unsigned int total_time;

    vector<expr>* elems_;
    vector<expr>* enqs_;
    vector<expr> enq_cnt_;
    vector<expr> deq_cnt_;

    vector<expr>* tmp_val;
    vector<expr>* enq_ind;
    vector<expr> tail;
    vector<expr> tmp_tail;

    map<metric_t, Metric*> metrics;

    void add_vars(NetContext& net_ctx);

    void sliding_window_vars(NetContext& net_ctx);
    virtual void sliding_window_constrs(NetContext& net_ctx, map<string, expr>& constr_map);
};

/* *********************** ImmQueue ********************** */
/* Enqueued packets appear in the queue in the same time step */

class ImmQueue : public Queue {
public:
    ImmQueue(cid_t module_id,
             cid_t queue_id,
             unsigned int size,
             unsigned int max_enq,
             unsigned int max_deq,
             unsigned int total_time,
             NetContext& net_ctx);

    friend ostream& operator<<(ostream& os, const ImmQueue& q);

protected:
    void sliding_window_constrs(NetContext& net_ctx, map<string, expr>& constr_map);
};

/* *********************** Link ********************** */
/* Packets that come in at time t, appear on the other side at time t + 1 */

class Link : public Queue {
public:
    Link(cid_t module_id, cid_t queue_id, unsigned int total_time, NetContext& net_ctx);

    void add_constrs(NetContext& net_ctx, map<string, expr>& constr_map);


    friend ostream& operator<<(ostream& os, const Link& q);
};

/* *********************** SBQueue ********************** */
/* Shared-buffer compatibility queue:
 * this is a coarse-step link/interface object, not a real packet container.
 * Packet storage lives inside SharedBufferSwitchQM. The queue keeps:
 *   - enqs -> enq_cnt interface visibility
 *   - elem state that exposes ingress-visible data to the shared-buffer core
 *   - coarse-step pause/resume, receiver-visible control arrivals, and delayed sender-visible
 *     apply state
 *
 * TODO(shared-buffer-semantics): Introduce a SharedBufferSemanticPolicy and make SBQueue a
 * thinner interface object. The queue API should continue to expose enqueue-visible arrivals and
 * pause/resume/sender_paused signals, but the timing semantics for those signals should move out
 * of SBQueue so FAST_STEADY and PRECISE_PFC can share the same queue shape while using different
 * transport / feedback timing models.
 */
class SBQueue : public Queue {
public:
    SBQueue(cid_t module_id,
            cid_t queue_id,
            unsigned int size,
            unsigned int max_enq,
            unsigned int max_deq,
            unsigned int total_time,
            NetContext& net_ctx);

    void add_constrs(NetContext& net_ctx, map<string, expr>& constr_map) override;

    string get_model_str(model& m, NetContext& net_ctx, unsigned int t) override;

    expr& pause_tx(unsigned int t);
    const expr& pause_tx(unsigned int t) const;

    expr& resume_tx(unsigned int t);
    const expr& resume_tx(unsigned int t) const;

    expr& pause_rx(unsigned int t);
    const expr& pause_rx(unsigned int t) const;

    expr& resume_rx(unsigned int t);
    const expr& resume_rx(unsigned int t) const;

    expr& sender_paused(unsigned int t);
    const expr& sender_paused(unsigned int t) const;

protected:
    virtual expr elem_pkt(unsigned int t, NetContext& net_ctx) const;

private:
    vector<expr> pause_tx_;
    vector<expr> resume_tx_;
    vector<expr> pause_rx_;
    vector<expr> resume_rx_;
    vector<expr> sender_paused_;
};

class ImmSBQueue : public SBQueue {
public:
    using SBQueue::SBQueue;

protected:
    expr elem_pkt(unsigned int t, NetContext& net_ctx) const override;
};

/* Compatibility wrappers: legacy code still refers to ingress/egress queue classes,
 * but both now share the same SBQueue semantics.
 */
class SBIngressQueue : public SBQueue {
public:
    using SBQueue::SBQueue;
};

class SBEgressQueue : public SBQueue {
public:
    using SBQueue::SBQueue;
};
#endif /* Queue_hpp */
