//
//  example.hpp
//  FPerf
//
//  Created by Mina Tahmasbi Arashloo on 12/15/20.
//  Copyright © 2020 Mina Tahmasbi Arashloo. All rights reserved.
//

#ifndef example_hpp
#define example_hpp

#include <deque>
#include <fstream>
#include <map>
#include <vector>

#include "util.hpp"

using namespace std;

struct PacketSnapshot {
    bool valid = false;
    int meta1 = -1;
    int meta2 = -1;
};

struct SharedBufferQueueCtrlState {
    PacketSnapshot vis_pkt;
    bool pause_tx = false;
    bool resume_tx = false;
    bool pause_rx = false;
    bool resume_rx = false;
    bool sender_paused = false;
};

struct SharedBufferIngressState {
    bool ingress_paused = false;
    SharedBufferQueueCtrlState queue_ctrl;
};

struct SharedBufferEgressState {
    bool sender_paused = false;
    bool can_emit = false;
    bool has_pkt = false;
    bool blocked = false;
    int min_stamp = -1;
};

struct SharedBufferSlotState {
    PacketSnapshot pkt;
    int ingress = -1;
    int egress = -1;
    int stamp = -1;
    bool read = false;
    bool write = false;
    // TODO(shared-buffer-witness): write_q is derivable from the microstate's active_q when
    // write=true. Keep it for witness readability for now and consider removing it later.
    int write_q = -1;
};

struct SharedBufferMicroState {
    int active_q = -1;
    int ingress_pool_total = 0;
    int headroom_pool_total = 0;
    int egress_lossless_pool_total = 0;
    vector<int> ingress_pool_occ;
    vector<int> headroom_pool_occ;
    vector<int> egress_lossless_pool_occ;
    vector<bool> admit_in;
    vector<bool> admit;
    vector<bool> alloc_in;
    vector<bool> alloc_hdr;
    vector<bool> admit_e;
    vector<bool> alloc_eg_lossless;
    vector<SharedBufferSlotState> slots;
};

struct SharedBufferSwitchState {
    vector<SharedBufferIngressState> ingress;
    vector<SharedBufferEgressState> egress;
    vector<SharedBufferMicroState> microstates;
};

struct Example {
    virtual ~Example() = default;

    unsigned int total_time;
    cid_t query_qid;
    map<cid_t, vector<unsigned int>> enqs;
    map<cid_t, vector<unsigned int>> deqs;
    map<cid_t, vector<unsigned int>> demands;

    map<cid_t, vector<vector<int>>> enqs_meta1;
    map<cid_t, vector<vector<int>>> enqs_meta2;

    friend ostream& operator<<(ostream& os, const Example& eg);
};

struct IndexedExample {
    virtual ~IndexedExample() = default;

    unsigned int total_time;
    cid_t query_qid;
    vector<vector<unsigned int>> enqs;
    vector<vector<unsigned int>> deqs;
    vector<vector<unsigned int>> demands;

    vector<vector<vector<int>>> enqs_meta1;
    vector<vector<vector<int>>> enqs_meta2;

    friend ostream& operator<<(ostream& os, const IndexedExample& eg);
};

struct SBExample : public Example {
    map<cid_t, vector<SharedBufferSwitchState>> shared_buffer_states;

    friend ostream& operator<<(ostream& os, const SBExample& eg);
};

struct SBIndexedExample : public IndexedExample {
    string query_quant;
    time_range_t query_time_range = {0, 0};
    string query_lhs;
    string query_metric;
    string query_op;
    unsigned int query_thresh = 0;

    vector<cid_t> input_queue_ids;
    vector<cid_t> shared_buffer_switch_ids;
    vector<vector<SharedBufferSwitchState>> shared_buffer_states;

    friend ostream& operator<<(ostream& os, const SBIndexedExample& eg);
};

struct Trace {
    unsigned int total_time;
    vector<vector<unsigned int>> enqs;

    friend ostream& operator<<(ostream& os, const Trace& tr);
};

void write_examples_to_file(deque<Example*>& examples, string fname);

void read_examples_from_file(deque<Example*>& examples, string fname);

#endif /* example_hpp */
