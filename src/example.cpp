//
//  example.cpp
//  FPerf
//
//  Created by Mina Tahmasbi Arashloo on 12/15/20.
//  Copyright © 2020 Mina Tahmasbi Arashloo. All rights reserved.
//

#include "example.hpp"

#include <iostream>

namespace {
void append_packet_snapshot(ostream& os, const PacketSnapshot& pkt) {
    if (!pkt.valid) {
        os << "x";
        return;
    }
    os << "[" << pkt.meta1 << ", " << pkt.meta2 << "]";
}

void append_shared_buffer_switch_states(ostream& os,
                                        const cid_t& switch_id,
                                        const vector<SharedBufferSwitchState>& states) {
    os << switch_id << endl;
    for (unsigned int t = 0; t < states.size(); t++) {
        const SharedBufferSwitchState& state = states[t];
        os << "  T=" << t << endl;
        os << "    ingress:";
        for (unsigned int q = 0; q < state.ingress.size(); q++) {
            const SharedBufferIngressState& ingress = state.ingress[q];
            os << " q" << q << "(paused=" << ingress.ingress_paused << ", vis=";
            append_packet_snapshot(os, ingress.queue_ctrl.vis_pkt);
            os << ", txP=" << ingress.queue_ctrl.pause_tx
               << ", txR=" << ingress.queue_ctrl.resume_tx
               << ", rxP=" << ingress.queue_ctrl.pause_rx
               << ", rxR=" << ingress.queue_ctrl.resume_rx
               << ", sp=" << ingress.queue_ctrl.sender_paused << ")";
        }
        os << endl;

        os << "    egress:";
        for (unsigned int e = 0; e < state.egress.size(); e++) {
            const SharedBufferEgressState& egress = state.egress[e];
            os << " e" << e << "(sp=" << egress.sender_paused << ", can_emit=" << egress.can_emit
               << ", has_pkt=" << egress.has_pkt << ", blocked=" << egress.blocked
               << ", min_stamp=" << egress.min_stamp << ")";
        }
        os << endl;

        for (unsigned int k = 0; k < state.microstates.size(); k++) {
            const SharedBufferMicroState& micro = state.microstates[k];
            os << "    k=" << k << " active_q=" << micro.active_q
               << " totals(in=" << micro.ingress_pool_total << ", hdr=" << micro.headroom_pool_total
               << ", eg=" << micro.egress_lossless_pool_total << ")" << endl;
            os << "      ingress_occ:";
            for (unsigned int q = 0; q < micro.ingress_pool_occ.size(); q++) {
                os << " q" << q << "=" << micro.ingress_pool_occ[q];
            }
            os << endl;
            os << "      headroom_occ:";
            for (unsigned int q = 0; q < micro.headroom_pool_occ.size(); q++) {
                os << " q" << q << "=" << micro.headroom_pool_occ[q];
            }
            os << endl;
            os << "      egress_occ:";
            for (unsigned int e = 0; e < micro.egress_lossless_pool_occ.size(); e++) {
                os << " e" << e << "=" << micro.egress_lossless_pool_occ[e];
            }
            os << endl;
            os << "      slots:";
            for (unsigned int b = 0; b < micro.slots.size(); b++) {
                const SharedBufferSlotState& slot = micro.slots[b];
                os << " b" << b << "=";
                append_packet_snapshot(os, slot.pkt);
                os << "(in=" << slot.ingress << ", e=" << slot.egress << ", st=" << slot.stamp;
                if (slot.read) os << ", R";
                if (slot.write) os << ", Wq" << slot.write_q;
                os << ")";
            }
            os << endl;
        }
    }
}
} // namespace

ostream& operator<<(ostream& os, const Example& eg) {
    os << "total time: " << eg.total_time << endl;
    os << eg.query_qid << endl;

    os << "enqs:" << endl;
    for (map<cid_t, vector<unsigned int>>::const_iterator it = eg.enqs.begin(); it != eg.enqs.end();
         it++) {
        os << it->first << " ";
        for (unsigned int t = 0; t < it->second.size(); t++) {
            os << it->second[t] << " ";
        }
        os << endl;
    }

    os << "deqs:" << endl;
    for (map<cid_t, vector<unsigned int>>::const_iterator it = eg.deqs.begin(); it != eg.deqs.end();
         it++) {
        os << it->first << " ";
        for (unsigned int t = 0; t < it->second.size(); t++) {
            os << it->second[t] << " ";
        }
        os << endl;
    }

    os << "demands:" << endl;
    for (map<cid_t, vector<unsigned int>>::const_iterator it = eg.demands.begin();
         it != eg.demands.end();
         it++) {
        os << it->first << " ";
        for (unsigned int t = 0; t < it->second.size(); t++) {
            os << it->second[t] << " ";
        }
        os << endl;
    }

    os << "pkts metadata: " << endl;
    for (map<cid_t, vector<vector<int>>>::const_iterator it = eg.enqs_meta1.begin();
         it != eg.enqs_meta1.end();
         it++) {
        cid_t qid = it->first;
        os << qid << " ";
        for (unsigned int t = 0; t < it->second.size(); t++) {
            vector<int> meta1 = it->second[t];
            vector<int> meta2 = eg.enqs_meta2.at(qid)[t];
            for (unsigned int p = 0; p < meta1.size(); p++) {
                os << "[" << meta1[p] << ", " << meta2[p] << "] ";
            }
            os << " | ";
        }
        os << endl;
    }

    return os;
}


void write_examples_to_file(deque<Example*>& examples, string fname) {
    ofstream out;
    out.open(fname);

    for (unsigned int i = 0; i < examples.size(); i++) {
        Example* eg = examples[i];

        out << eg->query_qid << " ";
        out << eg->total_time << " ";
        out << eg->enqs.size() << endl;

        for (map<cid_t, vector<unsigned int>>::iterator it = eg->enqs.begin(); it != eg->enqs.end();
             it++) {
            out << it->first << " ";
            for (unsigned int t = 0; t < it->second.size(); t++) {
                out << it->second[t] << " ";
            }
            out << endl;
        }

        for (map<cid_t, vector<vector<int>>>::iterator it = eg->enqs_meta1.begin();
             it != eg->enqs_meta1.end();
             it++) {
            out << it->first << " ";
            for (unsigned int t = 0; t < it->second.size(); t++) {
                vector<int> meta = it->second[t];
                for (unsigned int p = 0; p < meta.size(); p++) {
                    out << meta[p] << " ";
                }
                out << "| ";
            }
            out << endl;
        }

        for (map<cid_t, vector<vector<int>>>::iterator it = eg->enqs_meta2.begin();
             it != eg->enqs_meta2.end();
             it++) {
            out << it->first << " ";
            for (unsigned int t = 0; t < it->second.size(); t++) {
                vector<int> meta = it->second[t];
                for (unsigned int p = 0; p < meta.size(); p++) {
                    out << meta[p] << " ";
                }
                out << "| ";
            }
            out << endl;
        }

        for (map<cid_t, vector<unsigned int>>::iterator it = eg->deqs.begin(); it != eg->deqs.end();
             it++) {
            out << it->first << " ";
            for (unsigned int t = 0; t < it->second.size(); t++) {
                out << it->second[t] << " ";
            }
            out << endl;
        }

        for (map<cid_t, vector<unsigned int>>::iterator it = eg->enqs.begin(); it != eg->enqs.end();
             it++) {
            cid_t qid = it->first;
            out << qid << " ";
            if (eg->demands.find(qid) != eg->demands.end()) {
                vector<unsigned int>& ds = eg->demands[qid];
                for (unsigned int t = 0; t < ds.size(); t++) {
                    out << ds[t] << " ";
                }
            } else {
                // Backward-compatible default: if demands is absent, use enqs.
                vector<unsigned int>& enqs = eg->enqs[qid];
                for (unsigned int t = 0; t < enqs.size(); t++) {
                    out << enqs[t] << " ";
                }
            }
            out << endl;
        }
    }
    out.close();
}

vector<unsigned int> u_split_line(string line) {
    vector<unsigned int> parts;

    size_t next_part = line.find(" ", 0);
    while (next_part != string::npos) {
        string part = line.substr(0, next_part);
        if (part.size() > 0) parts.push_back((unsigned int) stoi(part));
        line = line.substr(next_part + 1);
        next_part = line.find(" ", 0);
    }
    if (line.size() > 0) parts.push_back((unsigned int) stoi(line));
    return parts;
}

vector<int> s_split_line(string line) {
    vector<int> parts;

    size_t next_part = line.find(" ", 0);
    while (next_part != string::npos) {
        string part = line.substr(0, next_part);
        if (part.size() > 0) parts.push_back(stoi(part));
        line = line.substr(next_part + 1);
        next_part = line.find(" ", 0);
    }
    if (line.size() > 0) parts.push_back(stoi(line));
    return parts;
}


/*
 File format:
 query_qid total_time num_queues
 num_queue lines for enqs:
 qid enq1 enq2 ...
 num_queue lines for enqs_meta1:
 qid meta00 meta01 meta02 ... | meta10 meta11 ...
 num_queue lines in the format above for enqs_meta2
 num_queue lines for deqs:
 qid deq1 deq2 ...
 num_queue lines for demands (optional in old files):
 qid demand1 demand2 ...
 */
void read_examples_from_file(deque<Example*>& examples, string fname) {
    ifstream in;
    in.open(fname);

    if (!in.is_open()) {
        cout << "Cannot open file: " << fname << endl;
        return;
    }
    string line;
    bool has_pending_line = false;
    string pending_line;
    while (true) {
        if (has_pending_line) {
            line = pending_line;
            has_pending_line = false;
        } else {
            if (!getline(in, line)) break;
        }
        if (line.size() == 0) continue;

        Example* eg = new Example();

        size_t next_part = line.find(" ", 0);
        eg->query_qid = line.substr(0, next_part);
        line = line.substr(next_part + 1);

        vector<unsigned int> u_parts = u_split_line(line);
        if (u_parts.size() != 2) {
            cout << "Bad input file format" << endl;
            in.close();
            return;
        }

        eg->total_time = u_parts[0];
        unsigned int num_queues = u_parts[1];

        for (unsigned int i = 0; i < num_queues; i++) {
            getline(in, line);

            size_t next_part = line.find(" ", 0);
            cid_t qid = line.substr(0, next_part);
            line = line.substr(next_part + 1);

            u_parts = u_split_line(line);

            if (u_parts.size() != eg->total_time) {
                cout << "Bad input file format" << endl;
                in.close();
                return;
            }

            eg->enqs[qid] = u_parts;
        }

        for (unsigned int i = 0; i < num_queues; i++) {
            getline(in, line);
            size_t next_part = line.find(" ", 0);
            cid_t qid = line.substr(0, next_part);
            line = line.substr(next_part + 1);

            vector<vector<int>> enqs_meta1;
            for (unsigned int t = 0; t < eg->total_time; t++) {
                size_t next_part = line.find("|", 0);

                if (next_part == string::npos) {
                    cout << "Bad input file format" << endl;
                    in.close();
                    return;
                }

                string enqs_meta_str = line.substr(0, next_part);
                line = line.substr(next_part + 1);

                vector<int> s_parts = s_split_line(enqs_meta_str);
                enqs_meta1.push_back(s_parts);
            }

            eg->enqs_meta1[qid] = enqs_meta1;
        }

        for (unsigned int i = 0; i < num_queues; i++) {
            getline(in, line);
            size_t next_part = line.find(" ", 0);
            cid_t qid = line.substr(0, next_part);
            line = line.substr(next_part + 1);

            vector<vector<int>> enqs_meta2;
            for (unsigned int t = 0; t < eg->total_time; t++) {
                size_t next_part = line.find("|", 0);

                if (next_part == string::npos) {
                    cout << "Bad input file format" << endl;
                    in.close();
                    return;
                }

                string enqs_meta_str = line.substr(0, next_part);
                line = line.substr(next_part + 1);

                vector<int> s_parts = s_split_line(enqs_meta_str);
                enqs_meta2.push_back(s_parts);
            }

            eg->enqs_meta2[qid] = enqs_meta2;
        }

        for (unsigned int i = 0; i < num_queues; i++) {
            getline(in, line);

            size_t next_part = line.find(" ", 0);
            cid_t qid = line.substr(0, next_part);
            line = line.substr(next_part + 1);

            u_parts = u_split_line(line);

            if (u_parts.size() != eg->total_time) {
                cout << "Bad input file format" << endl;
                in.close();
                return;
            }

            eg->deqs[qid] = u_parts;
        }

        // Read optional demands section.
        // New format has num_queues demand lines.
        // Old format has no demand lines; in that case fallback to enqs.
        bool parsed_demands = false;
        if (getline(in, line)) {
            if (line.size() > 0) {
                size_t next_part = line.find(" ", 0);
                if (next_part != string::npos) {
                    cid_t first_qid = line.substr(0, next_part);
                    string first_tail = line.substr(next_part + 1);
                    vector<unsigned int> first_parts = u_split_line(first_tail);

                    if (first_parts.size() == eg->total_time) {
                        // This is a demand line in the new format.
                        eg->demands[first_qid] = first_parts;
                        for (unsigned int i = 1; i < num_queues; i++) {
                            if (!getline(in, line)) {
                                cout << "Bad input file format" << endl;
                                in.close();
                                return;
                            }
                            next_part = line.find(" ", 0);
                            if (next_part == string::npos) {
                                cout << "Bad input file format" << endl;
                                in.close();
                                return;
                            }
                            cid_t qid = line.substr(0, next_part);
                            string tail = line.substr(next_part + 1);
                            vector<unsigned int> demand_parts = u_split_line(tail);
                            if (demand_parts.size() != eg->total_time) {
                                cout << "Bad input file format" << endl;
                                in.close();
                                return;
                            }
                            eg->demands[qid] = demand_parts;
                        }
                        parsed_demands = true;
                    } else {
                        // This line belongs to the next example in old-format files.
                        has_pending_line = true;
                        pending_line = line;
                    }
                } else {
                    has_pending_line = true;
                    pending_line = line;
                }
            }
        }

        if (!parsed_demands) {
            for (map<cid_t, vector<unsigned int>>::const_iterator it = eg->enqs.begin();
                 it != eg->enqs.end();
                 it++) {
                eg->demands[it->first] = it->second;
            }
        }

        examples.push_back(eg);
    }
}

ostream& operator<<(ostream& os, const IndexedExample& eg) {
    os << "total time: " << eg.total_time << endl;
    os << eg.query_qid << endl;

    os << "enqs:" << endl;
    for (unsigned int i = 0; i < eg.enqs.size(); i++) {
        os << i << " ";
        for (unsigned int t = 0; t < eg.enqs[i].size(); t++) {
            os << eg.enqs[i][t] << " ";
        }
        os << endl;
    }

    os << "deqs:" << endl;
    for (unsigned int i = 0; i < eg.deqs.size(); i++) {
        os << i << " ";
        for (unsigned int t = 0; t < eg.deqs[i].size(); t++) {
            os << eg.deqs[i][t] << " ";
        }
        os << endl;
    }

    os << "demands:" << endl;
    for (unsigned int i = 0; i < eg.demands.size(); i++) {
        os << i << " ";
        for (unsigned int t = 0; t < eg.demands[i].size(); t++) {
            os << eg.demands[i][t] << " ";
        }
        os << endl;
    }

    os << "pkts metadata: " << endl;
    for (unsigned int i = 0; i < eg.enqs_meta1.size(); i++) {
        os << i << " ";
        for (unsigned int t = 0; t < eg.enqs_meta1[i].size(); t++) {
            vector<int> meta1 = eg.enqs_meta1[i][t];
            vector<int> meta2 = eg.enqs_meta2[i][t];
            for (unsigned int p = 0; p < meta1.size(); p++) {
                os << "[" << meta1[p] << ", " << meta2[p] << "] ";
            }
            os << " | ";
        }
        os << endl;
    }

    return os;
}

ostream& operator<<(ostream& os, const SBExample& eg) {
    os << static_cast<const Example&>(eg);
    if (!eg.shared_buffer_states.empty()) {
        os << "shared buffer states:" << endl;
        for (map<cid_t, vector<SharedBufferSwitchState>>::const_iterator it =
                 eg.shared_buffer_states.begin();
             it != eg.shared_buffer_states.end();
             it++) {
            append_shared_buffer_switch_states(os, it->first, it->second);
        }
    }
    return os;
}

ostream& operator<<(ostream& os, const SBIndexedExample& eg) {
    os << "total time: " << eg.total_time << endl;
    os << "query: " << eg.query_quant << " " << eg.query_lhs;
    if (!eg.query_metric.empty()) {
        os << " " << eg.query_metric;
    }
    if (!eg.query_op.empty()) {
        os << " " << eg.query_op << " " << eg.query_thresh;
    }
    os << " on " << eg.query_time_range << endl;
    if (!eg.input_queue_ids.empty() && eg.input_queue_ids.size() == eg.enqs.size()) {
        os << "input queues:";
        for (unsigned int i = 0; i < eg.input_queue_ids.size(); i++) {
            os << " q" << i << "=" << eg.input_queue_ids[i];
        }
        os << endl;
    }

    os << "enqs:" << endl;
    for (unsigned int i = 0; i < eg.enqs.size(); i++) {
        os << "q" << i;
        if (i < eg.input_queue_ids.size()) os << " (" << eg.input_queue_ids[i] << ")";
        os << ": ";
        for (unsigned int t = 0; t < eg.enqs[i].size(); t++) {
            os << eg.enqs[i][t] << " ";
        }
        os << endl;
    }

    os << "deqs:" << endl;
    for (unsigned int i = 0; i < eg.deqs.size(); i++) {
        os << "q" << i;
        if (i < eg.input_queue_ids.size()) os << " (" << eg.input_queue_ids[i] << ")";
        os << ": ";
        for (unsigned int t = 0; t < eg.deqs[i].size(); t++) {
            os << eg.deqs[i][t] << " ";
        }
        os << endl;
    }

    os << "demands:" << endl;
    for (unsigned int i = 0; i < eg.demands.size(); i++) {
        os << "q" << i;
        if (i < eg.input_queue_ids.size()) os << " (" << eg.input_queue_ids[i] << ")";
        os << ": ";
        for (unsigned int t = 0; t < eg.demands[i].size(); t++) {
            os << eg.demands[i][t] << " ";
        }
        os << endl;
    }

    os << "pkts metadata:" << endl;
    for (unsigned int i = 0; i < eg.enqs_meta1.size(); i++) {
        os << "q" << i;
        if (i < eg.input_queue_ids.size()) os << " (" << eg.input_queue_ids[i] << ")";
        os << ": ";
        for (unsigned int t = 0; t < eg.enqs_meta1[i].size(); t++) {
            vector<int> meta1 = eg.enqs_meta1[i][t];
            vector<int> meta2 = eg.enqs_meta2[i][t];
            for (unsigned int p = 0; p < meta1.size(); p++) {
                os << "[" << meta1[p] << ", " << meta2[p] << "] ";
            }
            os << "| ";
        }
        os << endl;
    }

    if (!eg.shared_buffer_switch_ids.empty() &&
        eg.shared_buffer_switch_ids.size() == eg.shared_buffer_states.size()) {
        os << "shared buffer switches:";
        for (unsigned int i = 0; i < eg.shared_buffer_switch_ids.size(); i++) {
            os << " " << eg.shared_buffer_switch_ids[i];
        }
        os << endl;
        os << "shared buffer states:" << endl;
        for (unsigned int i = 0; i < eg.shared_buffer_switch_ids.size(); i++) {
            append_shared_buffer_switch_states(
                os, eg.shared_buffer_switch_ids[i], eg.shared_buffer_states[i]);
        }
    }
    return os;
}

ostream& operator<<(ostream& os, const Trace& tr) {
    os << "total time: " << tr.total_time << endl;

    os << "enqs:" << endl;
    for (unsigned int i = 0; i < tr.enqs.size(); i++) {
        os << i << " ";
        for (unsigned int t = 0; t < tr.enqs[i].size(); t++) {
            os << tr.enqs[i][t] << " ";
        }
        os << endl;
    }
    return os;
}
