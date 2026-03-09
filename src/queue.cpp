//
//  Queue.cpp
//  FPerf
//
//  Created by Mina Tahmasbi Arashloo on 11/5/20.
//  Copyright © 2020 Mina Tahmasbi Arashloo. All rights reserved.
//

#include "queue.hpp"

#include <iostream>
#include <sstream>

namespace {
void append_pkt_slots(stringstream& ss,
                      model& m,
                      NetContext& net_ctx,
                      vector<expr>* pkts,
                      int hi,
                      unsigned int t) {
    for (int i = hi; i >= 0; i--) {
        expr val = m.eval(net_ctx.pkt2val(pkts[i][t]));
        if (val.is_true()) {
            ss << "[";
            expr meta1 = m.eval(net_ctx.pkt2meta1(pkts[i][t]));
            if (meta1.is_numeral())
                ss << meta1.get_numeral_int() << " ";
            else
                ss << "- ";

            expr meta2 = m.eval(net_ctx.pkt2meta2(pkts[i][t]));
            if (meta2.is_numeral())
                ss << meta2.get_numeral_int();
            else
                ss << "-";

            ss << "] ";
        } else {
            ss << "x ";
        }
    }
}

int eval_bool_as_int(model& m, const expr& e) {
    expr val = m.eval(e);
    return val.is_true() ? 1 : 0;
}
} // namespace

/* ********************* Queue *********************** */

Queue::Queue(cid_t module_id,
             cid_t queue_id,
             unsigned int size,
             unsigned int max_enq,
             unsigned int max_deq,
             unsigned int total_time,
             NetContext& net_ctx):
size_(size),
max_enq_(max_enq),
max_deq_(max_deq),
total_time(total_time) {
    if (max_deq_ > size_) {
        cout << "Queue::Queue: Invalid configuration parameters" << endl;
    }
    elems_ = new vector<expr>[size_];
    enqs_ = new vector<expr>[max_enq_];

    id = get_unique_id(module_id, queue_id);

    add_vars(net_ctx);
}

unsigned int Queue::size() {
    return size_;
}

unsigned int Queue::max_enq() {
    return max_enq_;
}

unsigned int Queue::max_deq() {
    return max_deq_;
}

void Queue::sliding_window_vars(NetContext& net_ctx) {
    tmp_val = new vector<expr>[size_];
    // Temporary valid bits
    for (unsigned int p = 0; p < size_; p++) {
        for (unsigned int t = 0; t < total_time; t++) {
            char vname[100];
            snprintf(vname, 100, "%s_tmp_val[%d][%d]", id.c_str(), p, t);
            tmp_val[p].push_back(net_ctx.bool_const(vname));
        }
    }
}

void Queue::add_vars(NetContext& net_ctx) {
    // Queue elements
    for (unsigned int p = 0; p < size_; p++) {
        for (unsigned int t = 0; t < total_time; t++) {
            char vname[100];
            snprintf(vname, 100, "%s_elem[%d][%d]", id.c_str(), p, t);
            elems_[p].push_back(net_ctx.pkt_const(vname));
        }
    }

    // Packets to enqueue
    for (unsigned int p = 0; p < max_enq_; p++) {
        for (unsigned int t = 0; t < total_time; t++) {
            char vname[100];
            snprintf(vname, 100, "%s_enq[%d][%d]", id.c_str(), p, t);
            enqs_[p].push_back(net_ctx.pkt_const(vname));
        }
    }

    // Number of packets to enqueue
    for (unsigned int t = 0; t < total_time; t++) {
        char vname[100];
        snprintf(vname, 100, "%s_enq_cnt[%d]", id.c_str(), t);
        enq_cnt_.push_back(net_ctx.int_const(vname));
    }

    // Number of packets to dequeue
    for (unsigned int t = 0; t < total_time; t++) {
        char vname[100];
        snprintf(vname, 100, "%s_deq_cnt[%d]", id.c_str(), t);
        deq_cnt_.push_back(net_ctx.int_const(vname));
    }

    sliding_window_vars(net_ctx);
}

void Queue::sliding_window_constrs(NetContext& net_ctx, map<string, expr>& constr_map) {
    char /**/ constr_name[100];

    // taking care of enqs and deqs
    for (unsigned int t = 0; t < total_time; t++) {
        // For packet i, assign it the tmp validity of
        // the packet at (i + deq_cnt). If (i + deq_cnt)
        // is outside of queue bounds, assign it to false
        for (unsigned int p = 0; p < size_; p++) {
            for (unsigned int d = 0; d <= max_deq_; d++) {
                if (p + d < size_) {
                    snprintf(
                        constr_name, 100, "%s_tmp_val[%d][%d]_small_d_%d", id.c_str(), p, t, d);
                    expr constr_expr = implies(deq_cnt_[t] == (int) d,
                                               tmp_val[p][t] == net_ctx.pkt2val(elems_[p + d][t]));
                    constr_map.insert(named_constr(constr_name, constr_expr));
                } else {
                    snprintf(
                        constr_name, 100, "%s_tmp_val[%d][%d]_large_d_%d", id.c_str(), p, t, d);
                    expr constr_expr = implies(deq_cnt_[t] == (int) d, !tmp_val[p][t]);
                    constr_map.insert(named_constr(constr_name, constr_expr));
                }
            }
        }

        // initially all packet are null
        if (t == 0) {
            for (unsigned int p = 0; p < size_; p++) {
                snprintf(constr_name, 100, "%s[%d]_null_at_0", id.c_str(), p);
                expr constr_expr = elems_[p][0] == net_ctx.null_pkt();
                constr_map.insert(named_constr(constr_name, constr_expr));
            }
        }
        // At t + 1, if packet i's tmp_val was valid,
        // it will be assigned to the packet at i + deq_cnt.
        // If not, go over a sliding window of max_enq + 1 elements
        // (and smaller windows at the tail). If the first is valid
        // and the rest are invalid (using tmp_val), enqueue in those.
        // To catch the null ones, we can say if max_enq elements ago
        // was also null, then this one is not going to get any of
        // the enqueues and will stay null.
        else {
            // If it stays valid after dequeue, shift forward
            unsigned int prev_t = t - 1;
            for (unsigned int p = 0; p < size_; p++) {
                unsigned int max_relevant_d = min(max_deq_, size_ - p - 1);
                for (unsigned int d = 0; d <= max_relevant_d; d++) {
                    snprintf(constr_name, 100, "%s[%d][%d]_shift_forward_%d", id.c_str(), p, t, d);
                    expr constr_expr = implies(tmp_val[p][prev_t] && deq_cnt_[prev_t] == (int) d,
                                               elems_[p][t] == elems_[p + d][prev_t]);
                    constr_map.insert(named_constr(constr_name, constr_expr));
                }
            }

            // Sliding window

            for (unsigned int i = 0; i < max_enq_; i++) {
                snprintf(constr_name, 100, "%s[%d][%d]_gets_enqs[%d]", id.c_str(), i, t, i);
                expr constr_expr = implies(!tmp_val[0][prev_t], elems_[i][t] == enqs_[i][prev_t]);
                constr_map.insert(named_constr(constr_name, constr_expr));
            }

            for (unsigned int p = 0; p < size_ - 1; p++) {
                expr is_enq_wind = tmp_val[p][prev_t] && !tmp_val[p + 1][prev_t];
                for (unsigned int i = 1; i <= max_enq_; i++) {
                    if (p + i < size_) {
                        snprintf(constr_name,
                                 100,
                                 "%s[%d][%d]_gets_enqs[%d]",
                                 id.c_str(),
                                 p + i,
                                 t,
                                 i - 1);
                        expr constr_expr = implies(is_enq_wind,
                                                   elems_[p + i][t] == enqs_[i - 1][prev_t]);
                        constr_map.insert(named_constr(constr_name, constr_expr));
                    }
                }
            }

            for (unsigned int p = 0; p < size_ - max_enq_; p++) {
                snprintf(constr_name, 100, "%s[%d][%d]_gets_null", id.c_str(), p + max_enq_, t);
                expr constr_expr = implies(!tmp_val[p][prev_t],
                                           elems_[p + max_enq_][t] == net_ctx.null_pkt());
                constr_map.insert(named_constr(constr_name, constr_expr));
            }
        }


        // No holes in enqs
        for (unsigned int p = 0; p < max_enq_ - 1; p++) {
            expr enq1_val = net_ctx.pkt2val(enqs_[p][t]);
            expr enq2_val = net_ctx.pkt2val(enqs_[p + 1][t]);
            snprintf(constr_name, 100, "%s_no_enq_holes_%d_%d", id.c_str(), p, t);
            expr constr_expr = enq1_val || !enq2_val;
            constr_map.insert(named_constr(constr_name, constr_expr));
        }

        // Deriving enq_cnt from enqs (becuase there are no "holes" in the enq sequence)

        expr_vector enq_cnt_vec(net_ctx.z3_ctx());
        enq_cnt_vec.push_back(enq_cnt_[t] >= 0);
        enq_cnt_vec.push_back(enq_cnt_[t] <= (int) max_enq_);
        for (unsigned int i = 0; i < max_enq_; i++) {
            enq_cnt_vec.push_back(implies(net_ctx.pkt2val(enqs_[i][t]), enq_cnt_[t] > (int) i));
            enq_cnt_vec.push_back(implies(!net_ctx.pkt2val(enqs_[i][t]), enq_cnt_[t] <= (int) i));
        }
        snprintf(constr_name, 100, "%s_enq_cnt_bounds[%d]", id.c_str(), t);
        expr constr_expr = mk_and(enq_cnt_vec);
        constr_map.insert(named_constr(constr_name, constr_expr));

        //        for (unsigned int p = 0; p < max_enq_ - 1; p++){
        //            expr enq1_val = net_ctx.pkt2val(enqs_[p][t]);
        //            expr enq2_val = net_ctx.pkt2val(enqs_[p + 1][t]);
        //            snprintf(constr_name,100, "%s_enq_cnt[%d]_is_%d", id.c_str(), t, p + 1);
        //            expr constr_expr = implies(enq1_val && !enq2_val,
        //                                       enq_cnt_[t] == (int) p + 1);
        //            constr_map.insert(named_constr(constr_name, constr_expr));
        //        }
        //        snprintf(constr_name,100, "%s_enq_cnt[%d]_is_0", id.c_str(), t);
        //        expr constr_expr = implies(!net_ctx.pkt2val(enqs_[0][t]),
        //                                   enq_cnt_[t] == 0);
        //        constr_map.insert(named_constr(constr_name, constr_expr));
        //
        //        snprintf(constr_name,100, "%s_enq_cnt[%d]_is_%d", id.c_str(), t, max_enq_);
        //        constr_expr = implies(net_ctx.pkt2val(enqs_[max_enq_ - 1][t]),
        //                                   enq_cnt_[t] == (int) max_enq_);
        //        constr_map.insert(named_constr(constr_name, constr_expr));

        // deq_cnt should not be greater than the size of the queue
        expr_vector deq_cnt_ub_vec(net_ctx.z3_ctx());
        // TODO: is it ok not to have this?
        for (unsigned int p = 0; p < size_; p++) {
            deq_cnt_ub_vec.push_back(
                implies(!net_ctx.pkt2val(elems_[p][t]), deq_cnt_[t] <= (int) p));
        }
        deq_cnt_ub_vec.push_back(deq_cnt_[t] <= (int) max_deq_);

        snprintf(constr_name, 100, "%s_deq_cnt_bounds[%d]", id.c_str(), t);
        constr_expr = mk_and(deq_cnt_ub_vec);
        constr_map.insert(named_constr(constr_name, constr_expr));

        // deq_cnt should be greater than or equal to zero
        snprintf(constr_name, 100, "%s_deq_cnt_gt_zero[%d]", id.c_str(), t);
        constr_expr = deq_cnt_[t] >= 0;
        constr_map.insert(named_constr(constr_name, constr_expr));

        // no holes
        for (unsigned int p = 0; p < size_ - 1; p++) {
            expr elem1_val = net_ctx.pkt2val(elems_[p][t]);
            expr elem2_val = net_ctx.pkt2val(elems_[p + 1][t]);
            snprintf(constr_name, 100, "%s_no_holes_%d_%d", id.c_str(), p, t);
            constr_expr = elem1_val || !elem2_val;
            constr_map.insert(named_constr(constr_name, constr_expr));
        }
    }
}

void Queue::add_constrs(NetContext& net_ctx, map<string, expr>& constr_map) {
    sliding_window_constrs(net_ctx, constr_map);
}

vector<expr>& Queue::operator[](int ind) {
    return elems_[ind];
}

vector<expr>& Queue::elem(int ind) {
    return elems_[ind];
}

vector<expr>& Queue::enqs(unsigned int ind) {
    return enqs_[ind];
}

expr& Queue::enq_cnt(unsigned int time) {
    return enq_cnt_[time];
}

expr& Queue::deq_cnt(unsigned int time) {
    return deq_cnt_[time];
}

void Queue::add_metric(metric_t metric_type, Metric* m) {
    if (metrics.find(metric_type) != metrics.end()) {
        cout << "ERROR: " << metric_type << " already in queue " << id << endl;
        return;
    }
    metrics[metric_type] = m;
}

Metric* Queue::get_metric(metric_t metric_type) {
    map<metric_t, Metric*>::iterator it = metrics.find(metric_type);
    if (it == metrics.end()) {
        // TODO: Uncomment this after parametrizing search
        //        cout << "ERROR: " << "queue " << id << " does not have a " << metric_type <<
        //        endl;
        return NULL;
    }
    return metrics[metric_type];
}

cid_t Queue::get_id() const {
    return id;
}

string Queue::get_model_str(model& m, NetContext& net_ctx, unsigned int t) {
    stringstream ss;
    ss << get_id() << ": ";

    append_pkt_slots(ss, m, net_ctx, enqs_, (int) max_enq() - 1, t);
    ss << "|";
    ss << "-> ";
    append_pkt_slots(ss, m, net_ctx, elems_, (int) size_ - 1, t);
    ss << "->";
    ss << " (" << m.eval(enq_cnt(t)).get_numeral_int() << ", "
       << m.eval(deq_cnt(t)).get_numeral_int() << ") ";
    return ss.str();
}

ostream& operator<<(ostream& os, const Queue& q) {
    os << q.get_id();
    return os;
}

/* ********************* ImmQueue *********************** */

ImmQueue::ImmQueue(cid_t module_id,
                   cid_t queue_id,
                   unsigned int size,
                   unsigned int max_enq,
                   unsigned int max_deq,
                   unsigned int total_time,
                   NetContext& net_ctx):

Queue(module_id, queue_id, size, max_enq, max_deq, total_time, net_ctx) {
    /*
    elems_ = new vector<expr>[size_];
    enqs_ = new vector<expr>[max_enq_];
    tmp_val = new vector<expr>[size_];

    id = get_unique_id(module_id, queue_id);

    add_vars(net_ctx);
    */
}

void ImmQueue::sliding_window_constrs(NetContext& net_ctx, map<string, expr>& constr_map) {
    char constr_name[100];

    // taking care of enqs and deqs
    for (unsigned int t = 0; t < total_time; t++) {
        // For packet i, assign it the tmp validity of
        // the packet at (i + deq_cnt). If (i + deq_cnt)
        // is outside of queue bounds, assign it to false
        for (unsigned int p = 0; p < size_; p++) {
            for (unsigned int d = 0; d <= max_deq_; d++) {
                if (p + d < size_) {
                    snprintf(
                        constr_name, 100, "%s_tmp_val[%d][%d]_small_d_%d", id.c_str(), p, t, d);
                    expr constr_expr = implies(deq_cnt_[t] == (int) d,
                                               tmp_val[p][t] == net_ctx.pkt2val(elems_[p + d][t]));
                    constr_map.insert(named_constr(constr_name, constr_expr));
                } else {
                    snprintf(
                        constr_name, 100, "%s_tmp_val[%d][%d]_large_d_%d", id.c_str(), p, t, d);
                    expr constr_expr = implies(deq_cnt_[t] == (int) d, !tmp_val[p][t]);
                    constr_map.insert(named_constr(constr_name, constr_expr));
                }
            }
        }

        // initially packets in [0, max_enq] are the same as the enqs,
        // and the rest are null
        if (t == 0) {
            for (unsigned int p = 0; p < max_enq_; p++) {
                snprintf(constr_name, 100, "%s[%d]_is_enqs[%d]", id.c_str(), p, p);
                expr constr_expr = elems_[p][0] == enqs_[p][0];
                constr_map.insert(named_constr(constr_name, constr_expr));
            }
            for (unsigned int p = max_enq_; p < size_; p++) {
                snprintf(constr_name, 100, "%s[%d]_null_at_0", id.c_str(), p);
                expr constr_expr = elems_[p][0] == net_ctx.null_pkt();
                constr_map.insert(named_constr(constr_name, constr_expr));
            }
        }
        // At t + 1, if packet i's tmp_val was valid,
        // it will be assigned to the packet at i + deq_cnt.
        // If not, go over a sliding window of max_enq + 1 elements
        // (and smaller windows at the tail). If the first is valid
        // and the rest are invalid (using tmp_val), enqueue in those.
        // To catch the null ones, we can say if max_enq elements ago
        // was also null, then this one is not going to get any of
        // the enqueues and will stay null.
        else {
            // If it stays valid after dequeue, shift forward
            unsigned int prev_t = t - 1;
            for (unsigned int p = 0; p < size_; p++) {
                unsigned int max_relevant_d = min(max_deq_, size_ - p - 1);
                for (unsigned int d = 0; d <= max_relevant_d; d++) {
                    snprintf(constr_name, 100, "%s[%d][%d]_shift_forward_%d", id.c_str(), p, t, d);
                    expr constr_expr = implies(tmp_val[p][prev_t] && deq_cnt_[prev_t] == (int) d,
                                               elems_[p][t] == elems_[p + d][prev_t]);
                    constr_map.insert(named_constr(constr_name, constr_expr));
                }
            }

            // Sliding window

            for (unsigned int i = 0; i < max_enq_; i++) {
                snprintf(constr_name, 100, "%s[%d][%d]_gets_enqs[%d]", id.c_str(), i, t, i);
                expr constr_expr = implies(!tmp_val[0][prev_t], elems_[i][t] == enqs_[i][t]);
                constr_map.insert(named_constr(constr_name, constr_expr));
            }

            for (unsigned int p = 0; p < size_ - 1; p++) {
                expr is_enq_wind = tmp_val[p][prev_t] && !tmp_val[p + 1][prev_t];
                for (unsigned int i = 1; i <= max_enq_; i++) {
                    if (p + i < size_) {
                        snprintf(constr_name,
                                 100,
                                 "%s[%d][%d]_gets_enqs[%d]",
                                 id.c_str(),
                                 p + i,
                                 t,
                                 i - 1);
                        expr constr_expr = implies(is_enq_wind,
                                                   elems_[p + i][t] == enqs_[i - 1][t]);
                        constr_map.insert(named_constr(constr_name, constr_expr));
                    }
                }
            }

            for (unsigned int p = 0; p < size_ - max_enq_; p++) {
                snprintf(constr_name, 100, "%s[%d][%d]_gets_null", id.c_str(), p + max_enq_, t);
                expr constr_expr = implies(!tmp_val[p][prev_t],
                                           elems_[p + max_enq_][t] == net_ctx.null_pkt());
                constr_map.insert(named_constr(constr_name, constr_expr));
            }
        }

        // No holes in enqs
        //        for (unsigned int p = 0; p < max_enq_ - 1; p++){
        //            expr enq1_val = net_ctx.pkt2val(enqs_[p][t]);
        //            expr enq2_val = net_ctx.pkt2val(enqs_[p + 1][t]);
        //            snprintf(constr_name,100, "%s_no_enq_holes_%d_%d", id.c_str(), p, t);
        //            expr constr_expr = enq1_val || !enq2_val;
        //            constr_map.insert(named_constr(constr_name, constr_expr));
        //        }

        // Deriving enq_cnt from enqs (becuase there are no "holes" in the enq sequence)

        expr_vector enq_cnt_vec(net_ctx.z3_ctx());
        enq_cnt_vec.push_back(enq_cnt_[t] >= 0);
        enq_cnt_vec.push_back(enq_cnt_[t] <= (int) max_enq_);
        for (unsigned int i = 0; i < max_enq_; i++) {
            enq_cnt_vec.push_back(implies(net_ctx.pkt2val(enqs_[i][t]), enq_cnt_[t] > (int) i));
            enq_cnt_vec.push_back(implies(!net_ctx.pkt2val(enqs_[i][t]), enq_cnt_[t] <= (int) i));
        }
        snprintf(constr_name, 100, "%s_enq_cnt_bounds[%d]", id.c_str(), t);
        expr constr_expr = mk_and(enq_cnt_vec);
        constr_map.insert(named_constr(constr_name, constr_expr));

        //        for (unsigned int p = 0; p < max_enq_ - 1; p++){
        //            expr enq1_val = net_ctx.pkt2val(enqs_[p][t]);
        //            expr enq2_val = net_ctx.pkt2val(enqs_[p + 1][t]);
        //            snprintf(constr_name,100, "%s_enq_cnt[%d]_is_%d", id.c_str(), t, p + 1);
        //            expr constr_expr = implies(enq1_val && !enq2_val,
        //                                       enq_cnt_[t] == (int) p + 1);
        //            constr_map.insert(named_constr(constr_name, constr_expr));
        //        }
        //        snprintf(constr_name,100, "%s_enq_cnt[%d]_is_0", id.c_str(), t);
        //        expr constr_expr = implies(!net_ctx.pkt2val(enqs_[0][t]),
        //                                   enq_cnt_[t] == 0);
        //        constr_map.insert(named_constr(constr_name, constr_expr));
        //
        //        snprintf(constr_name,100, "%s_enq_cnt[%d]_is_%d", id.c_str(), t, max_enq_);
        //        constr_expr = implies(net_ctx.pkt2val(enqs_[max_enq_ - 1][t]),
        //                                   enq_cnt_[t] == (int) max_enq_);
        //        constr_map.insert(named_constr(constr_name, constr_expr));

        // deq_cnt should not be greater than the size of the queue
        expr_vector deq_cnt_ub_vec(net_ctx.z3_ctx());
        // TODO: ok not to have this?
        for (unsigned int p = 0; p < size_; p++) {
            deq_cnt_ub_vec.push_back(
                implies(!net_ctx.pkt2val(elems_[p][t]), deq_cnt_[t] <= (int) p));
        }
        deq_cnt_ub_vec.push_back(deq_cnt_[t] <= (int) max_deq_);

        snprintf(constr_name, 100, "%s_deq_cnt_bounds[%d]", id.c_str(), t);
        constr_expr = mk_and(deq_cnt_ub_vec);
        constr_map.insert(named_constr(constr_name, constr_expr));

        // deq_cnt should be greater than or equal to zero
        snprintf(constr_name, 100, "%s_deq_cnt_gt_zero[%d]", id.c_str(), t);
        constr_expr = deq_cnt_[t] >= 0;
        constr_map.insert(named_constr(constr_name, constr_expr));

        // no holes
        for (unsigned int p = 0; p < size_ - 1; p++) {
            expr elem1_val = net_ctx.pkt2val(elems_[p][t]);
            expr elem2_val = net_ctx.pkt2val(elems_[p + 1][t]);
            snprintf(constr_name, 100, "%s_no_holes_%d_%d", id.c_str(), p, t);
            constr_expr = elem1_val || !elem2_val;
            constr_map.insert(named_constr(constr_name, constr_expr));
        }
    }
}

ostream& operator<<(ostream& os, const ImmQueue& q) {
    os << q.get_id();
    return os;
}

/* ********************* Link *********************** */

Link::Link(cid_t module_id, cid_t queue_id, unsigned int total_time, NetContext& net_ctx):
Queue(module_id, queue_id, 1, 1, 1, total_time, net_ctx) {
}

void Link::add_constrs(NetContext& net_ctx, map<string, expr>& constr_map) {

    char constr_name[100];

    for (unsigned int t = 0; t < total_time; t++) {

        // deq_cnt is always one
        snprintf(constr_name, 100, "%s_deq_cnt_is_zero_or_one_at_%d", id.c_str(), t);
        expr pkt_val = net_ctx.pkt2val(elems_[0][t]);
        expr constr_expr = implies(pkt_val, deq_cnt_[t] == 1) &&
                           implies(!pkt_val, deq_cnt_[t] == 0);
        constr_map.insert(named_constr(constr_name, constr_expr));

        // move enq to elem
        if (t == 0) {
            snprintf(constr_name, 100, "%s[0]_is_null", id.c_str());
            constr_expr = elems_[0][0] == net_ctx.null_pkt();
            constr_map.insert(named_constr(constr_name, constr_expr));
        } else {
            unsigned int prev_t = t - 1;
            snprintf(constr_name, 100, "%s[0]_is_enqs[0][%d]_at_%d", id.c_str(), (t - 1), t);
            constr_expr = elems_[0][t] == enqs_[0][prev_t];
            constr_map.insert(named_constr(constr_name, constr_expr));
        }

        // set enq_cnt
        snprintf(constr_name, 100, "%s_enq_cnt_is_one_at_%d", id.c_str(), t);
        constr_expr = implies(net_ctx.pkt2val(enqs_[0][t]), enq_cnt_[t] == 1);
        constr_map.insert(named_constr(constr_name, constr_expr));

        snprintf(constr_name, 100, "%s_enq_cnt_is_zero_at_%d", id.c_str(), t);
        constr_expr = implies(!net_ctx.pkt2val(enqs_[0][t]), enq_cnt_[t] == 0);
        constr_map.insert(named_constr(constr_name, constr_expr));
    }
}


ostream& operator<<(ostream& os, const Link& q) {
    os << q.get_id();
    return os;
}

/* ********************* SBQueue *********************** */

SBQueue::SBQueue(cid_t module_id,
                 cid_t queue_id,
                 unsigned int size,
                 unsigned int max_enq,
                 unsigned int max_deq,
                 unsigned int total_time,
                 NetContext& net_ctx):
Queue(module_id, queue_id, size, max_enq, max_deq, total_time, net_ctx) {
    char vname[120];
    for (unsigned int t = 0; t < total_time; t++) {
        snprintf(vname, sizeof(vname), "%s_pause_tx[%u]", id.c_str(), t);
        pause_tx_.push_back(net_ctx.bool_const(vname));
        snprintf(vname, sizeof(vname), "%s_resume_tx[%u]", id.c_str(), t);
        resume_tx_.push_back(net_ctx.bool_const(vname));
        snprintf(vname, sizeof(vname), "%s_pause_rx[%u]", id.c_str(), t);
        pause_rx_.push_back(net_ctx.bool_const(vname));
        snprintf(vname, sizeof(vname), "%s_resume_rx[%u]", id.c_str(), t);
        resume_rx_.push_back(net_ctx.bool_const(vname));
        snprintf(vname, sizeof(vname), "%s_sender_paused[%u]", id.c_str(), t);
        sender_paused_.push_back(net_ctx.bool_const(vname));
    }
}

void SBQueue::add_constrs(NetContext& net_ctx, map<string, expr>& constr_map) {
    char constr_name[120];

    for (unsigned int t = 0; t < total_time; t++) {
        // Shared-buffer interface queues do not model internal packet storage.
        // Keep element state inert so no legacy queue sliding-window semantics leak in.
        for (unsigned int p = 0; p < size_; p++) {
            snprintf(constr_name, sizeof(constr_name), "%s_elem_null_%u_%u", id.c_str(), p, t);
            constr_map.insert(named_constr(constr_name, elems_[p][t] == net_ctx.null_pkt()));
        }

        // Shared-buffer interface queues expose at most one enqueue-visible packet per slot.
        for (unsigned int p = 1; p < max_enq_; p++) {
            snprintf(constr_name, sizeof(constr_name), "%s_enq_null_%u_%u", id.c_str(), p, t);
            constr_map.insert(named_constr(constr_name, enqs_[p][t] == net_ctx.null_pkt()));
        }

        // Counting: enq_cnt is exactly 0/1 based on enqs[0] validity.
        if (max_enq_ > 0) {
            snprintf(constr_name, sizeof(constr_name), "%s_enq_cnt_from_enq0_%u", id.c_str(), t);
            constr_map.insert(named_constr(constr_name,
                                           enq_cnt_[t] == ite(net_ctx.pkt2val(enqs_[0][t]),
                                                              net_ctx.int_val(1),
                                                              net_ctx.int_val(0))));
        } else {
            snprintf(constr_name, sizeof(constr_name), "%s_enq_cnt_zero_%u", id.c_str(), t);
            constr_map.insert(named_constr(constr_name, enq_cnt_[t] == 0));
        }

        // deq_cnt is not queue-local storage semantics anymore; only keep coarse bounds.
        snprintf(constr_name, sizeof(constr_name), "%s_deq_lb_%u", id.c_str(), t);
        constr_map.insert(named_constr(constr_name, deq_cnt_[t] >= 0));
        snprintf(constr_name, sizeof(constr_name), "%s_deq_ub_%u", id.c_str(), t);
        constr_map.insert(named_constr(constr_name, deq_cnt_[t] <= (int) max_deq_));

        // Downstream ingress cannot emit pause and resume simultaneously.
        snprintf(constr_name, sizeof(constr_name), "%s_ctrl_tx_mutex_%u", id.c_str(), t);
        constr_map.insert(named_constr(constr_name, !(pause_tx_[t] && resume_tx_[t])));

        if (t == 0) {
            // No delayed control packet has arrived before the first slot.
            snprintf(constr_name, sizeof(constr_name), "%s_pause_rx_init", id.c_str());
            constr_map.insert(named_constr(constr_name, !pause_rx_[0]));
            snprintf(constr_name, sizeof(constr_name), "%s_resume_rx_init", id.c_str());
            constr_map.insert(named_constr(constr_name, !resume_rx_[0]));
            snprintf(constr_name, sizeof(constr_name), "%s_sender_paused_init", id.c_str());
            constr_map.insert(named_constr(constr_name, !sender_paused_[0]));
        } else {
            // TODO(shared-buffer-semantics): This hard-coded 1-slot control propagation is the
            // current FAST_STEADY approximation. Move pause_rx/resume_rx/sender_paused evolution
            // behind SharedBufferSemanticPolicy so PRECISE_PFC can model launch-vs-arrival timing
            // and in-flight traffic without forking SharedBufferSwitchQM's common core logic.
            // Coarse-step control propagation: pause/resume sent at t-1 is observed at t.
            snprintf(constr_name, sizeof(constr_name), "%s_pause_rx_delay_%u", id.c_str(), t);
            constr_map.insert(named_constr(constr_name, pause_rx_[t] == pause_tx_[t - 1]));
            snprintf(constr_name, sizeof(constr_name), "%s_resume_rx_delay_%u", id.c_str(), t);
            constr_map.insert(named_constr(constr_name, resume_rx_[t] == resume_tx_[t - 1]));

            // The upstream sender obeys the delayed pause/resume state.
            snprintf(constr_name, sizeof(constr_name), "%s_sender_paused_step_%u", id.c_str(), t);
            constr_map.insert(
                named_constr(constr_name,
                             sender_paused_[t] ==
                                 ((sender_paused_[t - 1] || pause_rx_[t]) && !resume_rx_[t])));
        }
    }
}

string SBQueue::get_model_str(model& m, NetContext& net_ctx, unsigned int t) {
    stringstream ss;
    ss << get_id() << ": ";
    append_pkt_slots(ss, m, net_ctx, enqs_, (int) max_enq() - 1, t);
    ss << "->";
    ss << " (" << m.eval(enq_cnt(t)).get_numeral_int() << ", "
       << m.eval(deq_cnt(t)).get_numeral_int() << ") ";
    ss << "ctrl=("
       << "txP=" << eval_bool_as_int(m, pause_tx_[t]) << ", "
       << "txR=" << eval_bool_as_int(m, resume_tx_[t]) << ", "
       << "rxP=" << eval_bool_as_int(m, pause_rx_[t]) << ", "
       << "rxR=" << eval_bool_as_int(m, resume_rx_[t]) << ", "
       << "sp=" << eval_bool_as_int(m, sender_paused_[t]) << ")";
    return ss.str();
}

expr& SBQueue::pause_tx(unsigned int t) {
    return pause_tx_[t];
}

const expr& SBQueue::pause_tx(unsigned int t) const {
    return pause_tx_[t];
}

expr& SBQueue::resume_tx(unsigned int t) {
    return resume_tx_[t];
}

const expr& SBQueue::resume_tx(unsigned int t) const {
    return resume_tx_[t];
}

expr& SBQueue::pause_rx(unsigned int t) {
    return pause_rx_[t];
}

const expr& SBQueue::pause_rx(unsigned int t) const {
    return pause_rx_[t];
}

expr& SBQueue::resume_rx(unsigned int t) {
    return resume_rx_[t];
}

const expr& SBQueue::resume_rx(unsigned int t) const {
    return resume_rx_[t];
}

expr& SBQueue::sender_paused(unsigned int t) {
    return sender_paused_[t];
}

const expr& SBQueue::sender_paused(unsigned int t) const {
    return sender_paused_[t];
}
