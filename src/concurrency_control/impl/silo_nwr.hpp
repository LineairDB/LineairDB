/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef LINEAIRDB_SILO_NWR_H
#define LINEAIRDB_SILO_NWR_H

#include <lineairdb/tx_status.h>

#include <atomic>
#include <cstddef>
#include <cstring>
#include <vector>

#include "concurrency_control/concurrency_control_base.h"
#include "concurrency_control/pivot_object.hpp"
#include "index/concurrent_table.h"
#include "types.h"

namespace LineairDB {

namespace ConcurrencyControl {

template <bool EnableNWR = true>
class SiloNWRTyped final : public ConcurrencyControlBase {
  using NWRObjectType = std::atomic<NWRPivotObject>;

 private:
  struct ValidationItem {
    const DataItem* item_p_cache;
    TransactionId transaction_id;
  };
  struct PivotObjectSnapshot {
    DataItem* item_p_cache;
    NWRPivotObject pv_snapshot;
    enum SnapshotFrom { READSET, WRITESET };
    SnapshotFrom set_type;
  };

  std::vector<ValidationItem> validation_set_;
  NWRValidationResult nwr_validation_result_;
  NWRPivotObject my_pivot_object_;
  std::vector<PivotObjectSnapshot> pivot_object_snapshots_;

 public:
  SiloNWRTyped(TransactionReferences&& tx)
      : ConcurrencyControlBase(std::forward<TransactionReferences&&>(tx)),
        nwr_validation_result_(NWRValidationResult::NOT_YET_VALIDATED){};
  ~SiloNWRTyped() final override{};

  const DataItem Read(const std::string_view,
                      DataItem* index_leaf) final override {
    assert(index_leaf != nullptr);

    DataItem snapshot;
    for (;;) {
      auto tx_id = index_leaf->transaction_id.load();

      if (tx_id.tid & 1u) {  // locked
                             // WANTFIX user-space adaptive mutex locking may
                             // improve the performance
        std::this_thread::yield();
        continue;
      }

      snapshot.Reset(index_leaf->value, index_leaf->size, tx_id);

      if (index_leaf->transaction_id.load() == tx_id) {
        validation_set_.push_back({index_leaf, tx_id});
        return snapshot;
      }
    }
  };
  void Write(const std::string_view, const std::byte* const, const size_t,
             DataItem*) final override{};
  void Abort() final override{};
  bool Precommit() final override {
    /** Sorting write set to prevent deadlock **/
    std::sort(tx_ref_.write_set_ref_.begin(), tx_ref_.write_set_ref_.end(),
              Snapshot::Compare);

    if constexpr (EnableNWR) {
      if (!IsReadOnly() && IsOmittable()) {
        // we can safely clear writeset since all versions x_j in writeset_j are
        // omittable.
        tx_ref_.write_set_ref_.clear();
        return true;
      } else {
        // Preemptive abort: if anti_dependency validation of omittable version
        // order has failed, it is meaningless to acquire exclusive lockings
        // since the subsequent validation of Silo's version order will also
        // fail.
        if (nwr_validation_result_ == NWRValidationResult::ANTI_DEPENDENCY) {
          return false;
        }
      }
    }

    /** Acquire Lock **/
    for (auto& snapshot : tx_ref_.write_set_ref_) {
      auto* item = snapshot.index_cache;
      assert(item != nullptr);

      for (;;) {
        auto current = item->transaction_id.load();
        if (current.tid & 1llu) {
          // WANTFIX user-space adaptive mutex locking may
          // improve the performance
          std::this_thread::yield();
          continue;
        }
        auto desired = current;
        desired.tid |= 1llu;
        bool lock_acquired =
            item->transaction_id.compare_exchange_weak(current, desired);
        if (lock_acquired) {
          snapshot.data_item_copy.transaction_id.store(desired);
          // If this item is in readset, add 1 (lockflag) into snapshot for
          // validation
          for (auto& read_item : validation_set_) {
            if (read_item.item_p_cache == item) {
              read_item.transaction_id.tid++;
              break;
            }
          }
          break;
        }
      }
    }

    /** Update Metadata for NWR **/
    if constexpr (EnableNWR) { UpdatePivotObjects(); }

    /** Validation Phase **/
    if (!AntiDependencyValidation()) {
      // if validation failed, unlock all objects
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        auto current = snapshot.index_cache->transaction_id.load();
        current.tid--;
        snapshot.index_cache->transaction_id.store(current);
      }
      return false;
    }

    /** Buffer Update **/
    for (auto& snapshot : tx_ref_.write_set_ref_) {
      auto* item = snapshot.index_cache;
      item->Reset(snapshot.data_item_copy.value, snapshot.data_item_copy.size);
    }

    return true;
  };

  void PostProcessing(TxStatus status) final override {
    if (status == TxStatus::Committed) {
      if constexpr (EnableNWR) {
        if (nwr_validation_result_ == NWRValidationResult::ACYCLIC) { return; }
      }

      auto current_epoch = tx_ref_.my_epoch_ref_;

      /** Unlock **/
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        auto* item       = snapshot.index_cache;
        auto current_tid = snapshot.data_item_copy.transaction_id.load();
        EpochNumber written_in_epoch = current_tid.epoch;

        TransactionId unlocked_id;
        if (current_epoch != written_in_epoch) {
          unlocked_id = {current_epoch, 2};
        } else {
          unlocked_id = {current_epoch, current_tid.tid + 1};
        }
        item->transaction_id.store(unlocked_id);
        snapshot.data_item_copy.transaction_id.store(unlocked_id);
      }
    }
  }

 private:
  bool AntiDependencyValidation() {
    for (auto& validation_item : validation_set_) {
      auto* item = validation_item.item_p_cache;
      auto tx_id = item->transaction_id.load();
      if (tx_id != validation_item.transaction_id) { return false; }
    }
    return true;
  }

  bool IsOmittable() {
    // Brief: we now just collect and snapshot the pivot version objects.
    // Explanation: we first generate a version order << from the pivot
    // version objects for each data item. Let t_j be this transaction. A
    // pivot version object for x holds the pivot version x_pv, which is the
    // landmark for ordering x_j in the version order for x: for all x_j in
    // writeset_j, x_j < x_pv and there does not exist x_k such that x_j < x_k
    // < x_pv. In other words, the pivot version x_pv is just after version of
    // x_j, __in the generated version order <<__. When << fails to validate
    // the correctness, Silo generate the another version order which includes
    // x_pv < x_j by using exclusive locking.

    {  // snapshot the pivot version objects from write_set
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        auto* value_ptr = snapshot.index_cache;
        assert(value_ptr != nullptr);

        const auto pivot_object               = value_ptr->pivot_object.load();
        const PivotObjectSnapshot pv_snapshot = {value_ptr, pivot_object,
                                                 PivotObjectSnapshot::WRITESET};
        pivot_object_snapshots_.emplace_back(pv_snapshot);
      }
    }
    {  // snapshot the pivot version objects
       // from read_set
      for (auto& snapshot : tx_ref_.read_set_ref_) {
        auto* value_ptr = snapshot.index_cache;
        assert(value_ptr != nullptr);
        const auto pivot_object               = value_ptr->pivot_object.load();
        const PivotObjectSnapshot pv_snapshot = {value_ptr, pivot_object,
                                                 PivotObjectSnapshot::READSET};
        pivot_object_snapshots_.emplace_back(pv_snapshot);
      }
    }

    // We now validate Linearizability.
    // In short, linearizability prohibits the ordering of version orders
    // among non-concurrent transactions. To validate the concurrency of
    // transactions, we use epoch: transactions in the same epoch are
    // committed at the same time, and thus they are in concurrent, and thus
    // any version order for these transactions are valid for linearizability.
    const EpochNumber current_epoch = tx_ref_.my_epoch_ref_;
    for (auto& pivot_object : pivot_object_snapshots_) {
      if (pivot_object.set_type == PivotObjectSnapshot::READSET) continue;
      const EpochNumber epoch = pivot_object.pv_snapshot.versions.epoch;
      if (epoch != current_epoch) {
        nwr_validation_result_ = NWRValidationResult::LINEARIZABILITY;
        return false;
      }
    }

    // Next, we prepare validations of serializability.
    // In validation phase, we must compare the values between t_j's
    // read/write sets and pivot version object for each x in writeset_j. To
    // this end efficiently, we squash the version numbers of read/write set
    // into PivotObject.
    my_pivot_object_.versions.epoch = current_epoch;
    {  // make t_j's squashed read/write set

      // MergedRS
      for (auto& snapshot : tx_ref_.read_set_ref_) {
        const auto value_ptr = snapshot.index_cache;
        auto tid             = snapshot.data_item_copy.transaction_id.load();
        assert(value_ptr != nullptr);

        // Store the version number x_k read by t_j.
        // When x_k has written in the different (not the current) epoch,
        // version x_k in this merged RS must be less than all versions
        // written in the current epoch, and thus we store 1 as the oldest
        // version for all epochs, instead of actual value of 64-bits version
        // representation.
        if (tid.epoch == current_epoch) {
          my_pivot_object_.msets.rset.PutHigherside(value_ptr, tid.tid);
        } else {
          my_pivot_object_.msets.rset.PutHigherside(value_ptr, 1);
        }
      }

      // MergedWS
      for (auto& pivot_object : pivot_object_snapshots_) {
        if (pivot_object.set_type != PivotObjectSnapshot::WRITESET) continue;
        const auto* value_ptr = pivot_object.item_p_cache;
        uint32_t tk           = pivot_object.pv_snapshot.versions.target_id;
        assert(pivot_object.pv_snapshot.versions.epoch == current_epoch);

        my_pivot_object_.msets.wset.PutHigherside(value_ptr, tk);
      }
    }

    // Now we validate the version order << given by the pivot version
    // objects.
    // Validate Serializability 1.
    // Successors_j := {|Tk| wk(xk) in H and xj << xk and rg(xk) in H }
    // if there exists t_k such that t_k in successors_j and t_k -> ...,
    // -> t_j, there exists dependency cycle in MVSG.
    for (auto& pivot_object : pivot_object_snapshots_) {
      if (pivot_object.set_type != PivotObjectSnapshot::WRITESET) continue;
      auto& tj = my_pivot_object_;
      auto& tk = pivot_object.pv_snapshot;

      auto result = tk.IsReachableInto(tj);
      if (result != NWRValidationResult::ACYCLIC) {
        nwr_validation_result_ = result;
        return false;
      }
    }
    // Validate serializability 2:
    // Overwriters_j := {|Tg| rj(xk) in H and (xk = rg or xk << xg))}
    // if there exists t_k in overwriters_j such that Tk -> ..., -> Tj,
    // there exists dependency cycle in MVSG.
    // We adopt the same strategy with the baseline (Silo): if there exists
    // newer version of x, then we simply regard MVSG may be not acyclic.
    if (!AntiDependencyValidation()) {
      nwr_validation_result_ = NWRValidationResult::ANTI_DEPENDENCY;
      return false;
    }

    // We must updating mRS and mWS for each data item in read/write set,
    // to ensure serializability between this transaction and concurrent NWR
    // procedures. This updating need to execute CAS-loop.
    bool all_cas_succeed = true;
    for (auto& snapshot : pivot_object_snapshots_) {
      auto* data_item_p  = snapshot.item_p_cache;
      auto& atomic_ref   = data_item_p->pivot_object;
      auto& old_snapshot = snapshot.pv_snapshot;
      auto new_snapshot  = old_snapshot;

      new_snapshot.msets.rset =
          new_snapshot.msets.rset.Merge(my_pivot_object_.msets.rset);
      new_snapshot.msets.wset =
          new_snapshot.msets.wset.Merge(my_pivot_object_.msets.wset);

      if (new_snapshot.msets == old_snapshot.msets) continue;

      all_cas_succeed =
          atomic_ref.compare_exchange_weak(old_snapshot, new_snapshot);
      if (!all_cas_succeed) break;
    }

    // Unfortunately some CAS operations have failed.
    // but Anti-dependency validation may still pass this transaction and thus
    // we retry all procedure: generate version order, taking snapshots, do
    // validations for NWR, and finally trying to CAS.
    if (!all_cas_succeed) { return IsOmittable(); }

    // Fortunately we can safely omit this transaction.
    nwr_validation_result_ = NWRValidationResult::ACYCLIC;
    return true;
  }

  /**
   * @brief
   * Update the metadata (the pivot objects) for each data item in readset or
   * writeset, for the NWR-validation of the other transactions. This method
   * must be invoked before Anti-dependency validation.
   * @details
   * In SiloNWR, a transaction is committed by CAS-loop if the NWR-validation is
   * succeed. Otherwise, it runs on lock-base as with the Silo. Concurrency
   * control between these two different types of transactions, CAS-based and
   * Lock-based, is a difficult task since the NWR-validation does not check the
   * state of lockings. The following proposition helps us to solve this
   * problem:
   *   - Let t_cas be a CAS-based committed transaction.
   *   - Let t_lock be a lock-based committed transaction.
   *   - If t_lock updates the pivot version objects "before" its
   *     anti_dependency_validation, they do not violate serializability.
   * Proof (sketch):
   * To prove by contradiction, suppose that an MVSG of t_cas and t_lock depict
   * a cycle. That is, there is a transitive path t_cas -> t_lock -> t_cas in
   * the graph. Note that t_cas performs CAS into the pivot version object of
   * some pivot version. Let x_pv be the version such that there exists
   * w_cas(x_cas) and x_cas < x_pv. If t_lock also reads or writes the same data
   * item x and commits, then the accessed version x_k is always a version
   * greater than x_pv.
   * That is, t_cas -> t_lock consists of a single data item, but
   * in order to draw t_lock -> t_cas, one more data item `y` must be assumed.
   * Here we show the exaustive two cases as the followings:
   *   -# t_lock (wr)-> t_cas:
   *     - i.e., t_cas reads some newer version y_k such that y_lock < y_k
   *     - If c_cas precedes c_lock, it does not holds since t_lock holds the
   *       exclusive lock of y until c_lock and c_cas cannot read y_k.
   *     - Otherwise, t_cas aborts since the NWR-validation tells the edge
   *       t_cas -> t_lock.
   *   -# t_lock <<(rw, ww)-> t_cas:
   *      - i.e., t_cas writes some newer version y_cas, such that:
   *        -# y_lock < y_cas < y_pv
   *        -# y_k < y_cas < y_pv and r_lock(y_k) for some y_k
   *           - If c_cas precedes w_lock(y_lock), y_pv cannot be exist.
   *           - Otherwise, t_cas aborts by the NWR-validation.
   */
  void UpdatePivotObjects() {
    // Let t_j be this transaction.
    // Now update the pivot objects for all data items in read/write set of
    // t_j. Updating procedure will be completed by atomic::compare_exchange
    // operation into each data item with my_pivot_object.
    // We assume that this method is invoked from lock-based (not NWR)
    // protocol.
    assert(nwr_validation_result_ != NWRValidationResult::ACYCLIC);

    // Re-build my pivot object
    const EpochNumber current_epoch = tx_ref_.my_epoch_ref_;
    my_pivot_object_.versions.epoch = current_epoch;
    {  // make t_j's squashed read/write set

      // MergedRS
      for (auto& snapshot : tx_ref_.read_set_ref_) {
        const auto* value_ptr = snapshot.index_cache;
        auto tid              = snapshot.data_item_copy.transaction_id.load();
        assert(value_ptr != nullptr);
        if (tid.epoch == current_epoch) {
          my_pivot_object_.msets.rset.PutLowerside(value_ptr, tid.tid);
        } else {
          my_pivot_object_.msets.rset.PutLowerside(value_ptr, 1);
        }
      }

      // MergedWS
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        const auto* value_ptr = snapshot.index_cache;
        assert(value_ptr != nullptr);
        auto tid = snapshot.data_item_copy.transaction_id.load();
        assert(tid.tid & 1llu);  // is locked

        auto new_version = tid.tid;
        if (tid.epoch == current_epoch) {
          new_version += 1;  // unlocked version
        } else {
          new_version = 2;  // the first unlocked version in this epoch
        }

        my_pivot_object_.msets.wset.PutHigherside(value_ptr, new_version);
      }
    }

    // Updating mRS and mWS
    for (auto& snapshot : pivot_object_snapshots_) {
      auto* data_item_p = snapshot.item_p_cache;
      auto& atomic_ref  = data_item_p->pivot_object;
      auto old_snapshot = atomic_ref.load();

      // If this transaction performs the first-blind write into the data item
      // in this epoch, update the pivot version.
      if (old_snapshot.versions.epoch != current_epoch &&
          snapshot.set_type == PivotObjectSnapshot::WRITESET) {
        Snapshot* ws_entry_for_this_snapshot;
        for (auto& ws_entry : tx_ref_.write_set_ref_) {
          if (ws_entry.index_cache != snapshot.item_p_cache) continue;
          if (ws_entry.is_read_modify_write) break;
          ws_entry_for_this_snapshot = &ws_entry;
        }

        // It is the first blind write into the data item in this epoch
        auto new_snapshot = my_pivot_object_;
        assert(new_snapshot.versions.epoch == current_epoch);
        new_snapshot.versions.target_id =
            ws_entry_for_this_snapshot->data_item_copy.transaction_id.load()
                .tid;
        data_item_p->pivot_object.store(new_snapshot);
        continue;
      }

      bool cas_success = false;
      while (!cas_success) {
        old_snapshot      = data_item_p->pivot_object.load();
        auto new_snapshot = old_snapshot;
        auto new_rset =
            new_snapshot.msets.rset.Merge(my_pivot_object_.msets.rset);
        auto new_wset =
            new_snapshot.msets.wset.Merge(my_pivot_object_.msets.wset);
        new_snapshot.msets.rset = new_rset;
        new_snapshot.msets.wset = new_wset;

        cas_success =
            atomic_ref.compare_exchange_weak(old_snapshot, new_snapshot);
      }
    }
  }
};

using SiloNWR = SiloNWRTyped<true>;
using Silo    = SiloNWRTyped<false>;

}  // namespace ConcurrencyControl
}  // namespace LineairDB
#endif /* LINEAIRDB_SILO_NWR_H */
