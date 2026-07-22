#include "recovery/checkpoint_manager.hpp"

#include <lineairdb/database.h>
#include <lineairdb/transaction.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <msgpack.hpp>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "recovery/logger.h"
#include "table/table_dictionary.hpp"
#include "types/data_item.hpp"
#include "types/snapshot.hpp"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Recovery {

// Regression tests for: CPRManager::WriteDeltaCheckpoint silently
// discarding the newest write to an item when a second write to the same
// item arrives before the first has been drained.
//
// Both tests below only use CPRManager's existing public API --
// OnPrecommit, RotateDirtySets, GetCheckpointCompletedEpoch -- so the
// checkpoint worker's real background thread does the actual draining,
// exactly as in production. What makes these deterministic (not a timing
// race) is that every event that matters is driven explicitly and in a
// fixed order on the test thread (OnPrecommit calls, buffer updates,
// RotateDirtySets), and completion is detected by *polling a condition
// that is guaranteed to eventually become true* (GetCheckpointCompletedEpoch
// reaching the epoch just requested) rather than by sleeping for a
// duration and hoping enough happened. No backlog, no large payloads, no
// thread-count/sleep tuning -- correctness here has nothing to do with the
// worker being fast or slow.
class CPRManagerWhiteBoxTest : public ::testing::Test {
 protected:
  Config config_;
  void SetUp() override {
    config_.durability = Config::DurabilityStrategy::Checkpoint;
    config_.work_dir = "./lineairdb_logs_cpr_whitebox_test";
    // Driven manually: every RotateDirtySets call is eligible to start a
    // round immediately, regardless of wall-clock time since the last one.
    // Without this, only the *first* round in a test would start promptly
    // (last_round_started_at_ default-constructs far enough in the past to
    // satisfy the real checkpoint_period=5000ms default too), but a second,
    // same-test round would have to wait out the full production period.
    config_.checkpoint_period = 0;
    std::filesystem::remove_all(config_.work_dir);
  }
  void TearDown() override { std::filesystem::remove_all(config_.work_dir); }

  // Waits until the checkpoint worker has actually drained up to
  // target_epoch, however long that takes -- a busy-wait on a condition
  // that RotateDirtySets(target_epoch) guarantees will eventually become
  // true, not a fixed sleep. The timeout only guards against a genuine
  // hang (e.g. a future regression that stops the worker from progressing
  // at all); it is not what makes the test pass or fail under normal
  // conditions, which complete in well under a second.
  static void WaitForCheckpointCompleted(CPRManager& cpr,
                                         EpochNumber target_epoch) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (cpr.GetCheckpointCompletedEpoch() < target_epoch) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline)
          << "Checkpoint worker never reached epoch " << target_epoch
          << " -- it appears to have stalled rather than merely being slow.";
      std::this_thread::yield();
    }
  }

  // A checkpoint round now takes two RotateDirtySets calls (two simulated
  // epoch ticks) to actually drain: the first starts the round
  // (REST -> IN_PROGRESS, recording version_ = target_epoch), the second
  // observes one full tick has elapsed and triggers the drain
  // (IN_PROGRESS -> WAIT_PENDING). See RotateDirtySets's comment for why one
  // elapsed tick is sufficient. The second call's own epoch argument is
  // irrelevant to which round drains (version_ was already fixed by the
  // first call); target_epoch + 1 is passed purely for realism.
  static void TriggerAndWaitForRound(CPRManager& cpr,
                                     EpochNumber target_epoch) {
    cpr.RotateDirtySets(target_epoch);
    cpr.RotateDirtySets(target_epoch + 1);
    WaitForCheckpointCompleted(cpr, target_epoch);
  }

  static Logger::LogRecords ReadDeltaFile(const std::string& work_dir,
                                          EpochNumber cp_epoch) {
    const auto path =
        work_dir + "/cpr_delta_" + std::to_string(cp_epoch) + ".log";
    std::ifstream f(path, std::ios::binary);
    const std::string contents((std::istreambuf_iterator<char>(f)),
                               std::istreambuf_iterator<char>());
    auto handle = msgpack::unpack(contents.data(), contents.size());
    Logger::LogRecords records;
    handle.get().convert(records);
    return records;
  }
};

TEST_F(CPRManagerWhiteBoxTest,
       DrainEmitsLatestValueWhenSecondWriteArrivesBeforeFirstIsDrained) {
  EpochFramework epoch;
  epoch.Start();
  TableDictionary dict;  // unused by CPRManager, only needed for the ctor
  CPRManager cpr(config_, dict, epoch);

  const std::string table_name = "t";
  const std::string key = "k";
  DataItem item;

  auto write_set_for = [&](const std::string& value) {
    WriteSetType ws;
    ws.emplace_back(key, reinterpret_cast<const std::byte*>(value.data()),
                    value.size(), &item, table_name);
    return ws;
  };

  // Write A (epoch 5): PrecommitWriteSet first, buffer update second --
  // matches Silo's real order, where OnPrecommit runs *before* "Buffer
  // Update" (see silo_nwr.hpp's Precommit()).
  cpr.OnPrecommit(write_set_for("AAAA"), /*current_epoch=*/5);
  item.Reset(reinterpret_cast<const std::byte*>("AAAA"), 4);

  // Write B (epoch 7), landing on the same still-undrained item (nothing
  // has drained yet: RotateDirtySets hasn't been called).
  cpr.OnPrecommit(write_set_for("BBBB"), /*current_epoch=*/7);
  item.Reset(reinterpret_cast<const std::byte*>("BBBB"), 4);

  // Trigger and wait for a single drain covering both writes above.
  TriggerAndWaitForRound(cpr, /*target_epoch=*/10);

  auto records = ReadDeltaFile(config_.work_dir, 10);
  ASSERT_EQ(records.size(), 1u);
  ASSERT_EQ(records[0].key_value_pairs.size(), 1u);
  EXPECT_EQ(records[0].key_value_pairs[0].buffer, "BBBB")
      << "Drain emitted \"" << records[0].key_value_pairs[0].buffer
      << "\" instead of the item's true value (\"BBBB\") at drain time -- "
         "it discarded the second write in favor of a stale value from the "
         "first.";
}

// Companion regression test for a second, distinct failure mode: the dirty
// stack is per-thread, so if two writes to the *same* item land on two
// *different* threads' DirtySets before either is drained, a dedup scheme
// scoped to one thread's chain at a time can't see they're the same item --
// each thread's chain would independently emit its own record, both tagged
// with the identical tid {cp_epoch, 0}, and recovery's "keep the highest
// tid" merge never overwrites on a tie, so whichever happened to be
// processed first would silently win.
//
// join() (not a sleep or a timing race) is what makes the *setup*
// deterministic: both threads' PrecommitWriteSet calls are guaranteed
// complete, in a fixed order, before RotateDirtySets is even called.
TEST_F(CPRManagerWhiteBoxTest,
       DrainEmitsExactlyOneRecordWhenTwoThreadsDirtyTheSameItem) {
  EpochFramework epoch;
  epoch.Start();
  TableDictionary dict;
  CPRManager cpr(config_, dict, epoch);

  const std::string table_name = "t";
  const std::string key = "k";
  DataItem item;

  auto write_set_for = [&](const std::string& value) {
    WriteSetType ws;
    ws.emplace_back(key, reinterpret_cast<const std::byte*>(value.data()),
                    value.size(), &item, table_name);
    return ws;
  };

  std::thread thread_a(
      [&] { cpr.OnPrecommit(write_set_for("AAAA"), /*current_epoch=*/5); });
  thread_a.join();
  item.Reset(reinterpret_cast<const std::byte*>("AAAA"), 4);

  std::thread thread_b(
      [&] { cpr.OnPrecommit(write_set_for("BBBB"), /*current_epoch=*/7); });
  thread_b.join();
  item.Reset(reinterpret_cast<const std::byte*>("BBBB"), 4);

  TriggerAndWaitForRound(cpr, /*target_epoch=*/10);

  auto records = ReadDeltaFile(config_.work_dir, 10);
  ASSERT_EQ(records.size(), 1u);
  ASSERT_EQ(records[0].key_value_pairs.size(), 1u)
      << "Drain emitted " << records[0].key_value_pairs.size()
      << " records for one item dirtied on two different threads -- dedup "
         "must be global to the whole drain, not scoped per thread.";
  EXPECT_EQ(records[0].key_value_pairs[0].buffer, "BBBB");
}

// Regression tests for the CPR phase state machine itself (REST ->
// IN_PROGRESS -> WAIT_PENDING -> REST), exercising the per-item stable copy
// (item->stable_buffer / item->stable_version) that
// PrecommitWriteSet/WriteDeltaCheckpoint implement on top of it.

TEST_F(CPRManagerWhiteBoxTest, FirstTouchDuringInProgressProtectsPreimage) {
  EpochFramework epoch;
  epoch.Start();
  TableDictionary dict;
  CPRManager cpr(config_, dict, epoch);

  const std::string table_name = "t";
  const std::string key = "k";
  DataItem item;
  auto write_set_for = [&](const std::string& value) {
    WriteSetType ws;
    ws.emplace_back(key, reinterpret_cast<const std::byte*>(value.data()),
                    value.size(), &item, table_name);
    return ws;
  };

  // Establish an initial, already-committed value during REST (no round
  // active yet).
  cpr.OnPrecommit(write_set_for("OLD1"), /*current_epoch=*/3);
  item.Reset(reinterpret_cast<const std::byte*>("OLD1"), 4);

  // Start a round targeting epoch 10.
  cpr.RotateDirtySets(/*next_checkpoint_epoch=*/10);

  // First touch during the round (current_epoch >= version_): must take a
  // stable copy of "OLD1" before this write's own value lands.
  cpr.OnPrecommit(write_set_for("NEW1"), /*current_epoch=*/10);
  item.Reset(reinterpret_cast<const std::byte*>("NEW1"), 4);

  // One more tick triggers the drain.
  cpr.RotateDirtySets(11);
  WaitForCheckpointCompleted(cpr, 10);

  auto records = ReadDeltaFile(config_.work_dir, 10);
  ASSERT_EQ(records.size(), 1u);
  ASSERT_EQ(records[0].key_value_pairs.size(), 1u);
  EXPECT_EQ(records[0].key_value_pairs[0].buffer, "OLD1")
      << "Round 10's snapshot must represent state as of just before the "
         "round started, not this round's own write (\"NEW1\") -- that "
         "write belongs to a future round's snapshot instead.";
}

TEST_F(CPRManagerWhiteBoxTest, SecondTouchSameRoundDoesNotReprotect) {
  EpochFramework epoch;
  epoch.Start();
  TableDictionary dict;
  CPRManager cpr(config_, dict, epoch);

  const std::string table_name = "t";
  const std::string key = "k";
  DataItem item;
  auto write_set_for = [&](const std::string& value) {
    WriteSetType ws;
    ws.emplace_back(key, reinterpret_cast<const std::byte*>(value.data()),
                    value.size(), &item, table_name);
    return ws;
  };

  cpr.OnPrecommit(write_set_for("OLD1"), /*current_epoch=*/3);
  item.Reset(reinterpret_cast<const std::byte*>("OLD1"), 4);

  cpr.RotateDirtySets(/*next_checkpoint_epoch=*/10);

  // Two touches during the same round: only the *first* should protect.
  cpr.OnPrecommit(write_set_for("MID1"), /*current_epoch=*/10);
  item.Reset(reinterpret_cast<const std::byte*>("MID1"), 4);
  cpr.OnPrecommit(write_set_for("NEW1"), /*current_epoch=*/10);
  item.Reset(reinterpret_cast<const std::byte*>("NEW1"), 4);

  cpr.RotateDirtySets(11);
  WaitForCheckpointCompleted(cpr, 10);

  auto records = ReadDeltaFile(config_.work_dir, 10);
  ASSERT_EQ(records.size(), 1u);
  ASSERT_EQ(records[0].key_value_pairs.size(), 1u);
  EXPECT_EQ(records[0].key_value_pairs[0].buffer, "OLD1")
      << "A second touch within the same round must not re-protect (which "
         "would incorrectly capture \"MID1\" instead of the true pre-round "
         "value \"OLD1\").";
}

TEST_F(CPRManagerWhiteBoxTest,
       UntouchedItemEmitsLiveValueTouchedItemEmitsPreimage) {
  EpochFramework epoch;
  epoch.Start();
  TableDictionary dict;
  CPRManager cpr(config_, dict, epoch);

  const std::string table_name = "t";
  const std::string touched_key = "touched";
  const std::string untouched_key = "untouched";
  DataItem touched_item;
  DataItem untouched_item;
  auto write_set_for = [&](DataItem& item, const std::string& key,
                           const std::string& value) {
    WriteSetType ws;
    ws.emplace_back(key, reinterpret_cast<const std::byte*>(value.data()),
                    value.size(), &item, table_name);
    return ws;
  };

  cpr.OnPrecommit(write_set_for(touched_item, touched_key, "OLD1"),
                  /*current_epoch=*/3);
  touched_item.Reset(reinterpret_cast<const std::byte*>("OLD1"), 4);
  cpr.OnPrecommit(write_set_for(untouched_item, untouched_key, "STABLE1"),
                  /*current_epoch=*/3);
  untouched_item.Reset(reinterpret_cast<const std::byte*>("STABLE1"), 7);

  cpr.RotateDirtySets(/*next_checkpoint_epoch=*/10);

  // Only touched_item is written during the round.
  cpr.OnPrecommit(write_set_for(touched_item, touched_key, "NEW1"),
                  /*current_epoch=*/10);
  touched_item.Reset(reinterpret_cast<const std::byte*>("NEW1"), 4);

  cpr.RotateDirtySets(11);
  WaitForCheckpointCompleted(cpr, 10);

  auto records = ReadDeltaFile(config_.work_dir, 10);
  ASSERT_EQ(records.size(), 1u);
  ASSERT_EQ(records[0].key_value_pairs.size(), 2u);
  for (const auto& kvp : records[0].key_value_pairs) {
    if (kvp.key == touched_key) {
      EXPECT_EQ(kvp.buffer, "OLD1")
          << "touched item must emit its protected pre-round value.";
    } else {
      ASSERT_EQ(kvp.key, untouched_key);
      EXPECT_EQ(kvp.buffer, "STABLE1")
          << "untouched item must emit its live value directly -- it was "
             "never protected because nothing wrote it during the round.";
    }
  }
}

TEST_F(CPRManagerWhiteBoxTest, SubsequentRoundStartsCleanNoStaleProtection) {
  EpochFramework epoch;
  epoch.Start();
  TableDictionary dict;
  CPRManager cpr(config_, dict, epoch);

  const std::string table_name = "t";
  const std::string key = "k";
  DataItem item;
  auto write_set_for = [&](const std::string& value) {
    WriteSetType ws;
    ws.emplace_back(key, reinterpret_cast<const std::byte*>(value.data()),
                    value.size(), &item, table_name);
    return ws;
  };

  // Round 1: protect "V0", drain captures "V0", live value ends at "V1".
  cpr.OnPrecommit(write_set_for("V0"), /*current_epoch=*/3);
  item.Reset(reinterpret_cast<const std::byte*>("V0"), 2);
  cpr.RotateDirtySets(/*next_checkpoint_epoch=*/10);
  cpr.OnPrecommit(write_set_for("V1"), /*current_epoch=*/10);
  item.Reset(reinterpret_cast<const std::byte*>("V1"), 2);
  cpr.RotateDirtySets(11);
  WaitForCheckpointCompleted(cpr, 10);

  auto round1 = ReadDeltaFile(config_.work_dir, 10);
  ASSERT_EQ(round1.size(), 1u);
  ASSERT_EQ(round1[0].key_value_pairs.size(), 1u);
  ASSERT_EQ(round1[0].key_value_pairs[0].buffer, "V0");

  // Round 2, on the same item: must take a stable copy of "V1" (the value
  // as of just before round 2 started) for round 2's own version_ (20), not
  // leak or reuse anything from round 1's now-consumed stable copy.
  cpr.RotateDirtySets(/*next_checkpoint_epoch=*/20);
  cpr.OnPrecommit(write_set_for("V2"), /*current_epoch=*/20);
  item.Reset(reinterpret_cast<const std::byte*>("V2"), 2);
  cpr.RotateDirtySets(21);
  WaitForCheckpointCompleted(cpr, 20);

  auto round2 = ReadDeltaFile(config_.work_dir, 20);
  ASSERT_EQ(round2.size(), 1u);
  ASSERT_EQ(round2[0].key_value_pairs.size(), 1u);
  EXPECT_EQ(round2[0].key_value_pairs[0].buffer, "V1")
      << "Round 2 must independently protect its own pre-round value and "
         "the phase machine must correctly cycle back through REST to "
         "start a second round at all.";
}

// Regression test for 2PL support: TwoPhaseLockingImpl::Write() calls
// ProtectStableCopy() directly and *before* eagerly mutating item->buffer,
// independently of OnPrecommit -- unlike Silo/SiloNWR, where
// PrecommitWriteSet takes the stable copy itself, ahead of "Buffer Update"
// (see PrecommitWriteSet's comment). This test drives that exact sequence
// (protect, then mutate, then a separate OnPrecommit call for
// dirty-tracking only, mirroring Write() and Precommit() being two
// separate calls in TwoPhaseLockingImpl) through CPRManager's public API,
// with concurrency_control_protocol set to TwoPhaseLocking so
// PrecommitWriteSet's own protection loop is (correctly) skipped and only
// the explicit ProtectStableCopy call does the protecting.
TEST_F(CPRManagerWhiteBoxTest, ProtectStableCopyProtectsBeforeEagerMutation) {
  config_.concurrency_control_protocol =
      Config::ConcurrencyControl::TwoPhaseLocking;
  EpochFramework epoch;
  epoch.Start();
  TableDictionary dict;
  CPRManager cpr(config_, dict, epoch);

  const std::string table_name = "t";
  const std::string key = "k";
  DataItem item;
  auto write_set_for = [&](const std::string& value) {
    WriteSetType ws;
    ws.emplace_back(key, reinterpret_cast<const std::byte*>(value.data()),
                    value.size(), &item, table_name);
    return ws;
  };

  // Establish an initial, already-committed value during REST.
  cpr.OnPrecommit(write_set_for("OLD1"), /*current_epoch=*/3);
  item.Reset(reinterpret_cast<const std::byte*>("OLD1"), 4);

  cpr.RotateDirtySets(/*next_checkpoint_epoch=*/10);

  // Mirror TwoPhaseLockingImpl::Write(): protect *before* the eager
  // mutation, then mutate. OnPrecommit -- dirty-tracking only, since
  // PrecommitWriteSet's own protection loop is skipped for TwoPhaseLocking
  // -- comes later, exactly as it does from Precommit().
  cpr.ProtectStableCopy(&item, /*current_epoch=*/10);
  item.Reset(reinterpret_cast<const std::byte*>("NEW1"), 4);
  cpr.OnPrecommit(write_set_for("NEW1"), /*current_epoch=*/10);

  cpr.RotateDirtySets(11);
  WaitForCheckpointCompleted(cpr, 10);

  auto records = ReadDeltaFile(config_.work_dir, 10);
  ASSERT_EQ(records.size(), 1u);
  ASSERT_EQ(records[0].key_value_pairs.size(), 1u);
  EXPECT_EQ(records[0].key_value_pairs[0].buffer, "OLD1")
      << "Round 10's snapshot must represent state as of just before the "
         "round started, even for a protocol (2PL) that protects ahead of "
         "OnPrecommit rather than from within it.";
}

}  // namespace Recovery

// End-to-end non-regression check that 2PL's real Write()/Precommit() path
// (via the actual async Database/Transaction API, not CPRManager's API
// directly -- 2PL's write path lives in Transaction::Write(), only
// reachable this way) still recovers correctly now that Write() itself
// calls CPRManager::ProtectStableCopy(). The mechanism itself (that
// ProtectStableCopy correctly protects a pre-round value when called ahead
// of, and independently from, OnPrecommit) is covered precisely and
// deterministically by
// CPRManagerWhiteBoxTest.ProtectStableCopyProtectsBeforeEagerMutation
// above; this test only needs to confirm the real integration doesn't
// silently break end-to-end recovery -- same pattern as CPRTest in
// tests/cpr_test.cpp.
class TwoPhaseLockingCheckpointTest : public ::testing::Test {
 protected:
  Config config_;
  std::unique_ptr<Database> db_;

  void SetUp() override {
    std::filesystem::remove_all(config_.work_dir);
    config_.max_thread = 4;
    config_.concurrency_control_protocol =
        Config::ConcurrencyControl::TwoPhaseLocking;
    config_.durability = Config::DurabilityStrategy::Checkpoint;
    config_.enable_recovery = false;
    config_.epoch_duration_ms = 10;
    config_.checkpoint_period = 0;  // driven every tick, as in CPRTest
    db_ = std::make_unique<Database>(config_);
  }
  void TearDown() override {
    db_.reset();
    std::filesystem::remove_all(config_.work_dir);
  }
};

TEST_F(TwoPhaseLockingCheckpointTest, OverwriteSameKeyRecoversLatestValue) {
  // Several overwrites of the same key, each given time to become durable
  // (Fence() waits on the durability-gated commit callback, so this
  // synchronizes with the real checkpoint round regardless of how many
  // ticks a round now takes) -- if 2PL's writes were ever incorrectly
  // captured via a stale stable copy instead of the eventual live value,
  // recovery would surface an old value here.
  for (int i = 0; i < 5; ++i) {
    std::atomic<bool> done{false};
    db_->ExecuteTransaction(
        [i](Transaction& tx) { tx.Write<int>("key", i * 10); },
        [&](TxStatus) { done.store(true); }, std::nullopt);
    while (!done.load()) std::this_thread::yield();
    db_->Fence();
  }

  db_.reset();
  config_.enable_recovery = true;
  db_ = std::make_unique<Database>(config_);

  std::atomic<bool> verified{false};
  db_->ExecuteTransaction(
      [&](Transaction& tx) {
        auto res = tx.Read<int>("key");
        if (res.has_value()) {
          EXPECT_EQ(res.value(), 40);
          verified.store(true);
        }
      },
      [](TxStatus) {}, std::nullopt);
  db_->Fence();
  EXPECT_TRUE(verified.load());
}

}  // namespace LineairDB
