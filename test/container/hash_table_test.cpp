//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  int batch = 496*2;
  for (int i = 0; i < batch; ++i) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
    {
      for (int j = 0; j < i; ++j) {
        std::vector<int> res;
        ht.GetValue(nullptr, j, &res);
        EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
        EXPECT_EQ(j, res[0]);
      }
    }
  }
  for (int i = batch; i < 2*batch; ++i) {
    if (i == 1047) {
      printf("sdffds");
    }
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
    {
      for (int j = 0; j < i; ++j) {
        std::vector<int> res;
        ht.GetValue(nullptr, j, &res);
        if (res.size() != 1) {
          printf("%d, %d\n", i, j);
          EXPECT_EQ(1, res.size());
        }
        EXPECT_EQ(j, res[0]);
      }
    }
  }
  printf("middle global depth is %d\n", ht.GetGlobalDepth());
  // {
  //   printf("test split\n");
  //   ht.Insert(nullptr, 1000, 1000);
  //   std::vector<int> res;
  //   ht.GetValue(nullptr, 1000, &res);
  //   EXPECT_EQ(1, res.size()) << "Failed to insert " << 1000 << std::endl;
  //   EXPECT_EQ(1000, res[0]);
  // }

  for (int i = 0; i < 2*batch; ++i) {
    EXPECT_FALSE(ht.Insert(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  printf("middle global depth is %d\n", ht.GetGlobalDepth());

  for (int i = 0; i < 2*batch; ++i) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(0, res.size()) << "Failed to delete" << i << std::endl;
  }
  // {
  //   printf("test shrink\n");
  //   ht.Remove(nullptr, 1000, 1000);
  //   std::vector<int> res;
  //   ht.GetValue(nullptr, 1000, &res);
  //   EXPECT_EQ(0, res.size()) << "Failed to delete " << 1000 << std::endl;
  // }
  printf("final global depth is %d\n", ht.GetGlobalDepth());
  EXPECT_EQ(ht.GetGlobalDepth(), 0);
  // // insert a few values
  // for (int i = 0; i < 5; i++) {
  //   ht.Insert(nullptr, i, i);
  //   std::vector<int> res;
  //   ht.GetValue(nullptr, i, &res);
  //   EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
  //   EXPECT_EQ(i, res[0]);
  // }

  // ht.VerifyIntegrity();

  // // check if the inserted values are all there
  // for (int i = 0; i < 5; i++) {
  //   std::vector<int> res;
  //   ht.GetValue(nullptr, i, &res);
  //   EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
  //   EXPECT_EQ(i, res[0]);
  //   printf("%d \n", i);
  // }

  // ht.VerifyIntegrity();

  // // insert one more value for each key
  // for (int i = 0; i < 5; i++) {
  //   if (i == 0) {
  //     // duplicate values for the same key are not allowed
  //     EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
  //   } else {
  //     EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
  //   }
  //   ht.Insert(nullptr, i, 2 * i);
  //   std::vector<int> res;
  //   ht.GetValue(nullptr, i, &res);
  //   if (i == 0) {
  //     // duplicate values for the same key are not allowed
  //     EXPECT_EQ(1, res.size());
  //     EXPECT_EQ(i, res[0]);
  //   } else {
  //     EXPECT_EQ(2, res.size());
  //     if (res[0] == i) {
  //       EXPECT_EQ(2 * i, res[1]);
  //     } else {
  //       EXPECT_EQ(2 * i, res[0]);
  //       EXPECT_EQ(i, res[1]);
  //     }
  //   }
  // }

  // ht.VerifyIntegrity();

  // // look for a key that does not exist
  // std::vector<int> res;
  // ht.GetValue(nullptr, 20, &res);
  // EXPECT_EQ(0, res.size());

  // // delete some values
  // for (int i = 0; i < 5; i++) {
  //   EXPECT_TRUE(ht.Remove(nullptr, i, i));
  //   std::vector<int> res;
  //   ht.GetValue(nullptr, i, &res);
  //   if (i == 0) {
  //     // (0, 0) is the only pair with key 0
  //     EXPECT_EQ(0, res.size());
  //   } else {
  //     EXPECT_EQ(1, res.size());
  //     EXPECT_EQ(2 * i, res[0]);
  //   }
  // }

  // ht.VerifyIntegrity();

  // // delete all values
  // for (int i = 0; i < 5; i++) {
  //   if (i == 0) {
  //     // (0, 0) has been deleted
  //     EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
  //   } else {
  //     EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
  //   }
  // }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
