// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;
using namespace std;

std::string kDBPath = "/tmp/rocksdb_simple_example";

void printLSM(DB* db) {
  ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);

  int largest_used_level = 0;
  for (auto level : cf_meta.levels) {
    if (level.files.size() > 0) {
      largest_used_level = level.level;
    }
  }

  for (auto level : cf_meta.levels) {

    long level_size = 0;
    for (auto file : level.files) {
      level_size += file.size;
    }

    cout << "level " << level.level << ".  Size " << level_size << " bytes" << endl;
    // for (auto file : level.files) {
      //printf("%s   ", file.name.c_str());
    // }
    cout << endl;
    for (auto file : level.files)
    {
      cout << " \t " << file.size << " bytes \t " << file.smallestkey << "-" << file.largestkey;
      cout << "    "  << file.name << endl;
    }
    if (level.level == largest_used_level) {
      break;
    }
  }
  cout << endl;
}

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  //options.OptimizeLevelStyleCompaction();
  options.OptimizeUniversalStyleCompaction();
  options.write_buffer_size = 1024 * 1024 / 2;

  // create the DB if it's not already present
  options.create_if_missing = true;

  rocksdb::DestroyDB(kDBPath, options);

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  s = db->Put(WriteOptions(), "5000000000target", "this is the test target at the largest level");

  // Put key-value
  for (int i = 0; i < 500000; i++) {
    int rand_key = rand();
    s = db->Put(WriteOptions(), std::to_string(rand_key), "value");
    assert(s.ok());
  }

  db->Flush(FlushOptions());


  printLSM(db);

  std::string value = "default";
  ReadOptions ro;

  ro.window_size = 10000;
  s = db->Get(ro, "5000000000target", &value);
  cout << "result of point lookup 1: " << value << endl;

  ro.window_size = 500000;
  s = db->Get(ro, "5000000000target", &value);
  cout << "result of point lookup 2: " << value << endl;
  //assert(s.ok());
  //assert(value == "value");

  ro.window_size = 10000;
  rocksdb::Iterator* it = db->NewIterator(ro);
  it->Seek("5000000000target");
  cout << "result of range lookup 1: " << it->value().ToString() << endl;

  ro.window_size = 500000;
  it = db->NewIterator(ro);
  it->Seek("5000000000target");
  cout << "result of range lookup 2: " << it->value().ToString() << endl;

  return 0;
}
