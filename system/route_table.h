#ifndef _ROUTE_TABLE_H_
#define _ROUTE_TABLE_H_

#include "global.h"
#include "helper.h"

#define SIZE_OF_ROUTE (sizeof(route_table_node) * PART_CNT)
#define SIZE_OF_STATUS (sizeof(status_node) * NODE_CNT)
enum NS { OnCall = 0, Failure };
struct route_node_ts {
  uint64_t node_id;
  uint64_t last_ts;
};
struct route_table_node {
 public:
  uint64_t partition_id;
  route_node_ts primary;
  route_node_ts secondary_1;
  route_node_ts secondary_2;

  route_node_ts new_secondary[MAX_REPLICA_COUNT];
  uint64_t replica_cnt;
};
class RouteTable {
 public:
  void init();

  auto get_route_table_node(uint64_t partition_id) -> route_table_node {
    return table[partition_id];
  }
  auto get_route_node_new(int index, uint64_t partition_id) -> route_node_ts;
  void set_route_node_new(int index, uint64_t partition_id, uint64_t node_id,
                          uint64_t timestamp = 0, uint64_t thd_id = 0);

  route_node_ts get_primary(uint64_t partition_id);
  route_node_ts get_secondary_1(uint64_t partition_id);
  route_node_ts get_secondary_2(uint64_t partition_id);
  void set_primary(uint64_t partition_id, uint64_t node_id, uint64_t timestamp = 0,
                   uint64_t thd_id = 0);
  void set_secondary_1(uint64_t partition_id, uint64_t node_id, uint64_t timestamp = 0,
                       uint64_t thd_id = 0);
  void set_secondary_2(uint64_t partition_id, uint64_t node_id, uint64_t timestamp = 0,
                       uint64_t thd_id = 0);
  // private:
  route_table_node* table;

  void printRouteTable() {
    for (int i = 0; i < PART_CNT; i++) {
      PRINT_HEARTBEAT("partition %d: primary %d, secondary1 %d, secondary2 %d\n", i,
                      table[i].new_secondary[0].node_id, table[i].new_secondary[1].node_id,
                      table[i].new_secondary[2].node_id);
    }
  }
};

struct status_node {
 public:
  NS status;
  uint64_t last_ts;
};
class NodeStatus {
 public:
  void init();
  status_node* get_node_status(uint64_t node_id);
  void set_node_status(uint64_t node_id, NS newSatus, uint64_t thd_id);
  // private:
  status_node* table;

  void printStatusTable() {
    for (int i = 0; i < NODE_CNT; i++) {
      PRINT_HEARTBEAT("node %d last_ts: %lu, status %d\n", i, table[i].last_ts, table[i].status);
    }
  }
};

class RouteAndStatus {
 public:
  status_node* _status;
  route_table_node* _route;
  void printRouteTable() {
    for (int i = 0; i < PART_CNT; i++) {
      PRINT_HEARTBEAT("partition %d: primary %d, secondary1 %d, secondary2 %d\n", i,
                      _route[i].new_secondary[0].node_id, _route[i].new_secondary[1].node_id,
                      _route[i].new_secondary[2].node_id);
    }
    for (int i = 0; i < NODE_CNT; i++) {
      PRINT_HEARTBEAT("node status %d: last_ts %lu, status %d\n", i, _status[i].last_ts,
                      _status[i].status);
    }
  }
};

class Replica {
 public:
  Replica(uint64_t pid, uint64_t rid) : partition_id(pid), replica_id(rid) {}
  uint64_t partition_id;
  uint64_t replica_id;
};

// ! Recovery manager section
/*
 * In RedT prototype, assume we have 4 data center, and 12 data nodes
 * 0, 4, 8 in data center 1
 * 1, 5, 9 in data center 2
 * 2, 6, 10 in data center 3
 * 3, 7, 11 in data center 4.
 *
 * And we assume the RM in first node in each data center as the main RM.
 * When the first node fails, the main RM will be the second node.
 * Use the above Example, RMs in 0, 1, 2 and 3 are the main RM.
 * And 4 will be the new main RM when 0 fails.
 */
inline uint64_t get_center_primary(uint64_t center_id) {
  assert(g_node_cnt % CENTER_CNT == 0);
  uint64_t node_cnt_per_dc = g_node_cnt / CENTER_CNT;
  for (int i = 0; i < node_cnt_per_dc; i++) {
    uint64_t node_id = i * CENTER_CNT + center_id;
    status_node* st = node_status.get_node_status(node_id);
    if (st->status == NS::Failure) continue;
    return node_id;
  }
  return -1;
}
inline bool is_center_primary(uint64_t nid) {
  assert(g_node_cnt % CENTER_CNT == 0);
  uint64_t node_cnt_per_dc = g_node_cnt / CENTER_CNT;
  uint64_t center_id = GET_CENTER_ID(nid);
  for (int i = 0; i < node_cnt_per_dc; i++) {
    uint64_t node_id = i * CENTER_CNT + center_id;
    status_node* st = node_status.get_node_status(node_id);
    if (st->status == NS::Failure) {
      // DEBUG_H("Node %ld failed in center %d\n", node_id, center_id);
      continue;
    }
    return node_id == nid;
  }
  assert(false);
}
inline uint64_t get_global_primary(uint64_t center_id) {
  for (int i = 0; i < g_node_cnt; i++) {
    uint64_t node_id = i;
    status_node* st = node_status.get_node_status(node_id);
    if (st->status == NS::Failure) continue;
    return i;
  }
}

inline uint64_t get_primary_node_id(uint64_t part_id) {
  uint64_t node_id = route_table.get_primary(part_id).node_id;
  status_node* st = node_status.get_node_status(node_id);
  if (st->status == NS::Failure) return -1;
  return node_id;
}
inline uint64_t get_follower1_node_id(uint64_t part_id) {
  uint64_t node_id = route_table.get_secondary_1(part_id).node_id;
  status_node* st = node_status.get_node_status(node_id);
  if (st->status == NS::Failure) return -1;
  return node_id;
}
inline uint64_t get_follower2_node_id(uint64_t part_id) {
  uint64_t node_id = route_table.get_secondary_2(part_id).node_id;
  status_node* st = node_status.get_node_status(node_id);
  if (st->status == NS::Failure) return -1;
  return node_id;
}

inline auto get_node_id_new(int index, uint64_t part_id) -> uint64_t {
  uint64_t node_id = route_table.get_route_node_new(index, part_id).node_id;
  status_node* st = node_status.get_node_status(node_id);
  if (st->status == NS::Failure) return -1;
  return node_id;
}

inline auto get_part_repl_cnt(uint64_t part_id) -> uint64_t {
  route_table_node node_id = route_table.get_route_table_node(part_id);
  return node_id.replica_cnt;
}
// #define IS_CENTER_PRIMARY(nid) ((nid / g_center_cnt) == 0)
// ! Recovery manager section end

#endif