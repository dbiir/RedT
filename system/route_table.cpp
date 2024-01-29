#include "route_table.h"

#include "global.h"
#include "manager.h"

void RouteTable::init() {
  uint64_t table_size = (PART_CNT) * sizeof(route_table_node);
  assert(table_size < rdma_routetable_size);
  table = (route_table_node*)rdma_routetable_buffer;
  for (int i = 0; i < PART_CNT; i++) {
    table[i].partition_id = i;
    table[i].primary.node_id = GET_NODE_ID(i);
    table[i].secondary_1.node_id = GET_FOLLOWER1_NODE(i);
    table[i].secondary_2.node_id = GET_FOLLOWER2_NODE(i);
    table[i].primary.last_ts = get_wall_clock();
    table[i].secondary_1.last_ts = get_wall_clock();
    table[i].secondary_2.last_ts = get_wall_clock();

    /*new*/
    table[i].partition_id = i;
    table[i].replica_cnt = REPLICA_COUNT;
    for (int j = 0; j < REPLICA_COUNT; j++) {
      table[i].new_secondary[j].node_id = (i + j) % g_node_cnt;
      table[i].new_secondary[j].last_ts = get_wall_clock();
    }

    printf("Route table after initing:\n partition %d: primary %d, secondary1 %d, secondary2 %d\n",
           i, table[i].new_secondary[0].node_id, table[i].new_secondary[1].node_id,
           table[i].new_secondary[2].node_id);
    fflush(stdout);
  }
}

auto RouteTable::get_route_node_new(int index, uint64_t partition_id) -> route_node_ts {
  return table[partition_id].new_secondary[index];
}

void RouteTable::set_route_node_new(int index, uint64_t partition_id, uint64_t node_id,
                                    uint64_t timestamp, uint64_t thd_id) {
  table[partition_id].new_secondary[index].node_id = node_id;
  if (timestamp == 0)
    table[partition_id].new_secondary[index].last_ts = get_wall_clock();
  else
    table[partition_id].new_secondary[index].last_ts = timestamp;
}

route_node_ts RouteTable::get_primary(uint64_t partition_id) { return table[partition_id].primary; }

route_node_ts RouteTable::get_secondary_1(uint64_t partition_id) {
  return table[partition_id].secondary_1;
}
route_node_ts RouteTable::get_secondary_2(uint64_t partition_id) {
  return table[partition_id].secondary_2;
}
void RouteTable::set_primary(uint64_t partition_id, uint64_t node_id, uint64_t timestamp,
                             uint64_t thd_id) {
  table[partition_id].primary.node_id = node_id;
  if (timestamp == 0)
    table[partition_id].primary.last_ts = get_wall_clock();
  else
    table[partition_id].primary.last_ts = timestamp;
}
void RouteTable::set_secondary_1(uint64_t partition_id, uint64_t node_id, uint64_t timestamp,
                                 uint64_t thd_id) {
  table[partition_id].secondary_1.node_id = node_id;
  if (timestamp == 0)
    table[partition_id].secondary_1.last_ts = get_wall_clock();
  else
    table[partition_id].secondary_1.last_ts = timestamp;
}
void RouteTable::set_secondary_2(uint64_t partition_id, uint64_t node_id, uint64_t timestamp,
                                 uint64_t thd_id) {
  table[partition_id].secondary_2.node_id = node_id;
  if (timestamp == 0)
    table[partition_id].secondary_2.last_ts = get_wall_clock();
  else
    table[partition_id].secondary_2.last_ts = timestamp;
}

void NodeStatus::init() {
  uint64_t node_table_size = (NODE_CNT) * sizeof(status_node);
  table = (status_node*)(rdma_routetable_buffer + (PART_CNT) * sizeof(route_table_node));
  for (int i = 0; i < NODE_CNT; i++) {
    table[i].status = OnCall;
    table[i].last_ts = get_wall_clock();
    DEBUG_H("Node Status init node %d ts %lu state %s\n", i, table[i].last_ts,
            table[i].status == OnCall ? "OnCall" : "Failure");
  }
}

status_node* NodeStatus::get_node_status(uint64_t node_id) { return &table[node_id]; }

void NodeStatus::set_node_status(uint64_t node_id, NS newStatus, uint64_t thd_id) {
  table[node_id].status = newStatus;
  table[node_id].last_ts = get_wall_clock();
}