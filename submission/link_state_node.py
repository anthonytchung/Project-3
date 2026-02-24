import json

from simulator.node import Node


class Link_State_Node(Node):
    def __init__(self, id):
        super().__init__(id)
        self.links = {}
        self.local_link_seq = {}
        self.lsdb = {}

    # Return a string
    def __str__(self):
        records = []
        for key in sorted(self.lsdb.keys(), key=lambda k: tuple(sorted(k))):
            record = self.lsdb[key]
            records.append(
                "(%d,%d):cost=%d,seq=%d"
                % (record["u"], record["v"], record["cost"], record["seq"])
            )
        return "LSNode(id=%d, links=%s, lsdb=[%s])" % (
            self.id,
            sorted(self.links.items()),
            ", ".join(records),
        )

    def _link_key(self, node_a, node_b):
        return frozenset((node_a, node_b))

    def _send_ls_record(self, neighbor, record):
        message = {
            "type": "LS",
            "sender": self.id,
            "u": record["u"],
            "v": record["v"],
            "cost": record["cost"],
            "seq": record["seq"],
        }
        self.send_to_neighbor(neighbor, json.dumps(message, sort_keys=True, separators=(",", ":")))

    def _flood_record(self, record, exclude_neighbor=None):
        for neighbor in self.links.keys():
            if neighbor == exclude_neighbor:
                continue
            self._send_ls_record(neighbor, record)

    def _sync_neighbor(self, neighbor):
        for key in sorted(self.lsdb.keys(), key=lambda k: tuple(sorted(k))):
            self._send_ls_record(neighbor, self.lsdb[key])

    # Fill in this function
    def link_has_been_updated(self, neighbor, latency):
        # latency = -1 if delete a link
        if latency == -1:
            self.links.pop(neighbor, None)
        else:
            self.links[neighbor] = latency
        self.neighbors = sorted(self.links.keys())

        link_key = self._link_key(self.id, neighbor)
        next_seq = self.local_link_seq.get(link_key, 0) + 1
        self.local_link_seq[link_key] = next_seq

        node_a, node_b = sorted((self.id, neighbor))
        record = {"u": node_a, "v": node_b, "cost": latency, "seq": next_seq}
        self.lsdb[link_key] = record

        # Flood the updated link state to all current neighbors.
        self._flood_record(record)

        # Anti-entropy: send the full LSDB to a newly reachable neighbor.
        if latency != -1 and neighbor in self.links:
            self._sync_neighbor(neighbor)

    # Fill in this function
    def process_incoming_routing_message(self, m):
        try:
            message = json.loads(m)
        except (TypeError, ValueError):
            return
        if not isinstance(message, dict) or message.get("type") != "LS":
            return

        try:
            sender = int(message.get("sender"))
            node_a = int(message.get("u"))
            node_b = int(message.get("v"))
            cost = int(message.get("cost"))
            seq = int(message.get("seq"))
        except (TypeError, ValueError):
            return

        if sender not in self.links:
            return
        if node_a == node_b:
            return

        link_key = self._link_key(node_a, node_b)
        current = self.lsdb.get(link_key)

        if current is None or seq > current["seq"]:
            normalized_a, normalized_b = sorted((node_a, node_b))
            new_record = {"u": normalized_a, "v": normalized_b, "cost": cost, "seq": seq}
            self.lsdb[link_key] = new_record
            self._flood_record(new_record, exclude_neighbor=sender)
        elif seq < current["seq"]:
            # Sender is stale; return our newest version for this link.
            self._send_ls_record(sender, current)

    def _build_adjacency(self):
        adjacency = {self.id: {}}
        for record in self.lsdb.values():
            if record["cost"] < 0:
                continue
            node_a = record["u"]
            node_b = record["v"]
            weight = record["cost"]
            adjacency.setdefault(node_a, {})
            adjacency.setdefault(node_b, {})
            adjacency[node_a][node_b] = weight
            adjacency[node_b][node_a] = weight
        return adjacency

    # Return a neighbor, -1 if no path to destination
    def get_next_hop(self, destination):
        if destination == self.id:
            return self.id

        adjacency = self._build_adjacency()
        if self.id not in adjacency or destination not in adjacency:
            return -1

        infinity = float("inf")
        unvisited = set(adjacency.keys())
        distance = {node: infinity for node in adjacency.keys()}
        previous = {}
        distance[self.id] = 0

        while unvisited:
            current = min(unvisited, key=lambda node: (distance[node], node))
            if distance[current] == infinity:
                break
            unvisited.remove(current)
            if current == destination:
                break

            for neighbor, weight in adjacency[current].items():
                candidate = distance[current] + weight
                if candidate < distance[neighbor]:
                    distance[neighbor] = candidate
                    previous[neighbor] = current
                elif candidate == distance[neighbor]:
                    if previous.get(neighbor) is None or current < previous[neighbor]:
                        previous[neighbor] = current

        if distance.get(destination, infinity) == infinity:
            return -1

        current = destination
        while True:
            parent = previous.get(current)
            if parent is None:
                return -1
            if parent == self.id:
                return current
            current = parent
