import json

from simulator.node import Node


class Distance_Vector_Node(Node):
    def __init__(self, id):
        super().__init__(id)
        self.links = {}
        self.neighbor_state = {}
        self.routes = {self.id: {"next_hop": self.id, "cost": 0, "path": [self.id]}}
        self.local_seq = 0
        self.last_advertised_vector = None

    # Return a string
    def __str__(self):
        link_items = sorted(self.links.items())
        route_items = []
        for destination in sorted(self.routes.keys()):
            route = self.routes[destination]
            route_items.append(
                "%d:next=%d,cost=%d,path=%s"
                % (destination, route["next_hop"], route["cost"], route["path"])
            )
        return "DVNode(id=%d, links=%s, routes={%s})" % (
            self.id,
            link_items,
            ", ".join(route_items),
        )

    def _route_signature(self, routes):
        signature = {}
        for destination, route in routes.items():
            signature[destination] = (
                route["next_hop"],
                route["cost"],
                tuple(route["path"]),
            )
        return signature

    def _consider_route(self, routes, destination, next_hop, cost, path):
        if destination == self.id:
            return
        existing = routes.get(destination)
        if existing is None:
            routes[destination] = {"next_hop": next_hop, "cost": cost, "path": path}
            return

        should_replace = False
        if cost < existing["cost"]:
            should_replace = True
        elif cost == existing["cost"]:
            if next_hop < existing["next_hop"]:
                should_replace = True
            elif next_hop == existing["next_hop"] and path < existing["path"]:
                should_replace = True

        if should_replace:
            routes[destination] = {"next_hop": next_hop, "cost": cost, "path": path}

    def _sanitize_vector(self, vector):
        sanitized = {}
        if not isinstance(vector, dict):
            return sanitized
        for destination_key, entry in vector.items():
            if not isinstance(entry, dict):
                continue
            try:
                destination = int(destination_key)
                cost = int(entry.get("cost"))
            except (TypeError, ValueError):
                continue
            if cost < 0:
                continue
            path = entry.get("path")
            if not isinstance(path, list) or len(path) == 0:
                continue
            try:
                normalized_path = [int(node) for node in path]
            except (TypeError, ValueError):
                continue
            if len(set(normalized_path)) != len(normalized_path):
                continue
            sanitized[destination] = {"cost": cost, "path": normalized_path}
        return sanitized

    def _recompute_routes(self):
        old_signature = self._route_signature(self.routes)
        new_routes = {self.id: {"next_hop": self.id, "cost": 0, "path": [self.id]}}

        # Direct links are always valid route candidates.
        for neighbor, latency in self.links.items():
            if latency < 0:
                continue
            self._consider_route(
                new_routes,
                neighbor,
                neighbor,
                latency,
                [self.id, neighbor],
            )

        # Add path-vector routes advertised by neighbors.
        for neighbor, latency in self.links.items():
            if latency < 0:
                continue
            neighbor_info = self.neighbor_state.get(neighbor)
            if neighbor_info is None:
                continue
            vector = neighbor_info.get("vector", {})
            for destination, entry in vector.items():
                if destination == self.id:
                    continue
                advertised_path = entry["path"]
                if self.id in advertised_path:
                    continue

                base_path = advertised_path
                if base_path[0] != neighbor:
                    base_path = [neighbor] + base_path
                if len(set(base_path)) != len(base_path):
                    continue

                total_cost = latency + entry["cost"]
                full_path = [self.id] + base_path
                self._consider_route(
                    new_routes,
                    destination,
                    neighbor,
                    total_cost,
                    full_path,
                )

        self.routes = new_routes
        return old_signature != self._route_signature(self.routes)

    def _build_vector(self):
        vector = {}
        for destination, route in self.routes.items():
            if destination == self.id:
                continue
            vector[str(destination)] = {"cost": route["cost"], "path": route["path"]}
        return vector

    def _advertise(self):
        vector = self._build_vector()
        if vector == self.last_advertised_vector:
            return
        if not self.links:
            self.last_advertised_vector = vector
            return

        self.local_seq += 1
        message = {
            "type": "DV",
            "sender": self.id,
            "seq": self.local_seq,
            "vector": vector,
        }
        self.send_to_neighbors(json.dumps(message, sort_keys=True, separators=(",", ":")))
        self.last_advertised_vector = vector

    def _advertise_to_neighbor(self, neighbor):
        if neighbor not in self.links:
            return
        vector = self._build_vector()
        self.local_seq += 1
        message = {
            "type": "DV",
            "sender": self.id,
            "seq": self.local_seq,
            "vector": vector,
        }
        self.send_to_neighbor(neighbor, json.dumps(message, sort_keys=True, separators=(",", ":")))
        self.last_advertised_vector = vector

    # Fill in this function
    def link_has_been_updated(self, neighbor, latency):
        # latency = -1 if delete a link
        had_neighbor = neighbor in self.links
        if latency == -1:
            self.links.pop(neighbor, None)
            self.neighbor_state.pop(neighbor, None)
        else:
            self.links[neighbor] = latency
            if neighbor not in self.neighbor_state:
                self.neighbor_state[neighbor] = {"seq": -1, "vector": {}}

        self.neighbors = sorted(self.links.keys())
        routes_changed = self._recompute_routes()

        if routes_changed:
            self._advertise()
        elif latency != -1 and not had_neighbor:
            # Newly added neighbor may have missed prior updates.
            self._advertise_to_neighbor(neighbor)

    # Fill in this function
    def process_incoming_routing_message(self, m):
        try:
            message = json.loads(m)
        except (TypeError, ValueError):
            return
        if not isinstance(message, dict) or message.get("type") != "DV":
            return

        try:
            sender = int(message.get("sender"))
            seq = int(message.get("seq"))
        except (TypeError, ValueError):
            return

        # Ignore stale traffic from nodes that are no longer direct neighbors.
        if sender not in self.links:
            return

        state = self.neighbor_state.setdefault(sender, {"seq": -1, "vector": {}})
        if seq <= state["seq"]:
            return

        state["seq"] = seq
        state["vector"] = self._sanitize_vector(message.get("vector"))

        if self._recompute_routes():
            self._advertise()

    # Return a neighbor, -1 if no path to destination
    def get_next_hop(self, destination):
        route = self.routes.get(destination)
        if route is None:
            return -1
        return route["next_hop"]
