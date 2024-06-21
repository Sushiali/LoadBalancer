import os
import random
from typing import Tuple, Dict
from flask import Flask, request
import requests
from consistent_hash import ConsistentHash
from parsers import response_parser



class LoadBalancer:
    

    def __init__(self):
        self.app = Flask("LoadBalancer")
        self.consistent_hash = ConsistentHash()
        self.replicas = set()
        self.handle_routes()

    def get_server(self) -> Tuple[str, int]:
        
        request_id = random.randint(1000, 9999)
        server = self.consistent_hash.get_server(str(request_id))
        return server, 4000

    def get_replicas(self):
        
        response = {
            "message": {"N": len(self.replicas), "replicas": list(self.replicas)},
            "status": "success",
        }
        return response_parser(response)

    def add_replica(self):
        
        if request.method == "POST":
            data = request.json
            n = data.get("n", 1)
            hostnames = data.get("hostnames", [])
            if len(hostnames) != n:
                return response_parser(
                    "Number of hostnames do not match the number of replicas", 400
                )
            for hostname in hostnames:
                if hostname in self.replicas:
                    return response_parser(f"{hostname} already in the replicas", 400)
                try:
                    self.handle_add(hostname)
                except Exception as e:
                    print(f"Error adding replica: {e}")
                    return response_parser(str(e), 500)
            return self.get_replicas()

    def handle_add(self, hostname: str):
        try:
            self.spawn(hostname)
            self.replicas.add(hostname)
            self.consistent_hash.add_server(hostname)
            print(f"Added {hostname} to the replicas")
        except Exception as e:
            print(f"Error in handle_add: {e}")
            raise

    def remove_replica(self):
        if request.method == "DELETE":
            data = request.json
            hostnames = data.get("hostnames", [])
            n = data.get("n", 1)
            if len(hostnames) != n:
                return response_parser(
                    "Number of hostnames do not match the number of replicas", 400
                )
            for hostname in hostnames:
                if hostname not in self.replicas:
                    return response_parser(f"{hostname} not in the replicas", 400)
                try:
                    self.handle_remove(hostname)
                except Exception as e:
                    print(f"Error removing replica: {e}")
                    return response_parser(str(e), 500)
            return self.get_replicas()

    def handle_remove(self, hostname: str):
        try:
            self.kill(hostname)
            self.replicas.remove(hostname)
            self.consistent_hash.remove_server(hostname)
            print(f"Removed {hostname} from the replicas")
        except Exception as e:
            print(f"Error in handle_remove: {e}")
            raise

    def spawn(self, server_name: str):
        command = (
            f"sudo docker run --name {server_name} -d --network=app-network "
            f"-e SERVER_ID={server_name} --rm server_image"
        )
        res = os.popen(command).read()

        if not res:
            raise Exception("Could not start container")
        print("Successfully started container")

    def kill(self, server_name: str):
        command = f"sudo docker kill {server_name}"
        res = os.popen(command).read()

        if not res:
            raise Exception("Could not stop container")
        print("Successfully stopped container")

    def run(self, host: str, port: int):
        print(f"Load balancer is running on {host}:{port}")
        self.app.run(host=host, port=port)

    def handle_routes(self):
        
        self.app.add_url_rule("/rep", "get_replicas", self.get_replicas, methods=["GET"])
        self.app.add_url_rule("/add", "add_replica", self.add_replica, methods=["POST"])
        self.app.add_url_rule("/rm", "remove_replica", self.remove_replica, methods=["DELETE"])
        self.app.add_url_rule("/<path:path>", "forward", self.forward)

    def forward(self, path: str, method: str = "GET") -> str:
        server = self.get_server()
        url = f"http://{server[0]}:{server[1]}/{path}"
        response = requests.get(url)
        return response.text

load_balancer = LoadBalancer()
print("LOAD BALANCER RUNNING")
load_balancer.run(host="0.0.0.0", port=8000)