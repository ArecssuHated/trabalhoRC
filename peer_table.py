# ==============================================================================
# Disciplina: Redes de Computadores
# Projeto: Chat P2P com Servidor Rendezvous
# Grupo: 5
# Namespace: CIC
#
# Arquivo: peer_table.py
# ==============================================================================
import threading

class PeerTable:
    def __init__(self):
        self.peers = {} 
        self.lock = threading.Lock() 

    def update_peers(self, discovered_peers):
        with self.lock:
            self.peers.clear()
            for peer in discovered_peers:
                peer_id = f"{peer['name']}@{peer['namespace']}"
                self.peers[peer_id] = peer
            
    def get_all_peers(self):
        with self.lock:
            return list(self.peers.values())

    def get_peer(self, peer_id):
        with self.lock:
            return self.peers.get(peer_id)

    def marcar_como_stale(self, peer_id):
        with self.lock:
            if peer_id in self.peers:
                self.peers[peer_id]['status'] = 'STALE'