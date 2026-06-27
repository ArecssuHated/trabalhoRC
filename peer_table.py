# ==============================================================================
# Disciplina: Redes de Computadores
# Projeto: Chat P2P 
# Grupo: 5
# Namespace: CIC
#
# Integrantes:
# - Alexandre Henrique Diniz Paro (Matrícula: 232024349)
# - José Joaquim de Araújo Neto (Matrícula: 232027340)
# - Pedro Emanoel de Resende Santana (Matrícula: 200062646)
#
# Arquivo: peer_table.py]
# ==============================================================================
import threading

class PeerTable:
    """Gerencia a lista de peers ativos na rede de forma segura para múltiplas threads."""
    def __init__(self):
        # Dicionário para busca rápida O(1). Chave: "nome@namespace"
        self.peers = {} 
        # Lock impede que uma thread leia a tabela enquanto outra a atualiza (evita crash)
        self.lock = threading.Lock() 

    def update_peers(self, discovered_peers):
        """Atualiza a tabela local com a lista fresca recebida do servidor Rendezvous."""
        with self.lock:
            self.peers.clear()
            for peer in discovered_peers:
                peer_id = f"{peer['name']}@{peer['namespace']}"
                self.peers[peer_id] = peer
            
    def get_all_peers(self):
        """Retorna uma cópia da lista de peers para iteração segura."""
        with self.lock:
            return list(self.peers.values())

    def get_peer(self, peer_id):
        """Busca os dados de um peer específico pelo seu ID (ex: professor@UnB)."""
        with self.lock:
            return self.peers.get(peer_id)