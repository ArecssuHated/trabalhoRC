# ==============================================================================
# Disciplina: Redes de Computadores
# Projeto: Chat P2P com Servidor Rendezvous
# Grupo: 5
# Namespace: CIC
#
# Arquivo: main.py
# ==============================================================================
import queue
import threading
import time
import json

from peer_table import PeerTable
from rendezvous_connection import RendezvousClient
from peer_connection import PeerNode
from cli import cli_keyboard_listener, cli_ui_display
from message_router import message_router_worker

def rotina_descoberta_automatica(rdv_client, tabela, intervalo_desc):
    while True:
        try:
            peers_ativos = rdv_client.discover(None)
            tabela.update_peers(peers_ativos)
        except Exception:
            pass 
        time.sleep(intervalo_desc)

if __name__ == "__main__":
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        config = {}

    MEU_NOME = config.get("name", "Grupo5")       
    MEU_NAMESPACE = config.get("namespace", "CIC")
    MINHA_PORTA = config.get("listen_port", 12000)
    MEU_HOST = config.get("listen_host", "0.0.0.0")
    RDV_HOST = config.get("rdv_host", "pyp2p.mfcaetano.cc")
    RDV_PORT = config.get("rdv_port", 8080)
    DISCOVER_INTERVAL = config.get("discover_interval", 60)
    RDV_TTL = config.get("rdv_ttl", 7200)

    MEU_PEER_ID = f"{MEU_NOME}@{MEU_NAMESPACE}"
    
    queue_cli_to_router = queue.Queue()
    queue_network_to_ui = queue.Queue()

    tabela_de_peers = PeerTable()
    rdv_client = RendezvousClient(host=RDV_HOST, port=RDV_PORT)
    motor_p2p = PeerNode(host=MEU_HOST, port=MINHA_PORTA, peer_id=MEU_PEER_ID, tabela=tabela_de_peers, queue_ui=queue_network_to_ui)

    rdv_client.register(MEU_NOME, MEU_NAMESPACE, MINHA_PORTA, ttl=RDV_TTL)

    threading.Thread(target=rotina_descoberta_automatica, args=(rdv_client, tabela_de_peers, DISCOVER_INTERVAL), daemon=True).start()
    threading.Thread(target=motor_p2p.iniciar_servidor, daemon=True).start()
    threading.Thread(target=message_router_worker, args=(tabela_de_peers, motor_p2p, queue_cli_to_router, queue_network_to_ui, MEU_PEER_ID), daemon=True).start()
    threading.Thread(target=cli_ui_display, args=(queue_network_to_ui,), daemon=True).start()

    try:
        cli_keyboard_listener(queue_cli_to_router, MEU_PEER_ID)
    except KeyboardInterrupt:
        pass
    finally:
        rdv_client.unregister(MEU_NOME, MEU_NAMESPACE, MINHA_PORTA)