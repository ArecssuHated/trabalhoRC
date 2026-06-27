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
# Arquivo: main.py]
# ==============================================================================
import queue
import threading
import time

from peer_table import PeerTable
from rendezvous_connection import RendezvousClient
from peer_connection import PeerNode
from cli import cli_keyboard_listener, cli_ui_display
from message_router import message_router_worker

def rotina_descoberta_automatica(rdv_client, tabela):
    """Thread de varredura: Atualiza a tabela local mapeando a rede a cada 60s."""
    while True:
        try:
            # Passando None para buscar todos os namespaces globais (Radar Amplo)
            peers_ativos = rdv_client.discover(None)
            tabela.update_peers(peers_ativos)
        except Exception:
            pass 
        time.sleep(60)

if __name__ == "__main__":
    # 1. Configurações de Identidade do Peer
    MEU_NOME = "Grupo5"     
    MEU_NAMESPACE = "CIC"
    MINHA_PORTA = 12000
    MEU_PEER_ID = f"{MEU_NOME}@{MEU_NAMESPACE}"
    
    # 2. Filas assíncronas para comunicação segura entre a Interface Visual e a Rede
    queue_cli_to_router = queue.Queue()
    queue_network_to_ui = queue.Queue()

    # 3. Instanciação dos Módulos Core
    tabela_de_peers = PeerTable()
    rdv_client = RendezvousClient()
    motor_p2p = PeerNode(host='0.0.0.0', port=MINHA_PORTA, peer_id=MEU_PEER_ID)

    # 4. Anúncio no servidor central
    rdv_client.register(MEU_NOME, MEU_NAMESPACE, MINHA_PORTA)

    # 5. Inicialização das Threads em Paralelo (Daemon garante que fechem ao sair)
    
    # Mapeamento do servidor
    threading.Thread(target=rotina_descoberta_automatica, args=(rdv_client, tabela_de_peers), daemon=True).start()

    # Servidor local (Porta Aberta / Peer A)
    threading.Thread(target=motor_p2p.iniciar_servidor, daemon=True).start()

    # Roteador lógico (Peer B)
    threading.Thread(target=message_router_worker, args=(tabela_de_peers, motor_p2p, queue_cli_to_router, queue_network_to_ui, MEU_PEER_ID), daemon=True).start()

    # Atualizador da Tela
    threading.Thread(target=cli_ui_display, args=(queue_network_to_ui,), daemon=True).start()

    # 6. Inicia o bloqueio principal do programa lendo comandos do usuário
    try:
        cli_keyboard_listener(queue_cli_to_router, MEU_PEER_ID)
    except KeyboardInterrupt:
        pass
    finally:
        # Encerramento Gracioso (Grafully Shutdown): Avisa o servidor que estamos saindo
        print("\nEncerrando o programa e removendo registro da rede...")
        rdv_client.unregister(MEU_NOME, MEU_NAMESPACE, MINHA_PORTA)