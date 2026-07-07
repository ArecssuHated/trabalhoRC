# ==============================================================================
# Disciplina: Redes de Computadores
# Projeto: Chat P2P com Servidor Rendezvous
# Grupo: 5
# Namespace: CIC
#
# Arquivo: message_router.py
# ==============================================================================
import json
import uuid
import time
import sys

def message_router_worker(tabela, motor_p2p, queue_cli, queue_ui, meu_peer_id):
    while True:
        event = queue_cli.get()
        action = event.get("action")
        
        if action == "QUIT":
            peers_ativos = tabela.get_all_peers()
            
            pacote_bye = {
                "type": "BYE", "msg_id": str(uuid.uuid4()), "src": meu_peer_id
            }
            
            for p in peers_ativos:
                sock = motor_p2p.conectar_com_backoff(p['ip'], p['port'], max_tentativas=1)
                if sock:
                    motor_p2p.enviar_mensagem(sock, pacote_bye)
                    
            time.sleep(1) 
            sys.exit(0)
            
        elif action == "SEND":
            dst = event.get("dst")
            payload = event.get("payload")
            
            peer_info = tabela.get_peer(dst)
            if not peer_info:
                queue_ui.put({"origin": "SYSTEM", "content": f"Erro de roteamento: No '{dst}' indisponivel localmente."})
                queue_cli.task_done()
                continue
                
            pacote_send = {
                "type": "SEND", "msg_id": str(uuid.uuid4()), "src": meu_peer_id,
                "dst": dst, "payload": payload, "require_ack": True, "ttl": 1
            }
            
            sock = motor_p2p.conectar_com_backoff(peer_info['ip'], peer_info['port'])
            if sock:
                motor_p2p.enviar_mensagem(sock, pacote_send)
            
        elif action == "PUB":
            dst = event.get("dst") 
            payload = event.get("payload")
            
            pacote_pub = {
                "type": "PUB", "msg_id": str(uuid.uuid4()), "src": meu_peer_id,
                "dst": dst, "payload": payload, "require_ack": False, "ttl": 1
            }
            
            peers_ativos = tabela.get_all_peers()
            enviados = 0
            
            for p in peers_ativos:
                peer_id_alvo = f"{p['name']}@{p['namespace']}"
                if peer_id_alvo == meu_peer_id: continue 
                
                if dst.startswith("#"):
                    alvo_ns = dst[1:]
                    if p['namespace'] != alvo_ns: continue
                        
                sock = motor_p2p.conectar_com_backoff(p['ip'], p['port'], max_tentativas=1)
                if sock:
                    motor_p2p.enviar_mensagem(sock, pacote_pub)
                    enviados += 1
                    
            queue_ui.put({"origin": "SYSTEM", "content": f"Rotina de broadcast (PUB) encerrada. {enviados} envios executados."})
            
        elif action == "PEERS":
            peers = tabela.get_all_peers()
            lista = [f"{p['name']}@{p['namespace']}" for p in peers]
            queue_ui.put({"origin": "SYSTEM", "content": f"Inventario de rede ({len(lista)} nos alocados): {', '.join(lista)}"})
            
        elif action == "RECONNECT":
            queue_ui.put({"origin": "SYSTEM", "content": "Forcando reconexao com os pares (Handshake)..."})
            peers_ativos = tabela.get_all_peers()
            conexoes_restabelecidas = 0
            
            for p in peers_ativos:
                sock = motor_p2p.conectar_com_backoff(p['ip'], p['port'], max_tentativas=2)
                if sock:
                    conexoes_restabelecidas += 1
            
            queue_ui.put({"origin": "SYSTEM", "content": f"Ciclo de reconexao finalizado. {conexoes_restabelecidas} pares alcancados."})
            
        queue_cli.task_done()