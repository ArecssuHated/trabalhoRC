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
# Arquivo: message_router.py]
# ==============================================================================
import json
import uuid
import time
import sys

def message_router_worker(tabela, motor_p2p, queue_cli, queue_ui, meu_peer_id):
    """Cérebro de roteamento: Traduz os comandos de texto em conexões e pacotes JSON reais."""
    while True:
        event = queue_cli.get()
        action = event.get("action")
        
        if action == "QUIT":
            queue_ui.put({"origin": "SYSTEM", "content": "Encerrando aplicação..."})
            time.sleep(1)
            sys.exit(0)
            
        elif action == "SEND":
            dst = event.get("dst")
            payload = event.get("payload")
            
            # Passo 1: Valida se o destino está na nossa tabela local
            peer_info = tabela.get_peer(dst)
            if not peer_info:
                queue_ui.put({"origin": "SYSTEM", "content": f"Erro: O peer '{dst}' não foi encontrado."})
                queue_cli.task_done()
                continue
                
            # Passo 2: Empacota a mensagem estruturada
            pacote_send = {
                "type": "SEND", "msg_id": str(uuid.uuid4()), "src": meu_peer_id,
                "dst": dst, "payload": payload, "require_ack": True, "ttl": 1
            }
            
            # Passo 3: Tenta rotear fisicamente na rede para o IP de destino
            queue_ui.put({"origin": "SYSTEM", "content": f"Conectando a {dst}..."})
            # NOTA: Para testes puramente locais, troque peer_info['ip'] por "127.0.0.1"
            sock = motor_p2p.conectar_com_backoff(peer_info['ip'], peer_info['port'])
            if sock:
                motor_p2p.enviar_mensagem(sock, pacote_send)
                queue_ui.put({"origin": "SYSTEM", "content": f"Mensagem enviada com sucesso para {dst}."})
            else:
                queue_ui.put({"origin": "SYSTEM", "content": f"Falha ao conectar com {dst}."})
            
        elif action == "PUB":
            dst = event.get("dst") # Pode ser '*' (Todos) ou '#namespace' (Ex: #UnB)
            payload = event.get("payload")
            
            pacote_pub = {
                "type": "PUB", "msg_id": str(uuid.uuid4()), "src": meu_peer_id,
                "dst": dst, "payload": payload, "require_ack": False, "ttl": 1
            }
            
            peers_ativos = tabela.get_all_peers()
            enviados = 0
            
            # Faz varredura na tabela para enviar a mensagem em massa (Broadcast)
            for p in peers_ativos:
                peer_id_alvo = f"{p['name']}@{p['namespace']}"
                if peer_id_alvo == meu_peer_id: continue # Não envia de volta para si mesmo
                
                # Se for mensagem de grupo, ignora quem não for do grupo especificado
                if dst.startswith("#"):
                    alvo_ns = dst[1:]
                    if p['namespace'] != alvo_ns:
                        continue
                        
                sock = motor_p2p.conectar_com_backoff(p['ip'], p['port'], max_tentativas=1)
                if sock:
                    motor_p2p.enviar_mensagem(sock, pacote_pub)
                    enviados += 1
                    
            queue_ui.put({"origin": "SYSTEM", "content": f"Broadcast entregue a {enviados} peers."})
            
        elif action == "PEERS":
            peers = tabela.get_all_peers()
            lista = [f"{p['name']}@{p['namespace']}" for p in peers]
            queue_ui.put({"origin": "SYSTEM", "content": f"Peers Descobertos ({len(lista)}): {', '.join(lista)}"})
            
        queue_cli.task_done()