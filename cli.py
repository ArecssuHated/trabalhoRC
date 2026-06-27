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
# Arquivo: cli.py]
# ==============================================================================
import sys

def cli_keyboard_listener(queue_cli_to_router, peer_id):
    """Lê o teclado de forma bloqueante e repassa os comandos formatados para o Roteador."""
    print("\n=======================================================")
    print(f" P2P CHAT - USUÁRIO ATIVO: {peer_id}")
    print("=======================================================\n")
    
    while True:
        try:
            user_input = input("> ").strip()
            if not user_input: continue
            
            parts = user_input.split(" ", 1)
            command = parts[0]
            
            # Encaminha o comando de sair
            if command == "/quit":
                queue_cli_to_router.put({"action": "QUIT"})
                break
                
            # Valida e encaminha mensagem Unicast (Direta)
            elif command == "/msg":
                if len(parts) < 2: continue
                sub_parts = parts[1].split(" ", 1)
                if len(sub_parts) < 2: continue
                queue_cli_to_router.put({"action": "SEND", "dst": sub_parts[0], "payload": sub_parts[1]})
                
            # Valida e encaminha mensagem Broadcast/Namespace
            elif command == "/pub":
                if len(parts) < 2: continue
                sub_parts = parts[1].split(" ", 1)
                if len(sub_parts) < 2: continue
                queue_cli_to_router.put({"action": "PUB", "dst": sub_parts[0], "payload": sub_parts[1]})
                
            # Comandos simples de diagnóstico
            elif command in ["/peers", "/conn", "/rtt", "/reconnect"]:
                queue_cli_to_router.put({"action": command.replace("/", "").upper()})
                
        except (KeyboardInterrupt, EOFError): # Trata Ctrl+C 
            queue_cli_to_router.put({"action": "QUIT"})
            break

def cli_ui_display(queue_network_to_ui):
    """Thread dedicada a imprimir mensagens recebidas sem quebrar o layout do 'input()' atual."""
    while True:
        ui_event = queue_network_to_ui.get()
        origin = ui_event.get("origin", "SYSTEM")
        content = ui_event.get("content", "")
        
        # Mantém a estética visual limpa recriando o prompt ">"
        if origin == "SYSTEM":
            print(f"\n[SISTEMA] {content}\n> ", end="")
        else:
            print(f"\n[{origin}]: {content}\n> ", end="")
            
        queue_network_to_ui.task_done()