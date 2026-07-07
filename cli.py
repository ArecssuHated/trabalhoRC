# ==============================================================================
# Disciplina: Redes de Computadores
# Projeto: Chat P2P com Servidor Rendezvous
# Grupo: 5
# Namespace: CIC
#
# Arquivo: cli.py
# ==============================================================================
import sys

def cli_keyboard_listener(queue_cli_to_router, peer_id):
    print("\n=======================================================")
    print(f" INTERFACE P2P - NO OPERANTE: {peer_id}")
    print("=======================================================\n")
    
    while True:
        try:
            user_input = input("> ").strip()
            if not user_input: continue
            
            parts = user_input.split(" ", 1)
            command = parts[0]
            
            if command == "/quit":
                queue_cli_to_router.put({"action": "QUIT"})
                break
            elif command == "/msg":
                if len(parts) < 2: continue
                sub_parts = parts[1].split(" ", 1)
                if len(sub_parts) < 2: continue
                queue_cli_to_router.put({"action": "SEND", "dst": sub_parts[0], "payload": sub_parts[1]})
            elif command == "/pub":
                if len(parts) < 2: continue
                sub_parts = parts[1].split(" ", 1)
                if len(sub_parts) < 2: continue
                queue_cli_to_router.put({"action": "PUB", "dst": sub_parts[0], "payload": sub_parts[1]})
            elif command in ["/peers", "/conn", "/rtt", "/reconnect"]:
                queue_cli_to_router.put({"action": command.replace("/", "").upper()})
                
        except (KeyboardInterrupt, EOFError):
            queue_cli_to_router.put({"action": "QUIT"})
            break

def cli_ui_display(queue_network_to_ui):
    while True:
        ui_event = queue_network_to_ui.get()
        origin = ui_event.get("origin", "SYSTEM")
        content = ui_event.get("content", "")
        
        if origin == "SYSTEM":
            print(f"\n[SISTEMA] {content}\n> ", end="", flush=True)
        else:
            print(f"\n[{origin}]: {content}\n> ", end="", flush=True)
            
        queue_network_to_ui.task_done()