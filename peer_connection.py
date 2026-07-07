# ==============================================================================
# Disciplina: Redes de Computadores
# Projeto: Chat P2P com Servidor Rendezvous
# Grupo: 5
# Namespace: CIC
#
# Arquivo: peer_connection.py
# ==============================================================================
import socket
import json
import threading
import time
import uuid
from datetime import datetime, timezone

class PeerNode:
    def __init__(self, host: str, port: int, peer_id: str, tabela=None, queue_ui=None):
        self.host = host
        self.port = port
        self.peer_id = peer_id
        self.tabela = tabela
        self.queue_ui = queue_ui
        self.conexoes_ativas = {}
        self.lock_conexoes = threading.Lock()

    def ler_max_tentativas(self):
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
                return config.get("max_reconnect_attempts", 5)
        except (FileNotFoundError, json.JSONDecodeError):
            return 5

    def registrar_ui(self, mensagem, origin="SISTEMA_REDE"):
        if self.queue_ui:
            self.queue_ui.put({"origin": origin, "content": mensagem})
        else:
            print(f"\n[{origin}] {mensagem}\n> ", end="", flush=True)

    def enviar_mensagem(self, sock, dicionario_msg):
        try:
            msg_str = json.dumps(dicionario_msg) + "\n"
            msg_bytes = msg_str.encode('utf-8')
            
            if len(msg_bytes) > 32768:
                return
                
            sock.sendall(msg_bytes)
        except Exception:
            pass 

    def processar_mensagem(self, sock, msg):
        tipo = msg.get("type")
        
        if tipo == "HELLO":
            resposta_ok = {
                "type": "HELLO_OK", "peer_id": self.peer_id, "version": "1.0",
                "features": ["ack", "metrics"], "ttl": 1
            }
            self.enviar_mensagem(sock, resposta_ok)
            
        elif tipo == "PING":
            pong_msg = {"type": "PONG", "msg_id": msg.get("msg_id"), "timestamp": msg.get("timestamp"), "ttl": 1}
            self.enviar_mensagem(sock, pong_msg)
            
        elif tipo == "PONG":
            pass

        elif tipo == "SEND":
            remetente = msg.get("src", "Desconhecido")
            conteudo = msg.get("payload", "")
            self.registrar_ui(conteudo, origin=f"{remetente} - PRIVADO")
            
            if msg.get("require_ack"):
                ack_msg = {"type": "ACK", "msg_id": msg.get("msg_id")}
                self.enviar_mensagem(sock, ack_msg)
                
        elif tipo == "PUB":
            remetente = msg.get("src", "Desconhecido")
            conteudo = msg.get("payload", "")
            grupo = msg.get("dst", "Broadcast")
            self.registrar_ui(conteudo, origin=f"{remetente} - GRUPO {grupo}")

        elif tipo == "BYE":
            resposta_bye_ok = {"type": "BYE_OK", "msg_id": msg.get("msg_id")}
            self.enviar_mensagem(sock, resposta_bye_ok)
            
        elif tipo == "BYE_OK":
             pass

    def rotina_de_escuta(self, sock):
        buffer = ""
        remote_peer_id = None
        
        while True:
            try:
                parte = sock.recv(4096).decode('utf-8')
                if not parte: break
                buffer += parte
                while '\n' in buffer:
                    linha, buffer = buffer.split('\n', 1)
                    linha = linha.strip()
                    if linha:
                        try:
                            msg_json = json.loads(linha)
                            if msg_json.get("type") in ["HELLO", "HELLO_OK", "PING", "SEND"]:
                                remote_peer_id = msg_json.get("peer_id", msg_json.get("src"))
                            self.processar_mensagem(sock, msg_json)
                        except Exception:
                            pass 
            except (socket.error, ConnectionResetError):
                break
            except Exception:
                break
        
        sock.close()
        
        if remote_peer_id and self.tabela:
            self.tabela.marcar_como_stale(remote_peer_id)
            
        with self.lock_conexoes:
            chaves_remover = [k for k, v in self.conexoes_ativas.items() if v == sock]
            for k in chaves_remover:
                del self.conexoes_ativas[k]

    def rotina_keep_alive(self, sock):
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
                intervalo = config.get("keepalive_interval", 30)
        except Exception:
            intervalo = 30
            
        while True:
            time.sleep(intervalo)
            try:
                agora_str = datetime.now(timezone.utc).isoformat(timespec='seconds').replace("+00:00", "Z")
                ping_msg = {"type": "PING", "msg_id": str(uuid.uuid4()), "timestamp": agora_str, "ttl": 1}
                self.enviar_mensagem(sock, ping_msg)
            except Exception: break 

    def iniciar_servidor(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        
        while True:
            conn_socket, addr = server_socket.accept()
            threading.Thread(target=self.rotina_de_escuta, args=(conn_socket,), daemon=True).start()
            threading.Thread(target=self.rotina_keep_alive, args=(conn_socket,), daemon=True).start()

    def conectar_com_backoff(self, host_dst, port_dst, max_tentativas=None):
        if max_tentativas is None:
            max_tentativas = self.ler_max_tentativas()

        chave = f"{host_dst}:{port_dst}"
        
        with self.lock_conexoes:
            if chave in self.conexoes_ativas:
                return self.conexoes_ativas[chave]

        tentativa_atual = 0
        tempo_espera = 1

        while tentativa_atual < max_tentativas:
            try:
                cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                cliente.settimeout(3.0)
                cliente.connect((host_dst, port_dst))
                cliente.settimeout(None)
                
                msg_hello = {"type": "HELLO", "peer_id": self.peer_id, "version": "1.0", "features": ["ack", "metrics"], "ttl": 1}
                self.enviar_mensagem(cliente, msg_hello)
                
                threading.Thread(target=self.rotina_de_escuta, args=(cliente,), daemon=True).start()
                threading.Thread(target=self.rotina_keep_alive, args=(cliente,), daemon=True).start()
                
                with self.lock_conexoes:
                    self.conexoes_ativas[chave] = cliente
                return cliente
            
            except socket.error:
                time.sleep(tempo_espera)
                tentativa_atual += 1
                tempo_espera *= 2
                
        return None