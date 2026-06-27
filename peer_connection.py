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
# Arquivo: peer_connection.py]
# ==============================================================================
import socket
import json
import threading
import time
import uuid
from datetime import datetime, timezone

class PeerNode:
    """Motor de rede P2P: gerencia conexões diretas entre os alunos (Cliente/Servidor)."""
    def __init__(self, host: str, port: int, peer_id: str):
        self.host = host
        self.port = port
        self.peer_id = peer_id
        self.conexoes_ativas = {} 
        self.lock = threading.Lock()

    def enviar_mensagem(self, sock, dicionario_msg):
        """Converte o dicionário Python para string JSON com quebra de linha (\n) e envia via socket."""
        try:
            msg_str = json.dumps(dicionario_msg) + "\n"
            sock.sendall(msg_str.encode('utf-8'))
        except Exception as e:
            pass # Silenciado para não poluir a interface visual

    def processar_mensagem(self, sock, msg):
        """Lida com as regras de protocolo quando uma mensagem chega."""
        tipo = msg.get("type")
        
        # Regra do Handshake: Responde HELLO com HELLO_OK
        if tipo == "HELLO":
            resposta_ok = {
                "type": "HELLO_OK", "peer_id": self.peer_id, "version": "1.0",
                "features": ["ack", "metrics"], "ttl": 1
            }
            self.enviar_mensagem(sock, resposta_ok)
            
        # Regra do Keep-Alive: Responde PING com PONG
        elif tipo == "PING":
            pong_msg = {"type": "PONG", "msg_id": msg.get("msg_id"), "timestamp": msg.get("timestamp"), "ttl": 1}
            self.enviar_mensagem(sock, pong_msg)
            
        # Cálculo de latência ao receber o PONG de volta
        elif tipo == "PONG":
            send_time_str = msg.get("timestamp")
            send_time = datetime.fromisoformat(send_time_str.replace("Z", "+00:00"))
            agora = datetime.now(timezone.utc)
            rtt_ms = (agora - send_time).total_seconds() * 1000
            print(f"\n[REDE] PONG recebido! Latência (RTT): {rtt_ms:.2f} ms\n> ", end="", flush=True) #Tornar o PING/PONG visível

    def rotina_de_escuta(self, sock):
        """Loop contínuo que lê dados do socket e processa pacotes completos."""
        buffer = ""
        while True:
            try:
                parte = sock.recv(1024).decode('utf-8')
                if not parte: break
                buffer += parte
                # Extrai mensagens separadas por \n caso cheguem coladas
                while '\n' in buffer:
                    linha, buffer = buffer.split('\n', 1)
                    if linha.strip():
                        self.processar_mensagem(sock, json.loads(linha))
            except Exception: break
        sock.close()

    def rotina_keep_alive(self, sock):
        """Envia um PING a cada 30 segundos para manter o socket TCP aberto."""
        while True:
            time.sleep(30)
            try:
                agora_str = datetime.now(timezone.utc).isoformat(timespec='seconds').replace("+00:00", "Z")
                ping_msg = {"type": "PING", "msg_id": str(uuid.uuid4()), "timestamp": agora_str, "ttl": 1}
                print(f"\n[REDE] Enviando PING de verificação...\n> ", end="", flush=True) #Tornar o PING/PONG visível
                self.enviar_mensagem(sock, ping_msg)
            except Exception: break 

    def iniciar_servidor(self):
        """Abre a porta local e aguarda conexões de outros computadores."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        
        while True:
            conn_socket, addr = server_socket.accept()
            # Inicia threads independentes para cada nova conexão recebida
            threading.Thread(target=self.rotina_de_escuta, args=(conn_socket,), daemon=True).start()
            threading.Thread(target=self.rotina_keep_alive, args=(conn_socket,), daemon=True).start()

    def conectar_com_backoff(self, host_dst, port_dst, max_tentativas=5):
        """Tenta abrir conexão com política de recuo exponencial para evitar flood na rede."""
        tentativa_atual = 0
        tempo_espera = 1 # Segundos

        while tentativa_atual < max_tentativas:
            try:
                cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                cliente.connect((host_dst, port_dst))
                
                # Realiza o Handshake imediato após conectar
                msg_hello = {"type": "HELLO", "peer_id": self.peer_id, "version": "1.0", "features": ["ack", "metrics"], "ttl": 1}
                self.enviar_mensagem(cliente, msg_hello)
                
                # Deixa o socket "vivo" aguardando respostas
                threading.Thread(target=self.rotina_de_escuta, args=(cliente,), daemon=True).start()
                threading.Thread(target=self.rotina_keep_alive, args=(cliente,), daemon=True).start()
                return cliente
            
            except socket.error:
                # Falhou: Espera um pouco, aumenta o tempo (1s, 2s, 4s...) e tenta de novo
                time.sleep(tempo_espera)
                tentativa_atual += 1
                tempo_espera *= 2
                
        return None # Desiste após estourar o limite de tentativas