# ==============================================================================
# Disciplina: Redes de Computadores
# Projeto: Chat P2P com Servidor Rendezvous
# Grupo: 5
# Namespace: CIC
#
# Arquivo: rendezvous_connection.py
# ==============================================================================
import socket
import json
import logging

class RendezvousClient:
    def __init__(self, host="pyp2p.mfcaetano.cc", port=8080):
        self.host = host
        self.port = port

    def _send_command(self, payload: dict) -> dict:
        message = json.dumps(payload) + "\n"
        message_bytes = message.encode('utf-8')
        
        if len(message_bytes) > 32768:
            raise ValueError("O payload ultrapassa o teto de 32 KB.")

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5.0) 
                s.connect((self.host, self.port))
                s.sendall(message_bytes)
                
                response_data = b""
                while True:
                    chunk = s.recv(4096)
                    if not chunk: break 
                    response_data += chunk
                    if b'\n' in chunk: break 
                
                return json.loads(response_data.decode('utf-8').strip())
        except Exception as e:
            logging.error(f"Falha ao contatar a coordenacao central: {e}")
            return {"status": "ERROR", "message": str(e)}

    def register(self, name: str, namespace: str, port: int, ttl: int = 7200) -> dict:
        payload = {"type": "REGISTER", "namespace": namespace, "name": name, "port": port, "ttl": ttl}
        return self._send_command(payload)

    def discover(self, namespace: str = None) -> list:
        payload = {"type": "DISCOVER"}
        if namespace: payload["namespace"] = namespace
        response = self._send_command(payload)
        return response.get("peers", []) if response.get("status") == "OK" else []

    def unregister(self, name: str, namespace: str, port: int = None) -> dict:
        payload = {"type": "UNREGISTER", "namespace": namespace, "name": name}
        if port is not None: payload["port"] = port
        return self._send_command(payload)