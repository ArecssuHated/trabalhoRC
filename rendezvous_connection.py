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
# Arquivo: rendezvous_connection.py]
# ==============================================================================
import socket
import json
import logging

class RendezvousClient:
    """Responsável por registrar o peer e descobrir outros no servidor central do professor."""
    def __init__(self, host="pyp2p.mfcaetano.cc", port=8080):
        self.host = host
        self.port = port

    def _send_command(self, payload: dict) -> dict:
        """Função base para empacotar em JSON e enviar via TCP para o Rendezvous."""
        message = json.dumps(payload) + "\n"
        message_bytes = message.encode('utf-8')
        
        # Validação de segurança exigida pela especificação (máximo 32KB)
        if len(message_bytes) > 32768:
            raise ValueError("A mensagem excede o limite de 32 KB do servidor.")

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5.0) # Timeout evita travar caso o servidor caia
                s.connect((self.host, self.port))
                s.sendall(message_bytes)
                
                # Lê a resposta em pedaços até encontrar a quebra de linha (\n)
                response_data = b""
                while True:
                    chunk = s.recv(4096)
                    if not chunk: break 
                    response_data += chunk
                    if b'\n' in chunk: break 
                
                return json.loads(response_data.decode('utf-8').strip())
        except Exception as e:
            logging.error(f"Erro na comunicação com Rendezvous: {e}")
            return {"status": "ERROR", "message": str(e)}

    def register(self, name: str, namespace: str, port: int, ttl: int = 7200) -> dict:
        """Anuncia nossa presença para o servidor central."""
        payload = {"type": "REGISTER", "namespace": namespace, "name": name, "port": port, "ttl": ttl}
        return self._send_command(payload)

    def discover(self, namespace: str = None) -> list:
        """Busca quem está online. Traz toda a rede se namespace for None."""
        payload = {"type": "DISCOVER"}
        if namespace: payload["namespace"] = namespace
        response = self._send_command(payload)
        return response.get("peers", []) if response.get("status") == "OK" else []

    def unregister(self, name: str, namespace: str, port: int = None) -> dict:
        """Remove nosso registro ao fechar o programa."""
        payload = {"type": "UNREGISTER", "namespace": namespace, "name": name}
        if port is not None: payload["port"] = port
        return self._send_command(payload)