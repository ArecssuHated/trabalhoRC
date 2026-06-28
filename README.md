Percebemos que um dos objetivos do projeto não estava sendo cumprido e adicionamos mais alguma slinhas de código no message_router.pye em peer_connection.py
 message_router.pye
# --- BLOCO DE SAÍDA CORRIGIDO (BYE) ---
        if action == "QUIT":
            queue_ui.put({"origin": "SYSTEM", "content": "Enviando pacotes de BYE e encerrando sessoes..."})
            peers_ativos = tabela.get_all_peers()
            
            pacote_bye = {
                "type": "BYE", "msg_id": str(uuid.uuid4()), "src": meu_peer_id
            }
            
            # Avisa a todos os conhecidos que estamos a sair
            for p in peers_ativos:
                sock = motor_p2p.conectar_com_backoff(p['ip'], p['port'], max_tentativas=1)
                if sock:
                    motor_p2p.enviar_mensagem(sock, pacote_bye)
                    
            time.sleep(1) # Aguarda 1 segundo para o TCP terminar de despachar os pacotes fisicos
            sys.exit(0)
            
        # ... (O codigo de SEND e PUB continua igual aqui no meio) ...

        # ---(RECONNECT) ---
        elif action == "RECONNECT":
            queue_ui.put({"origin": "SYSTEM", "content": "Forcando reconexao com os pares (Handshake)..."})
            peers_ativos = tabela.get_all_peers()
            conexoes_restabelecidas = 0
            
            for p in peers_ativos:
                # O proprio metodo conectar_com_backoff ja envia o HELLO
                sock = motor_p2p.conectar_com_backoff(p['ip'], p['port'], max_tentativas=2)
                if sock:
                    conexoes_restabelecidas += 1
            
            queue_ui.put({"origin": "SYSTEM", "content": f"Ciclo de reconexao finalizado. {conexoes_restabelecidas} pares alcancados."})

peer_connection.py
# Extracao de delta de tempo para inferencia de RTT
        elif tipo == "PONG":
            send_time_str = msg.get("timestamp")
            send_time = datetime.fromisoformat(send_time_str.replace("Z", "+00:00"))
            agora = datetime.now(timezone.utc)
            rtt_ms = (agora - send_time).total_seconds() * 1000
            print(f"\n[SISTEMA_REDE] PONG recebido com latencia (RTT) aferida: {rtt_ms:.2f} ms\n> ", end="", flush=True)

        # --- NOVA REGRA: DESPEDIDA P2P ---
        elif tipo == "BYE":
            # Devolve o BYE_OK confirmando que entendeu a saida 
            resposta_bye_ok = {
                "type": "BYE_OK", "msg_id": msg.get("msg_id")
            }
            self.enviar_mensagem(sock, resposta_bye_ok)
            
            # Imprime na tela quem saiu da rede
            remetente = msg.get("src", "Um peer")
            print(f"\n[SISTEMA_REDE] {remetente} encerrou a conexao (BYE recebido).\n> ", end="", flush=True)
            
        elif tipo == "BYE_OK":
             print(f"\n[SISTEMA_REDE] BYE_OK recebido. Conexao finalizada com sucesso.\n> ", end="", flush=True)
