Percebemos que um dos objetivos do projeto não estava sendo cumprido e adicionamos mais alguma slinhas de código no message_router.py
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
