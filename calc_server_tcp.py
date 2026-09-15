#!/usr/bin/env python3
"""
=============================================================================
 calc_server_tcp.py — Servidor de Calculadora Remota via TCP
=============================================================================
 Disciplina : Sistemas Distribuídos
 Protocolo  : TCP (Transmission Control Protocol) — SOCK_STREAM
 Porta      : 9001 (padrão)

 CONCEITO TCP vs UDP
 ─────────────────────────────────────────────────────────────────────────
 TCP é um protocolo ORIENTADO À CONEXÃO que garante:
   ✔ Entrega confiável (sem perda de segmentos)
   ✔ Ordem dos dados mantida
   ✔ Controle de fluxo e congestionamento
   ✔ Detecção e retransmissão automática de pacotes perdidos
   → Maior overhead, mas não requer retransmissão na camada de aplicação

 O "handshake" TCP (SYN → SYN-ACK → ACK) é transparente à aplicação.

 MODELO DE CONEXÃO
 ─────────────────────────────────────────────────────────────────────────
   1. Servidor faz bind() + listen() → aguarda conexões
   2. Cliente faz connect() → cria conexão TCP
   3. Servidor faz accept() → retorna socket exclusivo para aquele cliente
   4. Ambos trocam dados via send/recv sobre o socket da conexão
   5. Ao final, close() encerra a conexão (FIN/ACK)

 CONCORRÊNCIA: MULTI-THREAD
 ─────────────────────────────────────────────────────────────────────────
 O servidor aceita múltiplos clientes simultâneos. Para cada nova conexão
 aceita via accept(), é criada uma thread independente que atende aquele
 cliente até a desconexão.
 O loop principal continua fazendo accept() para novas conexões.

 FRAMING (DELIMITAÇÃO DE MENSAGENS)
 ─────────────────────────────────────────────────────────────────────────
 TCP é um fluxo de bytes (stream), não de mensagens. Para saber onde uma
 mensagem termina e outra começa, usamos newline '\n' como delimitador.
 O servidor acumula bytes num buffer até encontrar '\n', então processa
 a linha completa.

 USO
 ─────────────────────────────────────────────────────────────────────────
   python3 calc_server_tcp.py
   python3 calc_server_tcp.py --host 0.0.0.0 --port 9001
=============================================================================
"""

import argparse
import socket
import threading


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 1: Lógica de Cálculo
# ─────────────────────────────────────────────────────────────────────────────

def calcular(operando1: float, op: str, operando2: float) -> tuple[bool, float | str]:
    """
    Executa a operação aritmética solicitada.

    Retorna (True, resultado) em caso de sucesso ou (False, mensagem_erro).
    """
    try:
        if op == "+":
            return True, operando1 + operando2
        elif op == "-":
            return True, operando1 - operando2
        elif op == "*":
            return True, operando1 * operando2
        elif op == "/":
            if operando2 == 0:
                return False, "divisão por zero"
            return True, operando1 / operando2
        else:
            return False, f"operação inválida: '{op}'"
    except Exception as e:
        return False, str(e)


def processar_mensagem(dados: str) -> str:
    """
    Faz o parse de uma linha no protocolo textual e retorna a resposta.

    Formato esperado: 'CALC:<n>:<op1>:<op>:<op2>'
    Retorna: 'RESULT:<n>:<resultado>' ou 'ERROR:<n>:<mensagem>'
    """
    try:
        partes = dados.strip().split(":")

        # Valida a estrutura da mensagem
        if len(partes) != 5 or partes[0] != "CALC":
            return "ERROR:?:formato inválido"

        _, n, op1_str, op, op2_str = partes
        operando1 = float(op1_str)
        operando2 = float(op2_str)

        ok, resultado = calcular(operando1, op, operando2)
        return f"RESULT:{n}:{resultado}" if ok else f"ERROR:{n}:{resultado}"

    except ValueError:
        return "ERROR:?:operandos inválidos (não são números)"
    except Exception as e:
        return f"ERROR:?:{e}"


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 2: Atendimento de Cliente em Thread
# ─────────────────────────────────────────────────────────────────────────────

def handle_cliente(conn: socket.socket, addr: tuple) -> None:
    """
    Atende um cliente TCP conectado até que ele se desconecte.

    FRAMING COM NEWLINE:
    TCP é um protocolo de fluxo — os dados chegam como bytes contínuos,
    sem fronteiras de mensagem garantidas. Por isso, usamos um buffer
    local e procuramos pelo delimitador '\n' para extrair mensagens completas.

    Exemplo: se o cliente enviar "CALC:1:10:+:5\n" em 2 pacotes:
      → Pacote 1: b"CALC:1:10"
      → Pacote 2: b":+:5\n"
    O buffer acumula até ter '\n' e aí processa a mensagem completa.

    Parâmetros:
        conn (socket): Socket da conexão com o cliente específico.
        addr (tuple) : Endereço (IP, porta) do cliente.
    """
    print(f"[CONEXÃO] Cliente conectado: {addr}")
    try:
        buffer = ""  # Buffer acumula bytes até termos uma linha completa

        while True:
            # recv() retorna até 4096 bytes disponíveis no momento
            # Retorna b"" (bytes vazio) se o cliente fechou a conexão
            chunk = conn.recv(4096)
            if not chunk:
                break  # Cliente desconectou normalmente

            # Adiciona os novos bytes ao buffer (decodificados como string)
            buffer += chunk.decode("utf-8", errors="replace")

            # Processa todas as linhas completas presentes no buffer
            # Uma linha completa contém '\n'
            while "\n" in buffer:
                # Extrai a primeira linha completa e mantém o restante no buffer
                linha, buffer = buffer.split("\n", 1)
                linha = linha.strip()
                if not linha:
                    continue  # Ignora linhas em branco

                print(f"[RECEBIDO] {addr} → {linha!r}")

                # Processa e envia resposta terminada com '\n' (delimitador)
                resposta = processar_mensagem(linha) + "\n"
                print(f"[RESPOSTA] {addr} ← {resposta.strip()!r}")

                # sendall() garante que todos os bytes serão enviados
                # (diferente de send(), que pode enviar parcialmente)
                conn.sendall(resposta.encode("utf-8"))

    except ConnectionResetError:
        print(f"[AVISO] Conexão com {addr} resetada pelo cliente.")
    except Exception as e:
        print(f"[ERRO] Exceção ao atender {addr}: {e}")
    finally:
        conn.close()  # Encerra a conexão com este cliente
        print(f"[FECHADO] Conexão com {addr} encerrada.")


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 3: Loop Principal do Servidor
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Servidor TCP de calculadora remota")
    parser.add_argument("--host", default="0.0.0.0",
                        help="Endereço de escuta (padrão: 0.0.0.0 = todas as interfaces)")
    parser.add_argument("--port", type=int, default=9001,
                        help="Porta TCP (padrão: 9001)")
    args = parser.parse_args()

    # ── Criação e Configuração do Socket TCP ──
    # AF_INET    = endereçamento IPv4
    # SOCK_STREAM = socket de fluxo (TCP)
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR: permite reusar o endereço imediatamente após fechar o servidor
    # Sem isso, o SO pode manter a porta em TIME_WAIT por alguns segundos
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # bind(): associa o socket ao endereço e porta
    servidor.bind((args.host, args.port))

    # listen(128): coloca o socket em modo de escuta, aceitando até 128 conexões
    # pendentes na fila (backlog) antes de começar a recusar novas
    servidor.listen(128)

    print(f"[CalcServerTCP] Escutando em {args.host}:{args.port}")
    print("Pressione Ctrl+C para encerrar.\n")

    try:
        while True:
            # accept() bloqueia até que um cliente complete o handshake TCP
            # Retorna (socket_do_cliente, endereço_do_cliente)
            # O socket retornado é EXCLUSIVO para esta conexão
            conn, addr = servidor.accept()

            # Cria uma thread para atender este cliente sem bloquear o loop
            t = threading.Thread(target=handle_cliente, args=(conn, addr), daemon=True)
            t.start()

    except KeyboardInterrupt:
        print("\n[CalcServerTCP] Encerrando.")
    finally:
        servidor.close()


if __name__ == "__main__":
    main()
