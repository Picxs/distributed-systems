#!/usr/bin/env python3
"""
=============================================================================
 calc_server_proto.py — Servidor de Calculadora via TCP + Protocol Buffers
=============================================================================
 Disciplina : Sistemas Distribuídos
 Protocolo  : TCP com serialização Protocol Buffers (binário)
 Porta      : 9002 (padrão)

 POR QUE USAR PROTOBUF AO INVÉS DO PROTOCOLO TEXTUAL?
 ─────────────────────────────────────────────────────────────────────────
   Protocolo Textual (Partes 1 e 2):
     - Fácil de debugar (legível por humanos)
     - Parsing manual com split(':')
     - Maior tamanho de mensagem

   Protocol Buffers (Parte 4):
     - Serialização binária → mensagens compactas
     - Schema fortemente tipado → sem erros de parse de strings
     - Geração automática de código pelo protoc
     - Mais eficiente para aplicações de alta performance

 PROTOCOLO DE FRAMING (DELIMITAÇÃO DE MENSAGENS)
 ─────────────────────────────────────────────────────────────────────────
 Mensagens protobuf são bytes crus sem delimitador natural.
 Precisamos saber quantos bytes ler para uma mensagem completa.
 Solução: LENGTH-PREFIXED FRAMING

   ┌──────────────────────────┬───────────────────────────────────────┐
   │  4 bytes (big-endian)    │  N bytes (payload protobuf)           │
   │  = tamanho N do payload  │  = mensagem serializada               │
   └──────────────────────────┴───────────────────────────────────────┘

 O receptor lê os 4 bytes de tamanho primeiro, depois lê exatamente
 N bytes do payload e desserializa.

 USO
 ─────────────────────────────────────────────────────────────────────────
   python3 calc_server_proto.py
   python3 calc_server_proto.py --host 0.0.0.0 --port 9002
=============================================================================
"""

import argparse
import socket
import struct
import threading

# Importa as classes geradas pelo protoc a partir de calc.proto
import calc_pb2


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 1: Lógica de Cálculo
# ─────────────────────────────────────────────────────────────────────────────

def calcular(op1: float, op: str, op2: float) -> tuple[bool, float | str]:
    """
    Executa a operação aritmética.
    Retorna (True, resultado) ou (False, mensagem_erro).
    """
    try:
        if op == "+":
            return True, op1 + op2
        elif op == "-":
            return True, op1 - op2
        elif op == "*":
            return True, op1 * op2
        elif op == "/":
            if op2 == 0:
                return False, "divisão por zero"
            return True, op1 / op2
        else:
            return False, f"operação inválida: '{op}'"
    except Exception as e:
        return False, str(e)


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 2: Funções de Framing (Length-Prefixed)
# ─────────────────────────────────────────────────────────────────────────────

def recv_mensagem(conn: socket.socket) -> bytes | None:
    """
    Recebe uma mensagem protobuf usando length-prefixed framing.

    Passo 1: Lê 4 bytes → interpreta como inteiro big-endian → tamanho N
    Passo 2: Lê exatamente N bytes → payload serializado

    O loop interno garante que todos os bytes sejam lidos mesmo que
    o TCP entregue os dados em múltiplos segmentos (fragmentation).

    Retorna:
        bytes: payload completo para desserializar com ParseFromString()
        None : se a conexão foi fechada antes de receber dados completos
    """
    # ── Lê os 4 bytes de cabeçalho (tamanho do payload) ──
    header = b""
    while len(header) < 4:
        chunk = conn.recv(4 - len(header))
        if not chunk:
            return None  # Conexão encerrada
        header += chunk

    # struct.unpack(">I", ...) interpreta 4 bytes como unsigned int big-endian
    # ">I" = big-endian (>) + unsigned int 32 bits (I)
    tamanho = struct.unpack(">I", header)[0]

    # ── Lê o payload completo (tamanho bytes) ──
    payload = b""
    while len(payload) < tamanho:
        chunk = conn.recv(tamanho - len(payload))
        if not chunk:
            return None  # Conexão encerrada no meio da mensagem
        payload += chunk

    return payload


def send_mensagem(conn: socket.socket, payload: bytes) -> None:
    """
    Envia uma mensagem protobuf com framing de 4 bytes.

    Concatena: [4 bytes de tamanho] + [payload protobuf]
    e envia tudo de uma vez com sendall() para evitar fragmentação
    desnecessária.
    """
    # struct.pack(">I", N) converte o inteiro N em 4 bytes big-endian
    header = struct.pack(">I", len(payload))
    conn.sendall(header + payload)


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 3: Atendimento de Cliente em Thread
# ─────────────────────────────────────────────────────────────────────────────

def handle_cliente(conn: socket.socket, addr: tuple) -> None:
    """
    Atende um cliente TCP usando Protocol Buffers.

    Fluxo por requisição:
      1. recv_mensagem() → bytes do CalcRequest serializado
      2. ParseFromString() → desserializa para objeto CalcRequest
      3. Executa o cálculo
      4. Monta CalcResponse e serializa com SerializeToString()
      5. send_mensagem() → envia com framing de 4 bytes
    """
    print(f"[CONEXÃO] Cliente conectado: {addr}")
    try:
        while True:
            # Recebe o payload da requisição (bytes crus do protobuf)
            payload = recv_mensagem(conn)
            if payload is None:
                break  # Cliente desconectou

            # ── Desserialização: bytes → objeto Python ──
            # ParseFromString() preenche os campos do objeto CalcRequest
            req = calc_pb2.CalcRequest()
            req.ParseFromString(payload)

            print(f"[RECEBIDO] {addr} → CALC:{req.seq}:{req.op1}:{req.op}:{req.op2}")

            # Realiza o cálculo com os valores já tipados (float, não string)
            ok, resultado = calcular(req.op1, req.op, req.op2)

            # ── Monta a resposta ──
            resp = calc_pb2.CalcResponse()
            resp.seq = req.seq  # Ecoa o número de sequência
            resp.ok  = ok

            if ok:
                resp.result = resultado
                print(f"[RESPOSTA] {addr} ← RESULT:{resp.seq}:{resp.result}")
            else:
                resp.error = resultado
                print(f"[RESPOSTA] {addr} ← ERROR:{resp.seq}:{resp.error}")

            # ── Serialização: objeto Python → bytes ──
            # SerializeToString() converte o objeto para bytes compactos
            send_mensagem(conn, resp.SerializeToString())

    except ConnectionResetError:
        print(f"[AVISO] Conexão com {addr} resetada.")
    except Exception as e:
        print(f"[ERRO] {addr}: {e}")
    finally:
        conn.close()
        print(f"[FECHADO] Conexão com {addr} encerrada.")


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 4: Loop Principal
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Servidor TCP/Protobuf de calculadora")
    parser.add_argument("--host", default="0.0.0.0",
                        help="Endereço de escuta (padrão: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=9002,
                        help="Porta TCP (padrão: 9002)")
    args = parser.parse_args()

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind((args.host, args.port))
    servidor.listen(128)

    print(f"[CalcServerProto] Escutando em {args.host}:{args.port}  (Protocol Buffers)")
    print("Pressione Ctrl+C para encerrar.\n")

    try:
        while True:
            conn, addr = servidor.accept()
            # Thread para atender cliente sem bloquear novas conexões
            t = threading.Thread(target=handle_cliente, args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\n[CalcServerProto] Encerrando.")
    finally:
        servidor.close()


if __name__ == "__main__":
    main()
