#!/usr/bin/env python3
"""
=============================================================================
 calc_client_proto.py — Cliente de Calculadora via TCP + Protocol Buffers
=============================================================================
 Disciplina : Sistemas Distribuídos

 COMPARAÇÃO DE TAMANHO: PROTOBUF vs TEXTUAL
 ─────────────────────────────────────────────────────────────────────────
 Este cliente coleta e compara o tamanho (em bytes) de cada requisição
 nos dois formatos:

   Textual : "CALC:0:27.89:*:-95.0\n"  → tamanho variável com os valores
   Protobuf: bytes binários compactos   → tamanho fixo por tipo de dado

 Resultado esperado:
   - Para operandos pequenos: protobuf pode ser MAIOR (overhead de tags)
   - Para operandos grandes/muitos campos: protobuf tende a ser menor
   - Vantagem real do protobuf está em parsing speed e type safety

 USO
 ─────────────────────────────────────────────────────────────────────────
   python3 calc_client_proto.py
   python3 calc_client_proto.py --host 127.0.0.1 --port 9002 --n 20
=============================================================================
"""

import argparse
import random
import socket
import struct
import time

# Importa as classes geradas pelo compilador protoc
import calc_pb2


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 1: Geração de Dados Aleatórios
# ─────────────────────────────────────────────────────────────────────────────

OPERACOES = ["+", "-", "*", "/"]


def gerar_dados(seq: int, rng: random.Random) -> tuple[int, float, str, float]:
    """
    Gera os valores de uma operação aleatória.

    Usa a mesma semente que os outros clientes (seed=42) para comparação
    justa entre UDP textual, TCP textual e TCP protobuf.

    Retorna:
        (seq, op1, op, op2)
    """
    op1 = round(rng.uniform(-100, 100), 2)
    op2 = round(rng.uniform(-100, 100), 2)
    op  = rng.choice(OPERACOES)
    return seq, op1, op, op2


def tamanho_textual(seq: int, op1: float, op: str, op2: float) -> int:
    """
    Calcula o tamanho em bytes da mensagem equivalente no protocolo ASCII.

    Usado para comparação: mostra o que seria enviado se usássemos
    o protocolo textual das Partes 1 e 2.
    """
    msg = f"CALC:{seq}:{op1}:{op}:{op2}\n"
    return len(msg.encode("utf-8"))


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 2: Funções de Framing (idênticas ao servidor)
# ─────────────────────────────────────────────────────────────────────────────

def recv_mensagem(conn: socket.socket) -> bytes | None:
    """
    Recebe uma mensagem com length-prefixed framing (4 bytes de tamanho + payload).
    Lê em loop para lidar com fragmentação do TCP.
    """
    header = b""
    while len(header) < 4:
        chunk = conn.recv(4 - len(header))
        if not chunk:
            return None
        header += chunk

    # Interpreta os 4 bytes como unsigned int big-endian
    tamanho = struct.unpack(">I", header)[0]

    payload = b""
    while len(payload) < tamanho:
        chunk = conn.recv(tamanho - len(payload))
        if not chunk:
            return None
        payload += chunk

    return payload


def send_mensagem(conn: socket.socket, payload: bytes) -> None:
    """Envia payload com cabeçalho de 4 bytes indicando o tamanho."""
    header = struct.pack(">I", len(payload))
    conn.sendall(header + payload)


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 3: Tabela de Resultados com Comparação de Tamanho
# ─────────────────────────────────────────────────────────────────────────────

def imprimir_tabela(resultados: list[dict]) -> None:
    """
    Imprime:
      1. Tabela detalhada: seq, operandos, resultado, RTT, bytes PB, bytes TXT
      2. Resumo estatístico de RTT
      3. Comparação de tamanho de mensagem: Protobuf vs Textual
    """
    cab = (
        f"{'Seq':>4}  {'op1':>8}  {'op':>2}  {'op2':>8}  "
        f"{'Resultado':<20}  {'RTT(ms)':>9}  {'Bytes PB':>9}  {'Bytes TXT':>10}"
    )
    sep = "-" * len(cab)

    print("\n" + sep)
    print(cab)
    print(sep)

    rtts = []
    bytes_pb_list = []
    bytes_txt_list = []

    for r in resultados:
        rtt_ms = r["rtt"] * 1000
        rtts.append(rtt_ms)
        bytes_pb_list.append(r["bytes_req_pb"])
        bytes_txt_list.append(r["bytes_req_txt"])

        # Formata o resultado dependendo de sucesso/erro
        resultado_str = (
            f"RESULT:{r['resp'].result:.4f}" if r["resp"].ok
            else f"ERROR:{r['resp'].error}"
        )
        print(
            f"{r['seq']:>4}  {r['op1']:>8.2f}  {r['op']:>2}  {r['op2']:>8.2f}  "
            f"{resultado_str:<20}  {rtt_ms:>9.2f}  {r['bytes_req_pb']:>9}  {r['bytes_req_txt']:>10}"
        )

    print(sep)

    # ── Resumo de RTT ──
    print("\n── Resumo ─────────────────────────────────────────────────────────────────")
    print(f"  Requisições enviadas          : {len(resultados)}")
    if rtts:
        print(f"  RTT médio                     : {sum(rtts)/len(rtts):.2f} ms")
        print(f"  RTT máximo                    : {max(rtts):.2f} ms")
        print(f"  RTT mínimo                    : {min(rtts):.2f} ms")

    # ── Comparação de tamanho ──
    print("\n── Comparação de Tamanho de Mensagem (Requisição) ─────────────────────────")
    media_pb  = sum(bytes_pb_list) / len(bytes_pb_list)
    media_txt = sum(bytes_txt_list) / len(bytes_txt_list)
    razao = media_pb / media_txt

    print(f"  Protobuf — média              : {media_pb:.1f} bytes")
    print(f"  Textual  — média              : {media_txt:.1f} bytes")
    print(f"  Razão protobuf/textual        : {razao:.2%}  "
          f"({'menor = mais eficiente' if razao < 1 else 'maior = menos eficiente para esta carga'})")
    print("───────────────────────────────────────────────────────────────────────────\n")


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 4: Ponto de Entrada Principal
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Cliente TCP/Protobuf de calculadora")
    parser.add_argument("--host", default="127.0.0.1",
                        help="Endereço do servidor (padrão: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=9002,
                        help="Porta TCP (padrão: 9002)")
    parser.add_argument("--n", type=int, default=20,
                        help="Número de requisições (padrão: 20)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Semente aleatória (padrão: 42)")
    args = parser.parse_args()

    rng = random.Random(args.seed)

    print(f"[CalcClientProto] Conectando a {args.host}:{args.port}  (Protocol Buffers)")
    print(f"  N={args.n} requisições\n")

    try:
        # Conexão TCP padrão
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.connect((args.host, args.port))
    except ConnectionRefusedError:
        print(f"[ERRO] Conexão recusada em {args.host}:{args.port}. O servidor está rodando?")
        return

    resultados = []
    try:
        for idx in range(args.n):
            seq, op1, op, op2 = gerar_dados(idx, rng)

            # ── Serialização da requisição ──
            req = calc_pb2.CalcRequest()  # Cria objeto CalcRequest (gerado pelo protoc)
            req.seq = seq                 # Preenche campos com os valores gerados
            req.op1 = op1
            req.op  = op
            req.op2 = op2

            # Converte o objeto para bytes binários
            payload = req.SerializeToString()

            # Tamanho total enviado: 4 bytes framing + N bytes payload
            bytes_req_pb  = 4 + len(payload)
            bytes_req_txt = tamanho_textual(seq, op1, op, op2)

            print(f"[{seq:02d}] Enviando: CALC:{seq}:{op1}:{op}:{op2}  "
                  f"({bytes_req_pb}B proto / {bytes_req_txt}B txt)")

            # Mede RTT: do envio até receber resposta completa
            t0 = time.perf_counter()
            send_mensagem(conn, payload)
            resp_payload = recv_mensagem(conn)
            rtt = time.perf_counter() - t0

            # ── Desserialização da resposta ──
            resp = calc_pb2.CalcResponse()
            resp.ParseFromString(resp_payload)

            if resp.ok:
                print(f"[{seq:02d}] Resposta: RESULT:{resp.seq}:{resp.result:.4f}  "
                      f"(RTT={rtt*1000:.2f}ms)")
            else:
                print(f"[{seq:02d}] Resposta: ERROR:{resp.seq}:{resp.error}  "
                      f"(RTT={rtt*1000:.2f}ms)")

            resultados.append({
                "seq": seq,
                "op1": op1,
                "op": op,
                "op2": op2,
                "resp": resp,
                "rtt": rtt,
                "bytes_req_pb":  bytes_req_pb,
                "bytes_req_txt": bytes_req_txt,
            })

    except KeyboardInterrupt:
        print("\n[CalcClientProto] Interrompido.")
    except Exception as e:
        print(f"[ERRO] {e}")
    finally:
        conn.close()

    imprimir_tabela(resultados)


if __name__ == "__main__":
    main()
