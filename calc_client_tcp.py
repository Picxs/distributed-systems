#!/usr/bin/env python3
"""
=============================================================================
 calc_client_tcp.py — Cliente de Calculadora Remota via TCP
=============================================================================
 Disciplina : Sistemas Distribuídos

 DIFERENÇA CHAVE PARA O CLIENTE UDP
 ─────────────────────────────────────────────────────────────────────────
   • TCP garante entrega → NÃO há retransmissão na camada de aplicação
   • connect() é necessário (estabelece a conexão antes de enviar dados)
   • A mesma conexão é reutilizada para todas as 20 requisições
     (connection-oriented, ao contrário do UDP stateless)
   • makefile() cria um objeto tipo-arquivo sobre o socket → readline()
     facilita a leitura de linhas delimitadas por '\n'

 MEDIÇÃO DE RTT NO TCP
 ─────────────────────────────────────────────────────────────────────────
 O RTT no TCP tende a ser mais baixo nas requisições subsequentes porque:
   1. Não há handshake de conexão (feito apenas uma vez)
   2. O TCP mantém estado de congestionamento e ajusta a janela
   3. Dados ficam no buffer do SO, reduzindo latência percebida

 USO
 ─────────────────────────────────────────────────────────────────────────
   python3 calc_client_tcp.py
   python3 calc_client_tcp.py --host 127.0.0.1 --port 9001 --n 20
=============================================================================
"""

import argparse
import random
import socket
import time


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 1: Geração de Requisições Aleatórias
# ─────────────────────────────────────────────────────────────────────────────

OPERACOES = ["+", "-", "*", "/"]


def gerar_requisicao(seq: int, rng: random.Random) -> str:
    """
    Gera uma requisição aleatória com semente fixa para reprodutibilidade.

    Usa a mesma semente que o cliente UDP (seed=42) para gerar as mesmas
    operações, permitindo comparação justa entre os protocolos.
    """
    op1 = round(rng.uniform(-100, 100), 2)
    op2 = round(rng.uniform(-100, 100), 2)
    op  = rng.choice(OPERACOES)
    return f"CALC:{seq}:{op1}:{op}:{op2}"


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 2: Exibição dos Resultados
# ─────────────────────────────────────────────────────────────────────────────

def imprimir_tabela(resultados: list[dict]) -> None:
    """
    Imprime tabela detalhada e resumo das métricas TCP.

    Comparação esperada com UDP:
      - RTT deve ser mais baixo (sem overhead de retransmissão)
      - Perdidas = 0 sempre (TCP garante entrega)
      - Sem coluna "Tentativas" (sempre 1 no TCP)
    """
    cabecalho = (
        f"{'Seq':>4}  {'Requisição':<30}  {'Resposta':<25}  {'RTT(ms)':>9}"
    )
    separador = "-" * len(cabecalho)

    print("\n" + separador)
    print(cabecalho)
    print(separador)

    rtts = []
    for r in resultados:
        rtt_ms = r["rtt"] * 1000
        rtts.append(rtt_ms)
        print(
            f"{r['seq']:>4}  {r['requisicao']:<30}  {r['resposta']:<25}  {rtt_ms:>9.2f}"
        )

    print(separador)
    print("\n── Resumo ─────────────────────────────────────────────────────")
    print(f"  Requisições enviadas  : {len(resultados)}")
    print(f"  Respostas recebidas   : {len(resultados)}")
    print(f"  Requisições perdidas  : 0  (TCP garante entrega)")
    if rtts:
        print(f"  RTT médio             : {sum(rtts)/len(rtts):.2f} ms")
        print(f"  RTT máximo            : {max(rtts):.2f} ms")
        print(f"  RTT mínimo            : {min(rtts):.2f} ms")
    print(f"  Tempo total           : {sum(rtts):.2f} ms")
    print("───────────────────────────────────────────────────────────────\n")


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 3: Ponto de Entrada Principal
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Cliente TCP de calculadora remota")
    parser.add_argument("--host", default="127.0.0.1",
                        help="Endereço do servidor (padrão: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=9001,
                        help="Porta TCP do servidor (padrão: 9001)")
    parser.add_argument("--n", type=int, default=20,
                        help="Número de requisições (padrão: 20)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Semente aleatória (padrão: 42)")
    args = parser.parse_args()

    rng = random.Random(args.seed)

    print(f"[CalcClientTCP] Conectando a {args.host}:{args.port}")
    print(f"  N={args.n} requisições\n")

    try:
        # ── Criação e Conexão do Socket TCP ──
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # connect() executa o handshake TCP de 3 vias (SYN → SYN-ACK → ACK)
        # Após connect(), a conexão está estabelecida e pronta para dados
        sock.connect((args.host, args.port))

        # makefile() cria um objeto tipo-arquivo sobre o socket
        # Permite usar readline() para ler linhas inteiras facilmente
        # Isso resolve o problema de fragmentação (framing) do TCP
        sock_file = sock.makefile("r", encoding="utf-8")

    except ConnectionRefusedError:
        print(f"[ERRO] Conexão recusada em {args.host}:{args.port}. O servidor está rodando?")
        return

    resultados = []
    try:
        for seq in range(args.n):
            req = gerar_requisicao(seq, rng)
            print(f"[{seq:02d}] Enviando: {req!r}")

            # Marca o tempo antes do envio
            t0 = time.perf_counter()

            # sendall() garante que toda a string (+ '\n') seja enviada
            # O '\n' é o delimitador de mensagem no protocolo textual
            sock.sendall((req + "\n").encode("utf-8"))

            # readline() lê até encontrar '\n' → mensagem completa garantida
            resposta = sock_file.readline().strip()

            # RTT = tempo total de ida (envio) + volta (resposta)
            rtt = time.perf_counter() - t0

            print(f"[{seq:02d}] Resposta: {resposta!r}  (RTT={rtt*1000:.2f}ms)")

            resultados.append({
                "seq": seq,
                "requisicao": req,
                "resposta": resposta,
                "rtt": rtt,
            })

    except KeyboardInterrupt:
        print("\n[CalcClientTCP] Interrompido pelo usuário.")
    except Exception as e:
        print(f"[ERRO] {e}")
    finally:
        # Fecha tanto o wrapper de arquivo quanto o socket
        sock_file.close()
        sock.close()  # Envia FIN → encerra a conexão TCP graciosamente

    imprimir_tabela(resultados)


if __name__ == "__main__":
    main()
