#!/usr/bin/env python3
"""
=============================================================================
 calc_client_udp.py — Cliente de Calculadora Remota via UDP
=============================================================================
 Disciplina : Sistemas Distribuídos
 Protocolo  : UDP com mecanismo Stop-and-Wait (timeout + retransmissão)

 PROBLEMA DA CONFIABILIDADE NO UDP
 ─────────────────────────────────────────────────────────────────────────
 Como UDP não garante entrega, o cliente precisa implementar sua própria
 lógica de confiabilidade. Usamos o esquema Stop-and-Wait:

   1. Envia a requisição
   2. Aguarda resposta com timeout (ex: 500 ms)
   3. Se receber resposta → OK, mede RTT
   4. Se timeout expirar  → retransmite a mesma requisição
   5. Após N tentativas sem resposta → considera requisição PERDIDA

 MÉTRICAS COLETADAS
 ─────────────────────────────────────────────────────────────────────────
   • RTT (Round-Trip Time): tempo entre envio e recebimento da resposta
   • Número de retransmissões por requisição
   • Total de requisições perdidas

 USO
 ─────────────────────────────────────────────────────────────────────────
   python3 calc_client_udp.py
   python3 calc_client_udp.py --host 127.0.0.1 --port 9000 --n 20
   python3 calc_client_udp.py --timeout 500 --max-retries 5
=============================================================================
"""

import argparse
import random
import socket
import time


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 1: Geração de Requisições Aleatórias
# ─────────────────────────────────────────────────────────────────────────────

# Operadores suportados pelo servidor
OPERACOES = ["+", "-", "*", "/"]


def gerar_requisicao(seq: int, rng: random.Random) -> str:
    """
    Gera uma requisição aleatória no protocolo textual da aplicação.

    Usa um objeto Random com semente fixa para garantir reprodutibilidade
    dos experimentos (mesmas operações geradas sempre com a mesma semente).

    Parâmetros:
        seq (int)          : Número de sequência da requisição.
        rng (random.Random): Gerador de números aleatórios com semente.

    Retorna:
        str: Mensagem formatada 'CALC:<seq>:<op1>:<op>:<op2>'
    """
    op1 = round(rng.uniform(-100, 100), 2)   # Operando 1: float entre -100 e 100
    op2 = round(rng.uniform(-100, 100), 2)   # Operando 2: float entre -100 e 100
    op  = rng.choice(OPERACOES)              # Operador aleatório
    return f"CALC:{seq}:{op1}:{op}:{op2}"


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 2: Envio com Timeout e Retransmissão (Stop-and-Wait)
# ─────────────────────────────────────────────────────────────────────────────

def enviar_com_retransmissao(
    sock: socket.socket,
    servidor: tuple,
    mensagem: str,
    timeout_ms: int,
    max_retries: int,
) -> tuple[str | None, float, int]:
    """
    Implementa o protocolo Stop-and-Wait sobre UDP.

    Envia uma mensagem e aguarda resposta. Se o timeout expirar, retransmite.
    Repete até receber resposta ou esgotar o número máximo de tentativas.

    FUNCIONAMENTO:
      → Envio
      ← Resposta recebida dentro do timeout  → retorna (resposta, rtt, tentativas)
      ← Timeout expirado                     → retransmite (até max_retries vezes)
      ← Esgotou tentativas sem resposta      → retorna (None, rtt_total, tentativas)

    Parâmetros:
        sock        : Socket UDP já criado.
        servidor    : Tupla (host, porta) do servidor.
        mensagem    : String da requisição a enviar.
        timeout_ms  : Tempo máximo de espera por resposta, em milissegundos.
        max_retries : Número máximo de tentativas (incluindo a primeira).

    Retorna:
        (resposta_str | None, rtt_segundos, tentativas_realizadas)
    """
    # Configura o timeout do socket em segundos
    sock.settimeout(timeout_ms / 1000.0)

    tentativas = 0
    inicio_total = time.perf_counter()  # Marca o início para RTT total (caso perceba tudo)

    while tentativas < max_retries:
        tentativas += 1

        # Registra o tempo imediatamente antes do envio para medir RTT
        t0 = time.perf_counter()
        sock.sendto(mensagem.encode("utf-8"), servidor)

        try:
            # Aguarda resposta; recvfrom() bloqueia até receber ou timeout expirar
            dados, _ = sock.recvfrom(4096)
            rtt = time.perf_counter() - t0  # RTT = tempo de ida + volta

            return dados.decode("utf-8", errors="replace"), rtt, tentativas

        except socket.timeout:
            # Timeout: nenhuma resposta chegou no prazo
            print(f"  [TIMEOUT] tentativa {tentativas}/{max_retries} — retransmitindo...")
            # Loop continua → retransmite

    # Esgotou todas as tentativas sem receber resposta
    rtt_total = time.perf_counter() - inicio_total
    return None, rtt_total, tentativas


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 3: Exibição dos Resultados
# ─────────────────────────────────────────────────────────────────────────────

def imprimir_tabela(resultados: list[dict]) -> None:
    """
    Imprime uma tabela formatada com os resultados de todas as requisições
    e um resumo estatístico ao final.

    Estatísticas exibidas:
      - RTT médio, máximo e mínimo (somente das requisições bem-sucedidas)
      - Total de retransmissões realizadas
      - Número de requisições perdidas (sem resposta após max_retries)
    """
    cabecalho = (
        f"{'Seq':>4}  {'Requisição':<30}  {'Resposta':<25}  "
        f"{'RTT(ms)':>9}  {'Tentativas':>10}  {'Status':<10}"
    )
    separador = "-" * len(cabecalho)

    print("\n" + separador)
    print(cabecalho)
    print(separador)

    rtts = []
    retransmissoes_total = 0
    perdidas = 0

    for r in resultados:
        status = "OK" if r["resposta"] else "PERDIDA"
        rtt_ms = r["rtt"] * 1000
        retrans = r["tentativas"] - 1  # Retransmissões = tentativas extras além da primeira

        print(
            f"{r['seq']:>4}  {r['requisicao']:<30}  {(r['resposta'] or '—'):<25}  "
            f"{rtt_ms:>9.2f}  {r['tentativas']:>10}  {status:<10}"
        )

        if r["resposta"]:
            rtts.append(rtt_ms)
        else:
            perdidas += 1
        retransmissoes_total += retrans

    print(separador)
    print("\n── Resumo ─────────────────────────────────────────────────────")
    print(f"  Requisições enviadas   : {len(resultados)}")
    print(f"  Respostas recebidas    : {len(resultados) - perdidas}")
    print(f"  Requisições perdidas   : {perdidas}")
    print(f"  Total de retransmissões: {retransmissoes_total}")
    if rtts:
        print(f"  RTT médio              : {sum(rtts)/len(rtts):.2f} ms")
        print(f"  RTT máximo             : {max(rtts):.2f} ms")
        print(f"  RTT mínimo             : {min(rtts):.2f} ms")
    print(f"  Tempo total            : {sum(r['rtt'] for r in resultados) * 1000:.2f} ms")
    print("───────────────────────────────────────────────────────────────\n")


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 4: Ponto de Entrada Principal
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # ── Argumentos de linha de comando ──
    parser = argparse.ArgumentParser(description="Cliente UDP de calculadora remota")
    parser.add_argument("--host", default="127.0.0.1",
                        help="Endereço do servidor (padrão: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=9000,
                        help="Porta UDP do servidor (padrão: 9000)")
    parser.add_argument("--n", type=int, default=20,
                        help="Número de requisições a enviar (padrão: 20)")
    parser.add_argument("--timeout", type=int, default=500,
                        help="Timeout de espera por resposta em ms (padrão: 500)")
    parser.add_argument("--max-retries", type=int, default=5,
                        help="Máximo de tentativas por requisição (padrão: 5)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Semente para geração aleatória (padrão: 42)")
    args = parser.parse_args()

    # Gerador de números aleatórios com semente fixa → experimentos reprodutíveis
    rng = random.Random(args.seed)
    servidor = (args.host, args.port)

    print(f"[CalcClientUDP] Conectando a {args.host}:{args.port}")
    print(f"  N={args.n} requisições | timeout={args.timeout}ms | max-retries={args.max_retries}\n")

    # ── Criação do Socket UDP ──
    # Em UDP, o cliente não precisa de connect() — sendto() já especifica o destino
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    resultados = []
    try:
        for seq in range(args.n):
            req = gerar_requisicao(seq, rng)
            print(f"[{seq:02d}] Enviando: {req!r}")

            # Envia com Stop-and-Wait (timeout + retransmissão)
            resposta, rtt, tentativas = enviar_com_retransmissao(
                sock, servidor, req, args.timeout, args.max_retries
            )

            if resposta:
                print(f"[{seq:02d}] Resposta: {resposta.strip()!r}  "
                      f"(RTT={rtt*1000:.2f}ms, tentativas={tentativas})")
            else:
                print(f"[{seq:02d}] PERDIDA após {tentativas} tentativas.")

            # Armazena resultado para a tabela final
            resultados.append({
                "seq": seq,
                "requisicao": req,
                "resposta": resposta.strip() if resposta else None,
                "rtt": rtt,
                "tentativas": tentativas,
            })

    except KeyboardInterrupt:
        print("\n[CalcClientUDP] Interrompido pelo usuário.")
    finally:
        sock.close()

    # Exibe tabela com todos os resultados e resumo estatístico
    imprimir_tabela(resultados)


if __name__ == "__main__":
    main()
