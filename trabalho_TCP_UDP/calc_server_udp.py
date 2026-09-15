#!/usr/bin/env python3
"""
=============================================================================
 calc_server_udp.py — Servidor de Calculadora Remota via UDP
=============================================================================
 Disciplina : Sistemas Distribuídos
 Protocolo  : UDP (User Datagram Protocol) — SOCK_DGRAM
 Porta      : 9000 (padrão)

 CONCEITO UDP
 ─────────────────────────────────────────────────────────────────────────
 UDP é um protocolo de transporte NÃO orientado à conexão:
   • Não há handshake (sem SYN/ACK)
   • Não há garantia de entrega
   • Não há garantia de ordem
   • Menor overhead → menor latência
   • Adequado para: jogos em tempo real, vídeo/áudio streaming, DNS, etc.

 PROTOCOLO DA APLICAÇÃO (Textual)
 ─────────────────────────────────────────────────────────────────────────
   Requisição : CALC:<n>:<op1>:<op>:<op2>
   Resposta OK: RESULT:<n>:<resultado>
   Resposta ERR: ERROR:<n>:<mensagem>

   onde:
     <n>   = número de sequência (inteiro)
     <op1> = primeiro operando (float)
     <op>  = operador: +  -  *  /
     <op2> = segundo operando (float)

 SIMULAÇÃO DE PERDA
 ─────────────────────────────────────────────────────────────────────────
 O servidor pode simular perdas de pacotes via --loss-rate.
 Quando um pacote é "perdido", ele é recebido mas a resposta é descartada,
 simulando o comportamento real de uma rede não confiável.
 Isso força o cliente a implementar retransmissão (Stop-and-Wait).

 USO
 ─────────────────────────────────────────────────────────────────────────
   python3 calc_server_udp.py                    # sem perdas
   python3 calc_server_udp.py --loss-rate 0.1   # 10% de perda simulada
   python3 calc_server_udp.py --loss-rate 0.3   # 30% de perda simulada
=============================================================================
"""

import argparse
import random
import socket
import threading


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 1: Lógica de Cálculo
# ─────────────────────────────────────────────────────────────────────────────

def calcular(operando1: float, op: str, operando2: float) -> tuple[bool, float | str]:
    """
    Executa a operação aritmética solicitada.

    Parâmetros:
        operando1 (float): Primeiro operando.
        op        (str)  : Operador (+, -, *, /).
        operando2 (float): Segundo operando.

    Retorna:
        (True,  resultado_float) em caso de sucesso.
        (False, mensagem_str)    em caso de erro (ex: divisão por zero).
    """
    try:
        if op == "+":
            return True, operando1 + operando2
        elif op == "-":
            return True, operando1 - operando2
        elif op == "*":
            return True, operando1 * operando2
        elif op == "/":
            # Verifica divisão por zero antes de executar
            if operando2 == 0:
                return False, "divisão por zero"
            return True, operando1 / operando2
        else:
            # Operador desconhecido
            return False, f"operação inválida: '{op}'"
    except Exception as e:
        return False, str(e)


def processar_mensagem(dados: str) -> str:
    """
    Analisa (faz o parse) de uma mensagem no protocolo textual da aplicação
    e produz a resposta adequada.

    Formato esperado: 'CALC:<n>:<op1>:<op>:<op2>'

    Parâmetros:
        dados (str): Mensagem recebida do cliente.

    Retorna:
        str: Resposta formatada ('RESULT:...' ou 'ERROR:...').
    """
    try:
        # Divide a mensagem pelos separadores ':'
        partes = dados.strip().split(":")

        # Valida estrutura: deve ter exatamente 5 partes e começar com "CALC"
        if len(partes) != 5 or partes[0] != "CALC":
            return "ERROR:?:formato inválido"

        # Extrai os campos
        _, n, op1_str, op, op2_str = partes

        # Converte operandos de string para float
        operando1 = float(op1_str)
        operando2 = float(op2_str)

        # Realiza o cálculo
        ok, resultado = calcular(operando1, op, operando2)

        # Formata a resposta de acordo com o sucesso/falha
        if ok:
            return f"RESULT:{n}:{resultado}"
        else:
            return f"ERROR:{n}:{resultado}"

    except ValueError:
        # Falha ao converter string para float
        return "ERROR:?:operandos inválidos (não são números)"
    except Exception as e:
        return f"ERROR:?:{e}"


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 2: Handler de Datagrama (executado em thread)
# ─────────────────────────────────────────────────────────────────────────────

def handle_datagrama(sock: socket.socket, dados: bytes, addr: tuple, loss_rate: float):
    """
    Processa um único datagrama UDP recebido.

    Cada datagrama é tratado de forma independente em uma thread própria,
    permitindo que o servidor atenda múltiplos clientes simultaneamente
    sem bloquear o loop principal.

    SIMULAÇÃO DE PERDA:
    Se um número aleatório [0,1) for menor que 'loss_rate', o datagrama
    é descartado (sem resposta), simulando perda de pacote na rede.

    Parâmetros:
        sock      : Socket UDP já vinculado à porta.
        dados     : Bytes recebidos do cliente.
        addr      : Endereço (IP, porta) do remetente.
        loss_rate : Probabilidade de descarte [0.0 = sem perda, 1.0 = perde tudo].
    """
    # Sorteio para simular perda de pacote
    if random.random() < loss_rate:
        print(f"[PERDA SIMULADA] datagrama de {addr} descartado.")
        return  # Não envia resposta → cliente ficará aguardando (timeout)

    # Decodifica os bytes para string UTF-8
    mensagem = dados.decode("utf-8", errors="replace")
    print(f"[RECEBIDO] {addr} → {mensagem.strip()!r}")

    # Processa e obtém a resposta
    resposta = processar_mensagem(mensagem)
    print(f"[RESPOSTA] {addr} ← {resposta!r}")

    # Envia a resposta de volta ao endereço do remetente
    # Em UDP, usamos sendto() passando o endereço explicitamente
    sock.sendto(resposta.encode("utf-8"), addr)


# ─────────────────────────────────────────────────────────────────────────────
# SEÇÃO 3: Loop Principal do Servidor
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # ── Configuração dos argumentos de linha de comando ──
    parser = argparse.ArgumentParser(description="Servidor UDP de calculadora remota")
    parser.add_argument("--host", default="0.0.0.0",
                        help="Endereço de escuta (0.0.0.0 = todas as interfaces)")
    parser.add_argument("--port", type=int, default=9000,
                        help="Porta UDP (padrão: 9000)")
    parser.add_argument("--loss-rate", type=float, default=0.0, metavar="RATE",
                        help="Taxa de perda simulada 0.0–1.0 (padrão: 0.0 = sem perda)")
    args = parser.parse_args()

    # Valida o range da taxa de perda
    if not 0.0 <= args.loss_rate <= 1.0:
        parser.error("--loss-rate deve estar entre 0.0 e 1.0")

    # ── Criação do Socket UDP ──
    # AF_INET    = endereçamento IPv4
    # SOCK_DGRAM = socket de datagrama (UDP)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Vincula o socket ao endereço e porta especificados
    # Após bind(), o socket está pronto para receber datagramas
    sock.bind((args.host, args.port))

    print(f"[CalcServerUDP] Escutando em {args.host}:{args.port}  "
          f"| taxa de perda simulada: {args.loss_rate * 100:.0f}%")
    print("Pressione Ctrl+C para encerrar.\n")

    try:
        # ── Loop principal: aguarda e processa datagramas indefinidamente ──
        while True:
            # recvfrom() bloqueia até receber um datagrama
            # Retorna (dados_bytes, (ip_origem, porta_origem))
            dados, addr = sock.recvfrom(4096)

            # Cada datagrama é processado em uma thread separada.
            # Isso garante que o servidor continue recebendo novos datagramas
            # enquanto processa os anteriores (paralelismo).
            # daemon=True: a thread encerra automaticamente quando o programa principal encerrar.
            t = threading.Thread(
                target=handle_datagrama,
                args=(sock, dados, addr, args.loss_rate),
                daemon=True,
            )
            t.start()

    except KeyboardInterrupt:
        print("\n[CalcServerUDP] Encerrando.")
    finally:
        sock.close()  # Libera o socket ao encerrar


if __name__ == "__main__":
    main()
