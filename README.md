# Atividade: Comunicação com Sockets UDP vs. TCP

> **Disciplina**: Sistemas Distribuídos — UFC  
> **Linguagem**: Python 3.10+  
> **Tópicos**: UDP, TCP, Protocol Buffers, retransmissão, RTT, serialização

---

## Visão Geral

Implementação de uma **calculadora remota** que compara os protocolos de transporte UDP e TCP, com serialização textual e binária (Protocol Buffers).

### Estrutura do Projeto

```
Trabalho_TCP_UDP/
│
├── Parte 1 — UDP Textual
│   ├── calc_server_udp.py     # Servidor UDP com perda simulada
│   └── calc_client_udp.py     # Cliente UDP com Stop-and-Wait
│
├── Parte 2 — TCP Textual
│   ├── calc_server_tcp.py     # Servidor TCP multi-thread
│   └── calc_client_tcp.py     # Cliente TCP com medição de RTT
│
└── Parte 4 — TCP + Protocol Buffers
    ├── calc.proto             # Schema das mensagens
    ├── calc_pb2.py            # Gerado automaticamente pelo protoc
    ├── calc_server_proto.py   # Servidor TCP + Protobuf
    └── calc_client_proto.py   # Cliente TCP + Protobuf + comparação de tamanho
```

---

## Protocolo Textual (Partes 1 e 2)

| Direção | Formato |
|---------|---------|
| Cliente → Servidor | `CALC:<n>:<op1>:<op>:<op2>` |
| Servidor → Cliente (sucesso) | `RESULT:<n>:<resultado>` |
| Servidor → Cliente (erro) | `ERROR:<n>:<mensagem>` |

- `<n>` = número de sequência
- `<op1>`, `<op2>` = operandos (`float`)
- `<op>` = `+` `-` `*` `/`

---

## Pré-requisitos

```bash
pip install protobuf grpcio-tools --break-system-packages
```

> Para regenerar `calc_pb2.py` após editar `calc.proto`:
> ```bash
> python3 -m grpc_tools.protoc -I. --python_out=. calc.proto
> ```

---

## Parte 1 — UDP

### Servidor

```bash
python3 calc_server_udp.py                   # sem perda
python3 calc_server_udp.py --loss-rate 0.1  # 10% de perda simulada
python3 calc_server_udp.py --loss-rate 0.3  # 30% de perda simulada
```

| Parâmetro | Padrão | Descrição |
|-----------|--------|-----------|
| `--host` | `0.0.0.0` | Interface de escuta |
| `--port` | `9000` | Porta UDP |
| `--loss-rate` | `0.0` | Taxa de perda simulada (0.0 a 1.0) |

### Cliente

```bash
python3 calc_client_udp.py
```

| Parâmetro | Padrão | Descrição |
|-----------|--------|-----------|
| `--host` | `127.0.0.1` | Endereço do servidor |
| `--port` | `9000` | Porta UDP |
| `--n` | `20` | Número de requisições |
| `--timeout` | `500` | Timeout por tentativa (ms) |
| `--max-retries` | `5` | Máximo de tentativas por requisição |
| `--seed` | `42` | Semente aleatória |

---

## Parte 2 — TCP

### Servidor

```bash
python3 calc_server_tcp.py
```

### Cliente

```bash
python3 calc_client_tcp.py
```

---

## Parte 4 — Protocol Buffers

### Servidor

```bash
python3 calc_server_proto.py   # porta 9002
```

### Cliente

```bash
python3 calc_client_proto.py
```

O cliente exibe uma tabela comparando o tamanho (bytes) das mensagens no formato **Protobuf** vs. **Textual**.

---

## Experimentos

### Experimento 1 — UDP sem perdas
```bash
# Terminal 1
python3 calc_server_udp.py --loss-rate 0.0

# Terminal 2
python3 calc_client_udp.py
```

### Experimento 2 — UDP com 10% de perda
```bash
python3 calc_server_udp.py --loss-rate 0.1 &
python3 calc_client_udp.py
```

### Experimento 3 — UDP com 30% de perda
```bash
python3 calc_server_udp.py --loss-rate 0.3 &
python3 calc_client_udp.py
```

### Experimento 4 — TCP
```bash
python3 calc_server_tcp.py &
python3 calc_client_tcp.py
```

### Experimento 5 — Protocol Buffers
```bash
python3 calc_server_proto.py &
python3 calc_client_proto.py
```

---

## Tabela de Portas

| Serviço | Protocolo | Porta |
|---------|-----------|-------|
| `calc_server_udp` | UDP | 9000 |
| `calc_server_tcp` | TCP | 9001 |
| `calc_server_proto` | TCP | 9002 |

---

## Conceitos Abordados

| Conceito | Onde |
|----------|------|
| Comunicação UDP (sem conexão, sem garantia) | Parte 1 |
| Retransmissão Stop-and-Wait | `calc_client_udp.py` |
| Simulação de perda de pacotes | `calc_server_udp.py --loss-rate` |
| Medição de RTT | Todos os clientes |
| Comunicação TCP (orientado à conexão) | Parte 2 |
| Framing com delimitador de linha `\n` | Partes 1 e 2 |
| Framing length-prefixed (4 bytes) | Parte 4 |
| Protocol Buffers — serialização binária | Parte 4 |
| Comparação textual vs. binário | `calc_client_proto.py` |
| Servidor multi-thread | `calc_server_tcp.py`, `calc_server_proto.py` |
