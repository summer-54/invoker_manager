# Invoker Manager — Technical Documentation

---

## Table of Contents

- [Environment Variables](#environment-variables)
- [API Protocol](#api-protocol)
    - [Master Stream](#master-stream)
        - [Incoming Messages](#incoming-messages)
        - [Outgoing Messages](#outgoing-messages)
- [Verdicts Reference](#verdicts-reference)

---

## Environment Variables

| Variable                      | Type         | Example                | Description               |
| ----------------------------- | ------------ | ---------------------- | ------------------------- |
| `INVOKER_GATE_SOCKET_ADDRESS` | `SocketAddr` | `127.0.0.1:5477`       | Address gate for invokers |
| `SYSTEM_SOCKET_ADDRESS`       | `SocketAddr` | `127.0.0.2:7754`       | Testing system address    |
| `AUTH_API_URL`                | `Url`        | `http://inf54.ru/api/` | URL to testing system API |

---

## API Protocol

Connect via WebSocket client to:

```
ws://$SYSTEM_SOCKET_ADDRESS
```

**Message Format:**

```
<stream name>
<message>
```

---

### Master Stream

#### Incoming Messages

##### Start Task

```plaintext
master
TYPE JUDGE
ID <uuid>
COUNT <tests count>
LANG G++/PYTHON3
DATA
<data: submission>
```

#### Outgoing Messages

##### Test Verdict

```plaintext
master
TYPE TEST
ID <id>
TEST <test id>
VERDICT <verdict>
DATA
<tar: (output, message)>
```

##### Full Verdict — Success

```plaintext
master
TYPE VERDICT
ID <id>
NAME OK
SUM <uint: score>
GROUPS <uint: score group 0> <uint: score group 1> ... <uint: score group n>
```

##### Full Verdict — Compile Error

```plaintext
master
TYPE VERDICT
NAME CE
MESSAGE <text: message>
```

##### Full Verdict — Testing Error

```plaintext
master
TYPE VERDICT
NAME TE
MESSAGE <text: message>
```

---

## Verdicts Reference

| Name | Description           | Success |
| ---- | --------------------- | ------- |
| `OK` | Accepted              | ✅ Yes  |
| `WA` | Wrong Answer          | ❌ No   |
| `TL` | Time Limit Exceeded   | ❌ No   |
| `ML` | Memory Limit Exceeded | ❌ No   |
| `SL` | Stack Limit Exceeded  | ❌ No   |
| `RE` | Runtime Error         | ❌ No   |
| `CE` | Compile Error         | ❌ No   |
| `TE` | Testing System Error  | ❌ No   |
| `SK` | Skipped               | ❌ No   |

---

_Documentation version: 1.0_
