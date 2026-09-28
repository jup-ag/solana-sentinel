Richat us-east  ──► wg-node (us-east) ──┐        wg-node (eu-west) ──┐        wg-node (ap-south) ───┐
        │                    │           │              │            │              │               │
        │                    ▼           │              ▼            │              ▼               │
        │            ┌──────────────┐    │      ┌────────────────┐   │      ┌─────────────────┐     │
        │            │ Redis (local │    │      │ Redis (local   │   │      │ Redis (local    │     │
        │            │  sidecar)    │    │      │  sidecar)      │   │      │  sidecar)       │     │
        │            │              │    │      │                │   │      │                 │     │
        │            │ • CooldownGate    │      │ • CooldownGate │   │      │ • CooldownGate  │     │
        │            │ • SlotCheckpoint  │      │ • SlotCheckpoint   │      │ • SlotCheckpoint│     │
        │            └──────────────┘    │      └────────────────┘   │      └─────────────────┘     │
        │                                │                           │                              │
        └────────────────────────────────┴──────────── on detection ────────────────────────────────┘
                                                             │
                                                  1. cooldown check (LOCAL redis)
                                                     SET cd:{key} NX EX <cooldown>
                                                     nil → skip (still cooling down)
                                                             │
                                                  2. hash = f(identity fields)
                                                  3. SET alert:{hash} NX EX 86400  (SHARED redis)
                                                             ▼
                                                     ┌─────────────┐
                                                     │   Redis     │  OK  → fire to sinks
                                                     │ (ElastiCache│  nil → skip (someone
                                                     │  Multi-AZ)  │        already fired)
                                                     └─────────────┘  err → FAIL OPEN: fire
                                                             │
                                                winner fires directly to
                                                             │
                                       ┌─────────────────────┼──────────────────┐
                                       ▼                     ▼                  ▼
                                     Slack             ClickHouse          Betterstack
                                 (notification)       (audit trail)     (critical paging)



CooldownGate: prevents alert storms by enforcing a cooldown period for each unique alert key. It uses a local Redis sidecar to track cooldowns and ensures that only one instance of an alert is fired within the specified cooldown period. 

SlotCheckpoint: ensures that each unique alert is only fired once per day. It uses a shared Redis instance (ElastiCache Multi-AZ) to track which alerts have already been fired, preventing duplicate alerts from being sent to sinks like Slack, ClickHouse, or Betterstack.
