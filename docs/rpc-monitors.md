# Poll-fed rules and stream sources

Most rules are fed by the account / transaction stream. Some state cannot be observed as a
stream event (wallet balances, oracle staleness, token supply vs reserves), so those rules are
**poll-fed**: a loop fetches `getMultipleAccounts` on a cadence and runs the rules over the
result. This doc covers the poll feed and the pluggable stream sources.

## Poll feed

```
spec().poll(source, default_interval, PollRules::new().register::<R>())
        │
        └─ validate: R::accounts(cfg) ∪ …, R::poll_interval(cfg) → buckets by cadence
        └─ bootstrap: R::build(cfg, deps), R::bootstrap(ctx)
        └─ install: one PollLoop per bucket
               tick ─▶ getMultipleAccounts(bucket.accounts) ─▶ source.parse(fetched, ctx) ─▶ rule.evaluate(item, ctx)
```

- A rule implements `RpcRule`: `accounts(cfg)` is what gets fetched, `poll_interval(cfg)` is
  optional (`None` = program default), `evaluate(item, &PollCtx)` gets the response slot.
- The program implements `PollSource`: `parse` turns one fetch into items, per account (pyth)
  or one cross-account snapshot (a collateralization ratio, say). Parse failures are logged, not fatal.
- Same-cadence rules share one fetch. A rule with no accounts (the `system` slot-lag rule)
  still gets a `PollCtx` whose slot comes from `getSlot`.
- `cooldown_period_seconds` should exceed the poll interval, or a persistent condition fires
  every tick.

Bundled programs with poll feeds: `pyth` (staleness) and `system` (slot lag).

## Stream sources

The stream feed is transport-agnostic. `[geyser_source].type` selects:

- `yellowstone` — shipstern runtime (default). Shipstern owns buffering, reconnect and
  dispatch; the framework installs `FilterPipeline`s into it and keeps the `RuntimeHandle`
  for live filter updates.
- `grpc` — `ingest::GrpcSource`, a plain `yellowstone-grpc-client` subscription. Updates
  carry the server's matched filter names; the framework `Dispatcher` routes them.
- `websocket` — `ingest::WebsocketSource`, Solana PubSub. Accounts via
  `accountSubscribe` / `programSubscribe` (memcmp / datasize honoured), transactions via
  `logsSubscribe(mentions)` + `getTransaction`, slots via `slotSubscribe`. Filter matching
  is done locally against the fetched transaction's keys. Needs `confirmed` or `finalized`.

A custom transport implements `solana_sentinel::StreamSource`.
