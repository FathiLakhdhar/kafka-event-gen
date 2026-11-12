# kafka-proto-gen

Generates fake events from a JSON plan and publishes them to Kafka, or prints them for a dry run. Payloads can be binary Protobuf, Protobuf encoded as JSON, or schemaless JSON.

- Fills every field with [Faker](https://fakerjs.dev) data, unless a rule says otherwise.
- Builds **datasets** of reference data at startup (users, customers, contracts…), which can be derived from and linked to each other like database tables.
- Links related events through **pools**. For example, `OrderPaid` reuses the `orderId`, amount and `traceparent` of an earlier `OrderCreated`.
- Picks event types by weight, or produces an exact count of each.
- Sends at a fixed rate in batches grouped by topic, with Kafka keys and headers.
- Reads schemas from `.proto` source files or compiled descriptor sets (`.protobin`).
- Checks the plan against the schemas and payload definitions at startup, so typos fail before anything is sent.

## Requirements

- Node.js **22.12 or later** (needed to `require()` Faker v10, which is ESM-only)
- A Kafka broker, unless you only do dry runs

## Install

```sh
npm install
```

## Usage

```sh
npm start                        # runs plans/plan.json
node plan.js path/to/plan.json   # runs any plan
```

With `"dryRun": true`, each message is printed to stdout as one JSON line, after encoding and decoding it, so you see exactly what would go on the wire. Progress and dataset sizes go to stderr, which keeps the output clean:

```sh
npm start > out.jsonl
```

Press Ctrl+C once to stop after the current batch, or twice to force quit.

## Project layout

```
plan.js                   the generator
plans/plan.json           simple order lifecycle, mixed by weight
plans/plan-quotas.json    the same lifecycle with exact counts per event
plans/plan-datasets.json  full example: users → customers → contracts, orders, payments, refunds
plans/plan-small.json     small example (22 messages) of a full order lifecycle, easy to check by eye
plans/plan-json.json      mixes JSON from a proto, binary Protobuf and a schemaless JSON event
plans/order.proto         schema used by the example plans
```

## Concepts

A plan has two kinds of named record collections. Events read both the same way.

| | Datasets | Pools |
|---|---|---|
| What | Reference data: users, customers, contracts… | Event state: created orders, payments made… |
| Filled | Once, at startup, before any event | During the run, by events |
| Built from | Rules, or other datasets | Values from generated messages |
| Database analogy | Tables loaded before the test | Rows the app inserts while it runs |

Both can be **split** into sub-pools that don't overlap, for example `customers/vip` and `customers/regular`, or `order/toPay` and `order/toCancel`.

## Plan reference

### Top-level settings

| Key          | Description |
|--------------|-------------|
| `brokers`    | Kafka bootstrap servers, e.g. `["localhost:9092"]` |
| `clientId`   | Kafka client id (default `kafka-proto-gen`) |
| `count`      | Total number of messages to generate. Required with weights; optional with quotas, where it must equal the sum of event counts |
| `ratePerSec` | Messages per one-second batch (default: all at once) |
| `dryRun`     | `true` prints messages instead of sending them (no pause between batches) |
| `outDir`     | If set, also writes each encoded message to `NNNN-<Type>.protobin` |
| `typeHeader` | If set, adds a header with this name holding the full message type |
| `clock`      | Fixed "now" for every date, e.g. `"2026-01-01T00:00:00Z"`, so dates are the same on every run (see [Dates and times](#dates-and-times)) |
| `datasets`   | Reference data built at startup (see [Datasets](#datasets)) |
| `pools`      | Pools that events fill and read (see [Pools](#pools)) |
| `defaults`   | Settings shared by every event (see below) |
| `events`     | List of event definitions |

Paths in `proto`, `descriptor` and `outDir` are relative to the **plan file**, not the folder you run from.

### Events and defaults

Each event is merged on top of `defaults`. `rules` and `headers` are merged key by key, and setting a key to `null` removes the default for that event.

| Key       | Description |
|-----------|-------------|
| `type`    | Fully qualified message type, e.g. `shop.OrderCreated` (required) |
| `proto`   | `.proto` file containing the type (this, `descriptor` or `payload` is required) |
| `descriptor` | Compiled `FileDescriptorSet` containing the type, in place of `proto` (see [Descriptor sets](#descriptor-sets)) |
| `format`  | `"protobuf"` (default) or `"json"` (see [Payload formats](#payload-formats)) |
| `payload` | Field path → rule, for a schemaless JSON event without a proto (see [Payload formats](#payload-formats)) |
| `topic`   | Kafka topic (required) |
| `weight`  | How often this event is picked relative to others (default `1`) |
| `count`   | Exact number of messages for this event (see [Weights or quotas](#weights-or-quotas)) |
| `key`     | Field path used as the Kafka message key, e.g. `orderId` |
| `rules`   | Field path → rule, overriding the random value for that field |
| `headers` | Header name → rule |
| `pools`   | Pool name → record to add after each message (see [Pools](#pools)) |
| `resend`  | Sends a stored message again, unchanged, in place of generating one (see [Duplicate messages](#duplicate-messages)) |

### Descriptor sets

If your schema is published as a compiled binary `FileDescriptorSet` rather than `.proto` sources, point `descriptor` at it wherever you would use `proto`:

```json
"defaults": { "descriptor": "schemas/my-api.protobin" },
"events": [ { "type": "my.pkg.v1.ThingCreated", "topic": "things" } ]
```

Messages are generated, checked and encoded exactly as with the `.proto` the set was built from, and field paths are the same camelCase names. Build the set with `--include_imports` so that imported types such as `google.protobuf.Timestamp` are in it:

```sh
protoc -I protos --include_imports --descriptor_set_out=my-api.protobin protos/my/pkg/v1/*.proto
```

`proto` and `descriptor` can't both be set in `defaults`, or both on one event. An event's own `proto` or `descriptor` replaces whichever one `defaults` sets.

```json
"defaults": {
  "topic": "orders",
  "proto": "order.proto",
  "rules":   { "currency": { "static": "EUR" } },
  "headers": { "tracestate": { "static": "app=orders" } }
},
"events": [
  { "type": "shop.OrderCreated", "weight": 70 },
  { "type": "shop.OrderCancelled", "weight": 5, "topic": "orders-cancelled",
    "rules": { "currency": null }, "headers": { "tracestate": null } }
]
```

### Payload formats

Each event chooses how its value is encoded. One plan can mix all three.

| | Binary Protobuf | Protobuf as JSON | Schemaless JSON |
|---|---|---|---|
| Set with | nothing (default) | `"format": "json"` | `"payload": { ... }` in place of `proto` and `rules` |
| Shape defined by | the `.proto` | the `.proto` | the payload rules alone |
| Fields without a rule | filled automatically | filled automatically | none: list every field |
| Startup field checks | yes | yes | paths checked against the payload |

**Protobuf as JSON** uses the standard [Protobuf JSON mapping](https://protobuf.dev/programming-guides/json/), so it matches what Protobuf libraries in other languages produce:

- enums are names (`"BANK_TRANSFER"`)
- `int64` values are strings, with no precision loss (`"9007199254740993"`)
- `bytes` values are base64
- `google.protobuf.Timestamp` values are RFC 3339 strings (`"2026-10-29T06:07:33.208Z"`)
- fields left at their default value are omitted

**Schemaless JSON** builds the value from `payload` rules alone. Dotted paths create nested objects, and `field` rules can use values set earlier in the payload:

```json
{ "type": "logistics.ShipmentCreated", "topic": "shipments", "key": "shipmentId",
  "payload": {
    "shipmentId":       { "regex": "SHP-[0-9]{6}", "unique": true },
    "orderId":          { "from": "paid.id", "take": true },
    "customer.name":    { "from": "paid.order.customer.name" },
    "customer.country": { "from": "paid.order.customer.country.code" },
    "address.city":     { "faker": "location.city", "locale": { "from": "paid.order.customer.country.locale" } },
    "address.country":  { "field": "customer.country" },
    "createdAt":        { "iso": "recent" }
  } }
```

```json
{ "shipmentId": "SHP-282231", "orderId": "ORD-762",
  "customer": { "name": "Mme Dorothée Prevost", "country": "FR" },
  "address":  { "city": "Issy-les-Moulineaux", "country": "FR" },
  "createdAt": "2026-10-29T05:55:57.492Z" }
```

`type` is just the event's name here, used for `typeHeader` and messages. A payload is always sent as JSON, and `defaults.payload` is merged into it like `rules` are.

In both JSON formats the value is UTF-8 JSON. The content type isn't set automatically: add a `content-type` header, e.g. `{ "static": "application/json" }`.

Messages are sent as plain bytes in every format, without the Confluent Schema Registry header (magic byte and schema ID). Consumers that use Confluent's deserializers will reject them.

### Field paths

Field paths use the camelCase names protobufjs generates (`order_id` → `orderId`) and dots for nesting: `items.sku`. A rule on a repeated field applies to every element. A rule on a field inside a `oneof` makes that branch always chosen.

### Rules

| Rule | Example | Produces |
|------|---------|----------|
| `static`      | `{ "static": "EUR" }` | That exact value |
| `uuid`        | `{ "uuid": true }` | A random UUID |
| `iso`         | `{ "iso": "now+7d" }` | A date: `"past"`, `"future"`, `"recent"`, a time like `"now+7d"`, or a range `["now", "now+7d"]`, with an optional `precision` (see [Dates and times](#dates-and-times)). Becomes a proper value for `google.protobuf.Timestamp` fields, otherwise an ISO string |
| `regex`       | `{ "regex": "EX[0-9]{4}" }` | A string matching the pattern |
| `oneOf`       | `{ "oneOf": ["CARD", "PAYPAL"] }` | One of the listed values. Enum names work |
| `int`         | `{ "int": [1, 5] }` | An integer between min and max, inclusive |
| `email`       | `{ "email": true }` | A random email address |
| `faker`       | `{ "faker": "person.fullName" }` | Any [Faker](https://fakerjs.dev/api/) function, with optional `"args": [...]` |
| `traceparent` | `{ "traceparent": { "sampled": 0.9 } }` | A W3C `traceparent`. `sampled` is the chance of flag `01` (default `1`). Unsampled ones get flag `00`; for messages with no `traceparent` at all, see [Traced and untraced services](#traced-and-untraced-services) |
| `field`       | `{ "field": "orderId" }` | A value from the record being built. **Headers, payloads and datasets only** |
| `from`        | `{ "from": "contracts.currency" }` | A value from a dataset or pool (see [Reading](#reading-datasets-and-pools)) |
| `ref`         | `{ "ref": "customers" }` | A whole record from a dataset or pool, as a link (same as `from` without a property) |

Any rule can also have:

- **`"unique": true`**: regenerates the value until it hasn't been used before for that field. Use it for IDs, like a primary key. If the rule can't produce enough different values (say, 1000 IDs from `CT-[0-9]{2}`), the run stops with an error.

`faker` and `email` rules can also have:

- **`"locale"`**: generates the value for a country, with local names, phone formats, cities and email domains. Use a fixed [Faker locale](https://fakerjs.dev/guide/localization.html) such as `"fr"`, `"de"`, `"es"` or `"en_GB"`, or a rule that produces one. For example, `{ "field": "country.locale" }` uses the locale of the record's own country.

Header rules can also have:

- **`"optional": true`**: leaves the header out of the message when the rule gives no value, instead of stopping the run. All other headers are unaffected. Use it for headers that are only sometimes present:

  ```json
  "headers": {
    "x-sampled":   { "oneOf": [null, "1"], "optional": true },
    "x-note":      { "field": "note", "optional": true },
    "x-trace":     { "from": "order.trace", "optional": true }
  }
  ```

  The second header follows a proto3 `optional` field, which is left out about 30% of the time. The third reads a header that an earlier event may have left out and stored with `$header.traceparent`. Without `optional`, a rule that gives no value is an error, so typos in a `field` path still fail loudly. `optional` doesn't make a pool or dataset read optional: an event still waits for a record to read. On a resend event, a left-out header keeps the copied value.

### Traced and untraced services

To mix messages from services that trace with messages from services that don't, define the event twice with weights, and remove the header from one copy with `null`:

```json
"defaults": { "headers": { "traceparent": { "traceparent": {} } } },
"events": [
  { "type": "shop.OrderPaid", "topic": "orders", "proto": "order.proto", "weight": 30 },
  { "type": "shop.OrderPaid", "topic": "orders", "proto": "order.proto", "weight": 70, "headers": { "traceparent": null } }
]
```

About 30% of the messages carry a `traceparent` and the rest have none. With quotas, give each copy its own `count` (e.g. 300 and 700). If the event also adds to a pool, give both copies the same `pools`, store the header with `$header.traceparent`, and read it in linked events with `"optional": true`, so they leave it out too. This is different from `"sampled"`, which always sends the header and only changes its flag.

`fromPool` still works as another name for `from`.

### Dates and times

An `iso` rule takes one of:

| Value | Produces |
|-------|----------|
| `"past"`, `"future"`, `"recent"` | A random date in the last year, the next year, or the last day |
| `"now"` | The current time |
| `"today"` | Midnight UTC of the current day |
| `"now+7d"`, `"today-1d"`, `"today-1d+9h-15m"` | `now` or `today` moved by one or more signed offsets in `s`, `m`, `h`, `d` or `w` (weeks) |
| `["now", "now+7d"]` | A random time between two of the times above |

```json
"createdAt": { "iso": "now", "precision": "seconds" },
"dueDate":   { "iso": "today+30d", "precision": "date" },
"expiresAt": { "iso": ["now+1d", "now+7d"], "precision": "seconds" }
```

`precision` sets the format:

| `precision` | Example |
|-------------|---------|
| `"millis"` (default) | `2026-10-28T14:03:22.208Z` |
| `"seconds"` | `2026-10-28T14:03:22Z` |
| `"date"` | `2026-10-28` |
| `"epochMillis"` | `1793196202208` (a number) |
| `"epochSeconds"` | `1793196202` (a number) |

`seconds` and `date` round down, so a range starting at `now` can land a fraction of a second, or with `date` part of a day, before it. On a `google.protobuf.Timestamp` field, `seconds` and `date` set the stored value (no nanos, or midnight). The epoch formats don't apply to Timestamps, since they produce numbers. Use them for `int64` fields or headers.

Without `clock`, "now" is the real time when each value is made, so it moves forward during a long run. With `clock`, "now" is that fixed instant for every message, so `now+7d` gives the same value on every run. Random dates (`past`, `future`, `recent`, ranges, and unset Timestamp fields) are also placed around the clock.

### Datasets

Datasets are built once at startup, in dependency order, and logged to stderr:

```
dataset countries: 3 records
dataset users: 1000 records
dataset customers: 300 records (vip 60, regular 240)
dataset contracts: 500 records
```

There are four ways to build one:

```json
"datasets": {
  "countries": {
    "records": [
      { "code": "FR", "name": "France",  "locale": "fr", "currency": "EUR" },
      { "code": "DE", "name": "Germany", "locale": "de", "currency": "EUR" },
      { "code": "ES", "name": "Spain",   "locale": "es", "currency": "EUR" }
    ]
  },
  "users": {
    "size": 1000,
    "fields": {
      "id":      { "uuid": true },
      "country": { "ref": "countries" },
      "name":    { "faker": "person.fullName", "locale": { "field": "country.locale" } },
      "email":   { "email": true, "locale": { "field": "country.locale" }, "unique": true }
    }
  },
  "customers": {
    "from": "users", "size": 300,
    "fields": { "customerNo": { "regex": "CUS-[0-9]{6}", "unique": true } },
    "split": { "vip": 20, "regular": 80 }
  },
  "contracts": {
    "size": 500,
    "fields": {
      "id":       { "regex": "CT-[0-9]{6}", "unique": true },
      "customer": { "ref": "customers" },
      "currency": { "field": "customer.country.currency" }
    }
  }
}
```

1. **Fixed** (`countries`): `records` lists the rows yourself. Use it for real reference data such as countries, currencies or product categories. `fields` can still add generated values to each row.
2. **Generated** (`users`): `size` records, each built from `fields`. Here each user links to a random country and gets a name and email in that country's locale. The country mix is random, about the same number per country.
3. **Derived** (`customers`): `from` picks `size` **different** records of another dataset or sub-pool (all of them if `size` is left out). Each record keeps its source fields (`id`, `name`, `email`) and adds its own `fields`. This is a subset with extra columns.
4. **Linked** (`contracts`): `{ "ref": "customers" }` stores a whole customer record in each contract, so you can read through it later: `contracts.customer.email` or `contracts.customer.country.code`. Many contracts can share a customer, like a foreign key. Add `"take": true` to give each contract its own customer.

`fields` are filled in order, so a `field` rule can use a value set earlier in the same record. Datasets can read other datasets, but not pools, which are still empty at startup.

| Key      | Description |
|----------|-------------|
| `records` | Fixed list of rows. Can't be combined with `size` or `from` |
| `size`   | Number of records. Required unless `from` or `records` is set |
| `from`   | Dataset or sub-pool to derive from, e.g. `users` or `customers/vip` |
| `fields` | Field name → rule |
| `split`  | Sub-pool name → share (see [Sub-pools](#sub-pools)) |

### Pools

Pools are declared at the top level. A pool with no options needs only `{}`:

```json
"pools": {
  "order":   { "split": { "toPay": 80, "toCancel": 15, "open": 5 } },
  "payment": {}
}
```

An event adds a record to a pool after each message it produces. Each record value is one of:

- a field path, such as `orderId`
- `$header.<name>`, the header value
- `$from.<dataset or pool>`, the record this message read from that collection, stored as a link
- `$message`, the whole message as sent (topic, key, headers and encoded value), for a [resend](#duplicate-messages)

```json
{ "type": "shop.OrderCreated",
  "pools": {
    "order": { "id": "orderId", "amount": "total", "contract": "$from.contracts", "trace": "$header.traceparent" }
  } }
```

An event can add to several pools. A pool that isn't declared at the top level is created automatically when an event adds to it. The older `"pool": { "name": ..., "add": ... }` syntax still works.

Pools live in memory for a single run. With weights, each pool keeps at most 10,000 records, dropping the oldest first. With quotas there's no limit, because the startup check counts on every record staying available.

### Sub-pools

`split` divides a dataset or pool into sub-pools that don't overlap. Each new record goes into exactly one of them, following the shares. Records are dealt out in a fixed rotation rather than at random, so the shares are exact: 700 orders split 80/15/5 give 560 / 105 / 35.

Typical uses:

- **Separate paths in a lifecycle.** Paid and cancelled orders come from different sub-pools, so they never compete for the same order. A sub-pool nobody reads (`open`) gives orders that are never closed.
- **Segments of reference data.** For example, an event that only uses `customers/vip`.

### Reading datasets and pools

`from` and `ref` take a path of the form `<name>[/<sub-pool>][.<property path>]`:

| Path | Reads |
|------|-------|
| `contracts.id` | The `id` of a record from any part of `contracts` |
| `customers/vip.email` | The `email` of a record from the `vip` sub-pool only |
| `contracts.customer.email` | Through a link: the email of the contract's customer |
| `order/toPay.contract.currency` | Through a link stored in a pool record |
| `customers` | The whole record (what `ref` does) |

The same behaviour applies whatever is read:

- **One record per message.** All reads of the same collection, or the same sub-pool, in one message use the same record. A contract's ID, currency and customer email always match.
- **`take: true`** removes the record, for one-time transitions such as an order that can't be paid twice. In events you take from a sub-pool, not from all of a split collection.
- **Without `fallback`**, the event only runs once the collection has records. The first messages are always of the events that fill the pools, and no orphan references are produced.
- **With `fallback`**, the event can run while the collection is empty and uses the fallback rule instead.

```json
{ "type": "shop.OrderPaid",
  "rules": {
    "orderId":       { "from": "order/toPay.id", "take": true },
    "amount":        { "from": "order/toPay.amount" },
    "currency":      { "from": "order/toPay.contract.currency" },
    "customerEmail": { "from": "order/toPay.contract.customer.email" }
  },
  "headers": { "traceparent": { "from": "order/toPay.trace" } } }
```

### Lifecycles: one pool per state

`take` removes a record, so later events can't find it. Leaving `take` off lets an order be paid twice, or paid and cancelled. To get both safety and reachability, **move** the record: the event that takes it adds it to the pool for the next state.

```json
"pools": { "order": { "split": { "toPay": 2, "toCancel": 1 } }, "paid": {}, "shipped": {} }
```

```json
{ "type": "shop.OrderPaid",
  "rules": { "orderId": { "from": "order/toPay.id", "take": true } },
  "pools": { "paid": { "id": "orderId", "order": "$from.order/toPay", "trace": "$header.traceparent" } } },

{ "type": "shop.OrderShipped",
  "rules": { "orderId": { "from": "paid.id", "take": true },
             "customerEmail": { "from": "paid.order.contract.customer.email" } },
  "pools": { "shipped": { "id": "orderId", "order": "$from.paid.order" } } }
```

```
OrderCreated ──▶ order ─┬─ toPay ──OrderPaid──▶ paid ──OrderShipped──▶ shipped ──OrderDelivered──▶ (done)
                        └─ toCancel ──OrderCancelled──▶ (done)
```

- Each order is in exactly one pool at a time, so it can't skip a step or happen twice.
- `"order": "$from.order/toPay"` carries a link to the original record forward, so later events can still read its amount, currency or customer.
- Events that can repeat within a state read **without** `take`, e.g. `OrderNoteAdded` from `paid`. They stop automatically once the order moves on.

| Kind of event | Read with | Example |
|---|---|---|
| Changes the state (happens once) | `take: true`, then add to the next pool | Paid, Shipped, Cancelled |
| Happens within a state (repeatable) | no `take` | NoteAdded, AddressChanged |

`plans/plan-small.json` is a complete, small version of this flow.

### Weights or quotas

A plan decides the mix of events in one of two ways:

| | Weights | Quotas |
|---|---|---|
| Set on each event | `"weight": 70` | `"count": 700` |
| Total messages | Top-level `count` | Sum of the event counts |
| Messages per event | About the weight's share, varies per run | Exactly the count |
| Typical use | Realistic traffic, scaling by changing one number | Tests that check exact numbers |

With **quotas**, every event needs a `count`, and `weight` isn't allowed. Events are still interleaved randomly: each message picks an event by how many it has left, so all events finish together rather than one after another.

Quotas are checked against datasets, pools and sub-pools at startup, so the run can't get stuck halfway:

- events can't take more records than a dataset has, or than the events adding to a pool produce
- for a sub-pool, only its share of the added records counts
- if other events read without taking, the takers must leave at least one record
- every collection that is read must get records

```
Error: "order/toPay": shop.OrderPaid (600) take 600 records, but shop.OrderCreated (700) add 700, and only 560 of them go to "order/toPay"
```

### Duplicate messages

To test a consumer that deduplicates, send the same message more than once. A **resend** event sends a message that an earlier event stored, byte for byte. Use the simpler patterns below if only the id needs to match.

**Exact copies: `resend`.** The original stores itself in a pool with `$message`, and a resend event sends it again:

```json
"pools": { "sent": {} },
"events": [
  { "type": "shop.OrderPaid", "topic": "orders", "proto": "order.proto", "key": "orderId", "count": 3,
    "headers": { "message-id": { "uuid": true } },
    "pools": { "sent": { "message": "$message" } } },
  { "resend": { "from": "sent.message", "take": true }, "count": 3 }
]
```

Each copy has the same topic, key, headers and encoded value as its original, in every format and with every field type. Since a resend reads from a pool, the original is always sent first. With `"take": true` each stored message is resent once, and quotas check that there are enough. Without it, a random stored message is picked each time.

A resend event doesn't use `defaults`, because a default topic or a new `traceparent` would change the copy. It can only have these keys:

| Key | Description |
|-----|-------------|
| `resend` | `{ "from": "<pool>.<field>", "take": true }`: the record field that holds the message. `take` is optional |
| `type` | A name for errors and logs. The copy keeps the original's type header |
| `topic` | Sends the copy to another topic, for example a retry topic. Default: the original's topic |
| `headers` | Headers to add or replace on the copy, e.g. `{ "x-retry": { "static": "1" } }` |
| `pools` | Records to add after the copy: `$message`, `$header.<name>` or `$from.<pool>` |
| `count`, `weight` | As for any event |

To send each message three times, chain resends: the first copy stores itself again with `$message`, and a second resend takes from there:

```json
"pools": { "sent1": {}, "sent2": {} },
"events": [
  { "type": "shop.OrderPaid", "topic": "orders", "proto": "order.proto", "count": 2,
    "pools": { "sent1": { "message": "$message" } } },
  { "resend": { "from": "sent1.message", "take": true }, "count": 2,
    "pools": { "sent2": { "message": "$message" } } },
  { "resend": { "from": "sent2.message", "take": true }, "count": 2 }
]
```

Copies are mixed in with the other messages rather than sent right after their original.

**Only the id matches.** When the consumer compares only an id header, the rest of each message can change:

- **A fixed id:** one event with `"headers": { "message-id": { "static": "dup-test-0001" } }` and a `count`. The id is the same on every run, so a consumer that remembers ids will skip all of them on the next run, including the first. Change the id or clear the consumer's store between runs.
- **A new id each run:** a dataset with one record, read by the header:

  ```json
  "datasets": { "dup": { "size": 1, "fields": { "id": { "uuid": true } } } },
  "events": [ { "type": "shop.OrderPaid", "topic": "orders", "proto": "order.proto", "count": 3,
    "headers": { "message-id": { "from": "dup.id" } } } ]
  ```

## Example: plans/plan-datasets.json

```
users (1000) ──from──▶ customers (300, split vip/regular) ◀──ref── contracts (500)
                                                                       │ read by
OrderCreated (700) ─────────────────────────────────────────────────────┘
   │ adds to order pool, split toPay 80 / toCancel 15 / open 5
   ├──▶ OrderPaid (500)       takes from order/toPay, adds to payment pool
   │       └──▶ PaymentRefunded (50)  takes from payment
   └──▶ OrderCancelled (100)  takes from order/toCancel
```

Every payment carries its order's amount, currency, customer email and trace. Every refund matches its payment. No order is both paid and cancelled, and 100 orders stay open.

## Values without a rule

| Field type | Generated value |
|------------|-----------------|
| `string` | Two lorem words |
| integers | 0–100000 |
| `float` / `double` | 0–1000, two decimals |
| `bool` | Random |
| `bytes` | 8 random alphanumeric bytes |
| enum | A random value, never `*_UNSPECIFIED` |
| `google.protobuf.Timestamp` | A recent date (relative to `clock` if set) |
| nested message | Generated recursively, up to 4 levels deep |
| `repeated` | 1–3 elements |
| `map` | One entry, with a key of the right type |
| `oneof` | One random branch, unless a rule forces one |
| proto3 `optional` | Left out about 30% of the time |

## Validation

Before any message is generated or sent, the plan is checked and the run stops with a clear error if:

- an event is missing `type`, `topic`, or a `proto`, `descriptor` or `payload`, or the proto, descriptor set or type can't be loaded (including a descriptor set built without `--include_imports`)
- `defaults` or an event sets both `proto` and `descriptor`
- an event has an unknown `format`, or combines `payload` with `proto`, `descriptor`, `rules` or `"format": "protobuf"`
- a payload sets both an object and a field inside it (`customer` and `customer.name`)
- the plan has no top-level `count` with weights, mixes `weight` and `count` on an event, or leaves `count` off some events with quotas
- a rule uses an unknown rule type (`{ "rgex": "..." }`), a Faker function that doesn't exist, or an unknown locale
- an `iso` value isn't one of the forms in [Dates and times](#dates-and-times) (`"nwo"`, `"now+7y"`), a range ends before it starts, `precision` is unknown or used on a rule other than `iso`, an epoch `precision` is used on a Timestamp field, or `clock` isn't a date-time with a time zone
- `optional` isn't `true` or `false`, or is used on something other than a header's rule
- a `traceparent` rule has an unknown option, or a `sampled` outside 0–1
- `locale` is used on a rule other than `faker` or `email`, or `records` isn't a list of objects
- a rule path, `key` or pool record path doesn't exist in the message or payload
- a `field` rule is used on a Protobuf message field
- a `from`, `ref` or `$from` names a dataset, pool or sub-pool that doesn't exist
- an event takes from all of a split collection instead of one sub-pool
- a dataset reads a pool, datasets depend on each other in a loop, or a derived dataset wants more records than its source has
- an event adds to a dataset, or a dataset and a pool share a name
- a pool is read but no event adds to it
- a `resend` reads a dataset, names no record field, or reads a field that no event fills with `$message`; or a resend event has keys it can't use, such as `rules`, `key` or `proto`
- a `unique` rule runs out of values, or a dataset `take` runs out of records
- quotas don't fit the datasets and pools (see [Weights or quotas](#weights-or-quotas))

For example:

```
Error: shop.OrderPaid rules.orderId: pool "order" has no sub-pool "toPai" (it has toPay, toCancel, open)
```

Protobuf messages are also checked with protobufjs before encoding.
