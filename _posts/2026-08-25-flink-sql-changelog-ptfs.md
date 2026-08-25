---
layout: post
title: "Flink SQL Evolution: Handling Custom CDC with FROM_CHANGELOG and TO_CHANGELOG Process Table Functions"
date: 2026-08-25
---

Hey all 👋 I'm Gustavo de Morais, an Apache Flink committer. I recently designed and released [FLIP-564: Support FROM_CHANGELOG and TO_CHANGELOG built-in PTFs](https://cwiki.apache.org/confluence/display/FLINK/FLIP-564%3A+Support+FROM_CHANGELOG+and+TO_CHANGELOG+built-in+PTFs). (Shoutout also to Ramin Gharib, who contributed with some clean PRs during development!).

This is brand new functionality for things that just weren't possible in Flink SQL before, so I thought it was worth a blog post - my first one after intensively contributing to Open Source Apache Flink for almost 2 years, inspired by [Robin Moffat](https://rmoff.net/) :)

## Is this for you?

If you're doing any CDC processing, moving data between systems, or you've ever hit a corner of Flink where you knew exactly what you wanted and Flink just wouldn't let you do it (for example, you've seen this lovely message: <code style="color:red">Can't generate a valid execution plan for the given query:</code>), keep reading. If you have no idea about what changelogs are, ~~good for you~~ there are some small examples which I hope will help!

[small diagram of the round trip: raw changelog -> FROM_CHANGELOG -> Flink table -> TO_CHANGELOG -> changelog back out]

## What are changelogs and what is CDC?

CDC stands for Change Data Capture: instead of just storing the current state of a database, you capture every insert, update, and delete that happens to it as a stream of events, so other systems can react to changes as they happen instead of polling for them. A changelog is just what that stream of events looks like on the wire.

When you create a table in Flink SQL, it ends up with one of three changelog modes:

- **Append**: only inserts (`+I`)
- **Retract**: inserts, plus updates split into an old image and a new image (`-U`, `+U`), plus deletes (`-D`)
- **Upsert**: inserts, updates, and deletes, all keyed - no need for the old image, the key tells you what's being replaced

Say an order gets created, then its status changes, then it gets deleted. In retract mode that's:

```
+I[order: 42, status: NEW]
-U[order: 42, status: NEW]
+U[order: 42, status: SHIPPED]
+I[order: 43, status: NEW]
-D[order: 42, status: SHIPPED]
```

```
Table
order: 43, status: NEW
```

In upsert mode (order id as key), it's shorter:

```
+I[order: 42, status: NEW]
+U[order: 42, status: SHIPPED]
+I[order: 43, status: NEW]
-D[order: 42, status: SHIPPED]
```

```
Table
order: 43, status: NEW
```

Both streams look nothing alike on the wire, but they land on the exact same table. That's really all a changelog is: an encoding of how a table got to where it is. The table is the actual information; the changelog is just one of several ways to say it out loud. And in append mode, you can't say it at all - there's no way to represent a delete, so this whole scenario can't be expressed. However, append is also the cheapest mode of all since it's a raw lag of messages. In general, append pipelines are the most scalable and efficient ones. But yeah, each mode has its use cases.

That's the core of the problem: three modes, and every CDC tool, format, and connector out there has its own opinion about which one it speaks and how. Debezium, DynamoDB Streams, a custom event you built yourself - they all encode inserts/updates/deletes differently, and until now Flink only understood the ones it had a connector for.

## The new swiss army knife: TO_CHANGELOG and FROM_CHANGELOG

Two new dangerous but powerful built-in functions, and for the first time in Flink SQL:

- `TO_CHANGELOG` lets you turn an updating pipeline back into an append-only one.
- `FROM_CHANGELOG` lets you bring in a CDC format Flink has never heard of.

### TO_CHANGELOG

```sql
SELECT * FROM TO_CHANGELOG(
    input => TABLE orders_per_region
)
```

Every row comes out as a plain insert, with an extra `op` column telling you what it originally was:

```
+I[op: 'INSERT',       region: 'EU', cnt: 1]
+I[op: 'UPDATE_AFTER',  region: 'EU', cnt: 2]
+I[op: 'DELETE',       region: 'EU', cnt: 1]
```

### FROM_CHANGELOG

```sql
SELECT * FROM FROM_CHANGELOG(
    input => TABLE raw_cdc
)
```

Same idea in reverse: read an append-only stream that carries its own operation code, and get back a proper updating Flink table. If you write the result to Kafka through an upsert sink, the deletes come out as real Kafka tombstones, not just rows with an `op` column saying "delete".

Here's the thing I keep coming back to: in Flink, a plain append event has no way to say "delete this downstream." Only an upsert table can turn into a real Kafka tombstone. `FROM_CHANGELOG` is how you get there even when your source never called itself upsert in the first place.

Now you may ask me, why are they dangerous, Gustavo? Well, let's say you can break things in very weird ways if you configure things incorrectly here and there are multiple ways of doing that. Imagine only this extreme example, imagine what you'll get if you map INSERT to DELETE and DELETE to INSERT lol

## What are they made for?

I think they solve problems in two distinct major areas. Disclaimer, this is my personal opinion:

1. **Reading and writing CDC in a format Flink doesn't have a connector for.** No custom deserializer to write, no waiting on a connector - just describe your operation column in SQL.
2. **Working around planner limitations Flink still has today.** Just as an example, some operators, like `LAG` over an `OVER` window, only accept certain changelog modes as input. `TO_CHANGELOG` lets you flatten an updating stream into something they can consume - which, not coincidentally, is exactly what produces that <code style="color:red">Can't generate a valid execution plan for the given query:</code> error from the top of this post if you skip it.

Obs.: A lot of times the error message means your query is broken and not the engine!

## Full function signatures

The examples above only scratched the surface - both functions take a few more arguments. Full availability lands in Flink 2.4; 2.3 ships with a more limited set.

```sql
SELECT * FROM FROM_CHANGELOG(
    input          => TABLE source_table [PARTITION BY key_col [ORDER BY time_col]],
    op             => DESCRIPTOR(op_column_name),
    op_mapping     => MAP['c, r', 'INSERT', 'u', 'UPDATE_AFTER', 'd', 'DELETE'],
    error_handling => 'FAIL' | 'SKIP'
)
```

- `PARTITION BY` co-locates every event for the same key on the same task, and lets you produce an upsert changelog instead of a retract one (map to `UPDATE_AFTER` without `UPDATE_BEFORE`).
- `ORDER BY` reorders out-of-order CDC events per key by a watermarked time attribute (requires `PARTITION BY`).
- `op_mapping` maps as many of your own codes to Flink's row kinds as you need, comma-separated codes included.
- `error_handling` decides what happens on an op code you didn't map: fail the job (default) or skip the row.

```sql
SELECT * FROM TO_CHANGELOG(
    input                 => TABLE source_table [PARTITION BY key_col],
    op                    => DESCRIPTOR(op_column_name),
    op_mapping            => MAP['INSERT, UPDATE_AFTER', 'u', 'DELETE', 'd'],
    produces_full_deletes => BOOLEAN
)
```

- `op_mapping` here only forwards the row kinds you list - anything you leave out gets dropped, which is a handy way to filter.
- `produces_full_deletes` controls whether a `DELETE` row carries every column (the default) or just the key, with everything else `null`. Turning it off skips a stateful step, but only makes sense once you have a key to fall back on (`PARTITION BY`, or a declared upsert key).

## Common use cases

### Writing an aggregation to an append-only sink

Any `GROUP BY` aggregation over a stream produces a retract table - Flink has to be able to update the result for a key when a new row for that key shows up. An append-only sink won't take that.

```sql
CREATE VIEW totals AS
SELECT customer_id, COUNT(*) AS cnt
FROM orders
GROUP BY customer_id;

INSERT INTO sink
SELECT * FROM TO_CHANGELOG(input => TABLE totals);
```

`TO_CHANGELOG` flattens the retract output into inserts with an explicit `op` column, so the append-only sink can take it.

### Using LAG on an updating stream

`LAG` over an `OVER` window expects append-only input. An updating view doesn't qualify, so Flink refuses to plan it. `TO_CHANGELOG` fixes that by turning the updates into explicit inserts first:

```sql
CREATE VIEW orders_changelog AS
SELECT op, order_id, status, ts
FROM TO_CHANGELOG(input => TABLE orders);

INSERT INTO sink
SELECT order_id, op, status,
       LAG(status) OVER (PARTITION BY order_id ORDER BY ts) AS prev_status
FROM orders_changelog;
```

Now you get the previous status of an order on every change, something that simply wasn't expressible before.

### Converting a custom CDC format

> This one comes from Martijn Visser, ~~the Swiss Army knife of PMs lol~~ Product Management Director at Confluent - when he first brought it up, I didn't think it was actually supported yet. Turns out it is.

DynamoDB Streams has no Table API/SQL connector, and its events don't look anything like Flink's row kinds - they carry an `eventName` of `INSERT`, `MODIFY`, or `REMOVE`. There is a DataStream connector, but it just hands you raw records; you're still on your own to write custom deserialization code to get any kind of CDC semantics out of it. `FROM_CHANGELOG` gets you there in plain SQL instead:

```sql
INSERT INTO items
SELECT id, message
FROM FROM_CHANGELOG(
    input      => TABLE dynamodb_cdc PARTITION BY id,
    op         => DESCRIPTOR(eventName),
    op_mapping => MAP[
        'INSERT', 'INSERT',
        'MODIFY', 'UPDATE_AFTER',
        'REMOVE', 'DELETE'
    ]
);
```

(A real DynamoDB record nests its attributes in typed maps, so in practice you'd add a couple of computed columns to pull `id` and `message` out first - skipped here to keep the example short.)

Note the `INSERT INTO items` - materialize the result into its own upsert table first, rather than querying `FROM_CHANGELOG` directly from something else downstream. You can read more about this example [in the Confluent Cloud docs](https://docs.confluent.io/cloud/current/flink/how-to-guides/read-write-custom-changelog.html#example-convert-aws-short-dynamodb-streams-change-data).

### Emitting Kafka tombstones as a side pipeline

> This example comes from Taku Suzuki, Solutions Architect at Confluent

Say you're keeping a compacted topic of current employees, keyed by employee ID. When someone leaves, you want that key physically gone downstream, not just a row with a "deleted" flag on it.

Your regular inserts and updates need nothing special - a plain `INSERT INTO` an upsert-kafka table already overwrites by key. The only piece missing is deletes, so add a small side pipeline just for those:

```sql
INSERT INTO employee_state
SELECT id, name
FROM FROM_CHANGELOG(
    input      => TABLE (SELECT * FROM employee_events WHERE op = 'd'),
    op         => DESCRIPTOR(op),
    op_mapping => MAP['d', 'DELETE']
);
```

That's it. The delete-flagged event becomes a real `DELETE` row, and your existing upsert-kafka sink writes the tombstone the same way it always has.

Of course, you don't have to split it into two pipelines - that's just handy when you already have one pipeline handling inserts and updates and want to bolt tombstones on without touching it. The more common story is probably a single pipeline that maps everything, tombstones included, in one `FROM_CHANGELOG` call:

```sql
INSERT INTO employee_state
SELECT id, name, op, __deleted
FROM FROM_CHANGELOG(
    input      => TABLE employee_events PARTITION BY id,
    op         => DESCRIPTOR(op),
    op_mapping => MAP[
        'c, r', 'INSERT',
        'u',    'UPDATE_AFTER',
        'd',    'DELETE'
    ]
);
```

Same result, one pipeline instead of two.

There are more creative uses out there. If you've found one, I'd love to hear about it - send me an <a href="&#109;&#97;&#105;&#108;&#116;&#111;&#58;&#103;&#117;&#115;&#116;&#97;&#118;&#111;&#112;&#103;&#117;&#116;&#111;&#64;&#103;&#109;&#97;&#105;&#108;&#46;&#99;&#111;&#109;">email</a> or a message on [LinkedIn](https://www.linkedin.com/in/gustavo-demorais/).

## What's still missing

So, we're almost at the end. Now, the FLIP is only partially implemented. Anything that needs turning one event into several, or several into one, isn't supported yet - for example, a CDC format that packs both the old and new image into a single message. `FROM_CHANGELOG` maps one input row to exactly one output row, so it can't split that message into an `UPDATE_BEFORE`/`UPDATE_AFTER` pair on its own. You can usually work around this upstream with a Kafka Connect single message transform (SMT) - the same idea Debezium uses to unwrap or filter events before they hit the topic.

Why did we do that? I've tried to ship core first with stateless functions. I want to see how far people get with just this before we add the extra complexity the remaining cases would need. It's easy to implement two super complex functions that do it all but I think optimally we want to keep things lean. If you want to dig into this more, I'm giving a talk on it at Community Over Code (formerly ApacheCon) - come say hi 👋

One more thing worth being upfront about: Flink 2.3 itself ships with very limited feature availability - pretty much the bare bones, retract only. Everything else I've shown above, `PARTITION BY`, upsert output, `op_mapping`, `error_handling`, `produces_full_deletes`, lands in Flink 2.4. Whether even more gets built after that is TBD. If you run into a hard blocker along the way that you can't work around, I'd genuinely like to hear about it.

## That's it for today

I think that's it for a general overview of what these two trouble-makers ~~underrated heroes~~ will be messing with. By the way, if you asked yourself why these are not regular built-in functions but Process Table Functions: well, these functions are extra powerful because they need to interact and use elements from the engine that a regular built-in function isn't allowed to. Btw, these are the first ever released built-in Process Table Functions for Flink. Which is in itself a pretty exciting feature you should look into if you don't know yet. Spoiler: if you didn't like my functions, you can write your own version of them ;) Check it out [here](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/functions/ptfs/).
