---
layout: post
title: "Flink SQL Evolution: Handling Custom CDC with FROM_CHANGELOG and TO_CHANGELOG"
date: 2026-08-25
---

Hey all 👋 I'm Gustavo de Morais, an Apache Flink committer. I recently designed and released [FLIP-564: Support FROM_CHANGELOG and TO_CHANGELOG built-in PTFs](https://cwiki.apache.org/confluence/display/FLINK/FLIP-564%3A+Support+FROM_CHANGELOG+and+TO_CHANGELOG+built-in+PTFs). This is brand new functionality for things that just weren't possible in Flink SQL before.

That said, I thought it was worth a blog post - my first one after intensively contributing to Open Source Apache Flink for almost 2 years, inspired by [Robin Moffat](https://rmoff.net/) :)

> Before we start, shoutout to [Ramin Gharib](https://github.com/raminqaf), who contributed with some clean PRs during development!

**In this post:**

- [What are changelogs and what is CDC?](#changelogs-and-cdc)
- [What are TO_CHANGELOG and FROM_CHANGELOG?](#to-and-from-changelog)
- [Unblocked Use Cases](#common-use-cases)
- [What's missing?](#whats-still-missing)

## Why is this important?

Flink gets used for a lot of different things, and it works well for most of them, but there are still some gaps here and there. Let's take data replication, one of the most common use cases: it works great today for a set of supported formats. But if your database or service outputs events in another format, the only escape hatch is writing custom code in Flink SQL. There are also cases where Flink already picks an internal changelog format (append, upsert, retract) for you, and you just want a bit more control that.

`TO_CHANGELOG` and `FROM_CHANGELOG` are new built-in tools that fill exactly those gaps. Here is a general diagram of how the functions could be used to connect two systems with different changelogs:

![The round trip: a raw changelog goes through FROM_CHANGELOG into a Flink table, then through TO_CHANGELOG back into a changelog](/blog/assets/images/changelog-ptfs-roundtrip.png)

Using the functions sounds simple, right? Making this reliable, scalable, and efficient across billions of records between two systems isn't. That's what Flink takes care of under the hood - let's just focus on the functions and changelogs here.

## What are changelogs and what is CDC? {#changelogs-and-cdc}

If you have no idea about what changelogs are, ~~good for you~~ there are some small examples which I hope will help! They are pretty interesting and something you might want to hear about - they're behind a lot of scalable systems and databases you know (MySQL, Postgres, Kafka Streams and the list is long).

CDC stands for Change Data Capture: instead of just storing the current state of a database, you capture every insert, update, and delete that happens to it as a stream of events, so other systems can react to changes as they happen instead of polling for them. A changelog is just what that series of events looks like.

When you create a table in Flink SQL, it ends up with one of three changelog modes:

- **Append**: only inserts (`+I`)
- **Retract**: inserts, plus updates split into an old image and a new image (`-U`, `+U`), plus deletes (`-D`)
- **Upsert**: inserts, updates, and deletes, all keyed - no need for the old image, the key tells you what's being replaced

Say an order gets created, then its status changes, then it gets deleted. In retract mode that's:

```
+I[order: 1, status: NEW]
-U[order: 1, status: NEW]
+U[order: 1, status: SHIPPED]
+I[order: 2, status: NEW] -> Second new unrelated order
-D[order: 1, status: SHIPPED] -> First order deletion event
```

```
Final table, you only see the second order:
order: 2, status: NEW
```

In upsert mode (order id as key), same changes are shorter:

```
+I[order: 1, status: NEW]
+U[order: 1, status: SHIPPED]
+I[order: 2, status: NEW]
-D[order: 1, status: SHIPPED] -> First order deletion event
```

```
Table, what you see
order: 2, status: NEW
```

Both streams look different, but they land on the exact same table. That's really all a changelog is: an encoding of how a table got to where it is. The table is the actual information; the changelog is just one of several ways to say it out loud. And in append mode, things are easier and every event is a new row, there are no updates. You just append things on top of each other and this is your table. Append can "express less". However, append is also the cheapest mode of all since it's a raw log of messages. In general, append pipelines are the most scalable and efficient ones. But yeah, each mode has its use cases.

That's the core of the problem: three modes, and every CDC tool, format, and connector out there has its own opinion about which one it speaks and how. MySQL's binlog, Debezium, DynamoDB Streams, a custom event you built yourself - they all encode inserts/updates/deletes differently, and until now Flink only understood the ones it had a connector for.

## What are TO_CHANGELOG and FROM_CHANGELOG? {#to-and-from-changelog}

Two new dangerous but powerful built-in functions, and for the first time in Flink SQL:

- `TO_CHANGELOG` lets you turn an updating pipeline back into an append-only one.
- `FROM_CHANGELOG` lets you bring in a CDC format Flink has never heard of.

### TO_CHANGELOG

```sql
SELECT * FROM TO_CHANGELOG(
    input => TABLE orders_per_region
)
```

Say `orders_per_region` currently looks like this:

```
Table
region: 'EU', cnt: 2
```

That single row in the table is the end result of two changes: an insert (`cnt: 1`), then an update (`cnt: 2`). `TO_CHANGELOG` hands you the changelog that led to that final table:

```
Output of TO_CHANGELOG for orders_per_region
+I[op: 'INSERT',       region: 'EU', cnt: 1]
+I[op: 'UPDATE_AFTER',  region: 'EU', cnt: 2]
```

### FROM_CHANGELOG

```sql
SELECT * FROM FROM_CHANGELOG(
    input => TABLE raw_cdc
)
```

Now go the other way. Say you have a `raw_cdc` Kafka topic coming from whichever system or database. For simplicity, we'll use the same example as above.

```
raw_cdc
+I[op: 'INSERT',       region: 'EU', cnt: 1]
+I[op: 'UPDATE_AFTER',  region: 'EU', cnt: 2]
```

`FROM_CHANGELOG` reads that `op` column and turns each row into the row kind it names, landing you right back at the same Flink table:

```
Table
region: 'EU', cnt: 2
```

That's the whole trick: nothing about the data changed, just which side is allowed to see it as a table and which side has to see it as a plain, appendable log. These new functions are just a nice, custom way of connecting the input and output of two data format worlds to unlock new use cases!

Now you may ask me, why are they dangerous, Gustavo? Well, let's say you can break things in very weird ways if you configure things incorrectly here and there are multiple ways of doing that. Imagine only this extreme example: what will you get if you map INSERT to DELETE and DELETE to INSERT? 🧨

## What are they made for?

I think they solve problems in two distinct major areas. Disclaimer, this is my personal opinion:

1. **Reading and writing CDC in a format Flink doesn't have a connector for.** No custom deserializer to write, no waiting on a connector - just describe your operation column in SQL.
2. **Working around planner limitations.** Just as an example, some operators, like `LAG` over an `OVER` window, only accept certain changelog modes as input. `TO_CHANGELOG` lets you explicitly flatten an updating stream into something they can consume - which, not coincidentally, is exactly what produces <code style="color:red">Can't generate a valid execution plan for the given query:</code>.

Obs.: A lot of times the error message means your query is broken and not the engine!

## Full function signatures

The examples above only scratched the surface - both functions take a few more arguments. Full availability lands in Flink 2.4; 2.3 ships with a more limited set. [Full Open Source documentation can be found here](https://github.com/apache/flink/blob/master/docs/content/docs/sql/reference/queries/changelog.md).

```sql
SELECT * FROM FROM_CHANGELOG(
    input          => TABLE source_table [PARTITION BY key_col [ORDER BY time_col]],
    op             => DESCRIPTOR(op_column_name),
    op_mapping     => MAP['c, r', 'INSERT', 'u', 'UPDATE_AFTER', 'd', 'DELETE'],
    error_handling => 'FAIL' | 'SKIP'
)
```

```sql
SELECT * FROM TO_CHANGELOG(
    input                 => TABLE source_table [PARTITION BY key_col],
    op                    => DESCRIPTOR(op_column_name),
    op_mapping            => MAP['INSERT, UPDATE_AFTER', 'u', 'DELETE', 'd'],
    produces_full_deletes => BOOLEAN
)
```

## Unblocked Use Cases {#common-use-cases}

### Writing an aggregation to an append-only sink

Any `GROUP BY` aggregation over a stream produces an updating table - Flink has to be able to update the result for a key when a new row for that key shows up. An append-only sink won't take that.

```sql
CREATE VIEW totals AS
SELECT customer_id, COUNT(*) AS cnt
FROM orders
GROUP BY customer_id;

INSERT INTO sink
SELECT * FROM TO_CHANGELOG(input => TABLE totals);
```

`TO_CHANGELOG` flattens the retract output into inserts with an explicit `op` column, so the append-only sink can take it.

### Deduplicating records without watermarks

> This one comes from David Anderson, Principal Software Practice Lead

Flink only trusts a dedup's `ROW_NUMBER()` winner as final once it's ordered by a watermarked time attribute. Order by anything else, and the planner keeps the result updating - even when the winner can never actually change, like with exact duplicates:

```sql
SELECT * FROM TO_CHANGELOG((
    SELECT trade_id, ticker, quantity, price
    FROM (
        SELECT *, ROW_NUMBER()
            OVER (PARTITION BY trade_id, ticker, quantity, price
                  ORDER BY trade_id, ticker, quantity, price ASC) AS row_num
        FROM trades_with_dups
    )
    WHERE row_num = 1
));
```

Now dedup is append-only and deterministic, no event time or watermarks involved at all - just keep in mind this holds for true exact duplicates. This is common after Flink jobs reprocess and leave exact duplicates in the sink.

### Using an append-only built-in function on an updating stream

Let's take LAG as an example: it's a function that gives you the previous row's value, but only accepts append-only tables. An updating view doesn't qualify, so Flink refuses to plan it. `TO_CHANGELOG` fixes that by turning the updates into explicit inserts first:

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

In Flink, a plain append event has no way to say "delete this downstream." Only an upsert table can turn into a real Kafka tombstone. `FROM_CHANGELOG` is how you get there even when your source never called itself upsert in the first place.

Say you're keeping a compacted topic of current employees, keyed by employee ID. When someone leaves, you want that key physically gone downstream, not just a row with a "deleted" flag on it.

Your regular inserts and updates need nothing special - a plain `INSERT INTO` an upsert-kafka table already overwrites by key. The only piece missing is deletes, so add a small side pipeline just for those:

```sql
INSERT INTO employee_state
SELECT id, name
FROM FROM_CHANGELOG(
    input      => TABLE (SELECT * FROM employee_events WHERE op = 'd'),
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
    op_mapping => MAP[
        'c, r', 'INSERT',
        'u',    'UPDATE_AFTER',
        'd',    'DELETE'
    ]
);
```

Same result, one pipeline instead of two.

There are more creative uses out there. If you've found one, I'd love to hear about it - send me an <a href="&#109;&#97;&#105;&#108;&#116;&#111;&#58;&#103;&#117;&#115;&#116;&#97;&#118;&#111;&#112;&#103;&#117;&#116;&#111;&#64;&#103;&#109;&#97;&#105;&#108;&#46;&#99;&#111;&#109;">email</a> or a message on [LinkedIn](https://www.linkedin.com/in/gustavo-demorais/).

## What's missing? {#whats-still-missing}

So, we're almost at the end. Now, the FLIP is only partially implemented. Anything that needs turning one event into several, or several into one, isn't supported yet - for example, a CDC format that packs both the old and new image into a single message. `FROM_CHANGELOG` maps one input row to exactly one output row, so it can't split that message into an `UPDATE_BEFORE`/`UPDATE_AFTER` pair on its own. You can usually work around this upstream with a Kafka Connect single message transform (SMT) - the same idea Debezium uses to unwrap or filter events before they hit the topic.

Why did we do that? I've tried to ship core first with stateless functions. I want to see how far people get with just this before we add the extra complexity the remaining cases would need. It's easy to implement two super complex functions that do it all but I think optimally we want to keep things lean. If you want to dig into this more, send me an email or a message! I'm also giving a talk on it at Community Over Code (formerly ApacheCon) - come say hi 👋

One more thing worth being upfront about: Flink 2.3 itself ships with very limited feature availability - pretty much the bare bones, retract only. Everything else I've shown above, `PARTITION BY`, upsert output, `op_mapping`, `error_handling`, `produces_full_deletes`, lands in Flink 2.4. Whether even more gets built after that is TBD. If you run into a hard blocker along the way that you can't work around, I'd be happy to hear about it.

## That's it for today

I think that's it for a general overview of what these two ~~trouble-makers~~ underrated heroes will be messing with. By the way, if you asked yourself why these are not regular built-in functions but Process Table Functions: well, these functions are extra powerful because they need to interact and use elements from the engine that a regular built-in function isn't allowed to. Btw, these are the first ever released built-in Process Table Functions for Flink. Which is in itself a pretty exciting feature you should look into if you don't know yet. Spoiler: if you didn't like my functions, you can write your own version of them ;) Check it out [here](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/functions/ptfs/).

