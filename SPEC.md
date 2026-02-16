# SPEC.md — pypipe (Pythonic Data Pipeline Library)

## Purpose of this Document

This document defines the **long-term architecture, goals, and design principles** of the **pypipe** library.
It is intended to serve as the **central specification across all future versions** of the library.
Individual releases may introduce features incrementally, but all implementations should align with this specification.

---

## Overview

**pypipe** is a lightweight, pythonic library for building repeatable data pipelines that move data from **A → B** using lazy, composable processing chains.

Core concepts:

* **Sources** produce an `Iterable[T]` or `AsyncIterable[T]`
* **Operators** transform, filter, batch, deduplicate, retry, and rate-limit data
* **Sinks** consume an iterable and write results to a destination
* **Runners** execute pipelines and provide execution statistics
* Both **synchronous and asynchronous pipelines** are first-class citizens

The design prioritizes:

* Python iterator semantics
* Functional composition
* Lazy execution
* Minimal dependencies
* Clear extensibility for future features

---

## Design Goals

The library must support:

1. Composable pipeline construction
2. Lazy execution (no work before `.run()`)
3. Iterator-based processing for low memory usage
4. Batch-friendly processing for efficient I/O
5. Deduplication and normalization workflows
6. Retryable operations for unstable sources or sinks
7. Rate-limited operations for APIs
8. Structured execution statistics
9. Sync and async pipelines
10. Extensibility without breaking existing pipelines

---

## Non-Goals

The library is not intended to replace:

* Workflow orchestration systems
* Distributed compute engines
* Crawling frameworks
* Schema validation frameworks

Those concerns are intentionally kept outside the core scope.

---

## Core Architecture

### Pipeline Flow

```
Source → Operators → Sink → Runner
```

Where:

* Source: `Iterable[T]` or `AsyncIterable[T]`
* Operators: chained transformations producing new iterables
* Sink: terminal consumer
* Runner: execution controller

---

## Public API Concepts

### Entry Points

```
pipe(iterable: Iterable[T]) -> Pipe[T]
apipe(async_iterable: AsyncIterable[T]) -> AsyncPipe[T]
```

---

### Core Operators

Pipelines must support the following operator categories:

#### Transform

* `map`
* `flat_map`

#### Filtering

* `filter`

#### Side-effects

* `tap`

#### Flow control

* `batch`
* `rate_limit`

#### Data quality

* `dedupe`

#### Reliability

* `retry_map`
* `retry_tap`

Additional operators may be introduced over time, but these form the foundational operator set.

---

## Deduplication Model

Deduplication must support:

* key-based uniqueness
* optional bounded memory mode
* deterministic retention of the first seen element

---

## Retry Model

Retry mechanisms must support:

* configurable retry count
* configurable backoff strategy
* configurable exception filtering
* deterministic retry behavior per element

---

## Statistics and Observability

Pipeline execution must provide structured statistics including:

* execution duration
* number of processed items
* number of emitted items
* dropped elements
* batch counts
* error counts

Statistics must be available through the runner interface.

---

## Async Architecture

Async pipelines must mirror the sync API wherever possible to ensure conceptual symmetry:

* equivalent operators
* async runners
* async iteration semantics
* compatible statistics model

---

## Package Structure (Conceptual)

```
pypipe/
    core/
    operators/
    async/
    runner/
    stats/
tests/
docs/
```

Exact module layout may evolve, but logical separation must remain.

---

## Extensibility Principles

Future versions must preserve:

* backward-compatible operator chaining
* iterable-based processing model
* stateless operator philosophy
* separation of sources/sinks from the core pipeline engine

New features should be introduced as additional operators or optional extensions rather than modifying core semantics.

---

## Performance Principles

* Streaming processing by default
* Batch support for I/O efficiency
* No unnecessary materialization of intermediate datasets
* Memory-safe deduplication options

---

## Compatibility Philosophy

This specification defines the **stable conceptual contract** of the library.

Future versions may:

* add operators
* add optional modules
* improve performance
* expand async capabilities

but must maintain:

* the pipeline chaining model
* iterable-first architecture
* deterministic execution semantics

---

## Evolution of the Specification

This document is intended to evolve alongside the project.
Updates should refine architecture and concepts rather than redefine the fundamental processing model.

---
