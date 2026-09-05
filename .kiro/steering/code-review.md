# Code Review Guidelines

When reviewing or writing code, always check **naming** in addition to logic:

- Variable, method, type, and file names should be clear, consistent, and idiomatic Scala.
- Flag names that are ambiguous, misleading, or inconsistent with the rest of the module.
- Prefer camelCase for vals/methods, PascalCase for types/classes, and lowercase for package objects.
- Ensure names across a module tell a coherent story (e.g., sibling types should follow the same naming pattern).

### Project naming conventions

These are the naming rules agreed for this library. Apply them consistently across all modules.

- **Casing by visibility:** local-private identifiers use `snake_case`; package-private and
  public identifiers use `camelCase`; types/classes/objects use `PascalCase`. The `snake_case`
  for local-privates is deliberate, it visually distinguishes internal helpers from the public API.
- **"Id" is a word, not an acronym:** treat `Id` (short for "Identifier") as an ordinary word,
  so only its first letter is capitalized. This gives one uniform rule with no type-vs-field
  special case: an identifier derived from a type is the *type name with its first letter
  lowercased*. So the type is `ServiceId` and the field/param/val/pattern binding is `serviceId`
  (`MetricId`, `GroupId`, `RegisteredSchemaId`; `metricId`, `groupId`). The pair echoes cleanly
  everywhere — `serviceId: ServiceId`, `case ServiceId(serviceId)`, `def lookup(serviceId: ServiceId)`.
  Rationale: the older `ID` form (`ServiceID`) forced a two-part rule because `ServiceID` does not
  lowercase to a clean binding name, and trailing caps (`serviceID`) read like a constant on a val.
  Note `ServiceIdentity` is "Identity", not `Id`, so it is unaffected.
  This supersedes the earlier `ID`-uppercased convention; existing `*ID` names should migrate to
  `*Id` in a deliberate, module-wide rename pass (see "Wire format is frozen" — type renames are
  wire-safe, but check any serialized string keys separately).
- **Private shared constants:** a private `val` that holds a fixed literal reused internally
  (e.g. a JSON field-key string) uses `SCREAMING_SNAKE_CASE`. Three properties justify it: it is
  private (does not pollute the public space), it is a shared literal (reused internally), and it
  is a constant (never varies). This sits visually apart from types (PascalCase) and ordinary
  vals (camelCase/snake_case). Examples: `TOPIC`/`PARTITION` in kafka, `EMPTY`/`JITTER`/`POLICY`
  in common/chrono.
- **Wire format is frozen:** never rename a value that is serialized, JSON object keys, Avro
  record field names, OAuth token fields, etc. Renaming the Scala *identifier* of a constant is
  fine as long as its literal *value* (the wire key) is unchanged. Type renames are wire-safe
  because type names are not serialized.

Also look for **edge cases**:

- Null inputs, empty collections, zero/negative values, boundary conditions.
- Concurrency issues: shared mutable state, race conditions, resource leaks.
- Error handling gaps: unhandled exceptions, swallowed errors, missing recovery paths.
- Type safety holes: unchecked casts, implicit conversions that may surprise.

Check **resource safety**:

- Streams, connections, and clients must be properly bracketed (Resource, .onFinalize, .use).
- Flag leaked references, missing cleanup, or blocking calls on the cats-effect thread pool.

Verify **lawfulness**:

- Typeclass instances (Eq, Show, Encoder/Decoder) should obey their laws.
- Codec round-trips: decode(encode(a)) == a.

Review **API surface**:

- Is anything exposed that should be private or package-private?
- Are sealed hierarchies exhaustive? Could a method signature be tighter (narrower types, fewer parameters)?

Watch for **performance traps**:

- Unnecessary allocations in hot paths.
- Lazy val where eager is fine, or eager where lazy is needed.
- Blocking calls that should be wrapped in Sync[F].blocking.

Ensure **consistency with existing patterns**:

- New code should follow conventions already established in the module.
- Flag one-off styles that diverge from neighboring code without good reason.

### Documentation style

- **Use backtick code spans, not Scaladoc `[[...]]` links.** When referring to a type, method, or
  identifier in a Scaladoc comment, write it as a backtick code span (`` `EmailObserver` ``,
  `` `Params.apply` ``) rather than a Scaladoc wiki link (`[[EmailObserver]]`,
  `[[Params.apply]]`). This applies to references to both project symbols and external library
  symbols. Convert any existing `[[...]]` to backticks when touching a file.
