# LAW import note

The imported `AGENTS.md` is preserved verbatim. Its opening paragraph identifies the
project LAW as `adr/`; its convention block names the glob `adr/*.md`. A later
engineering-responsibility sentence refers to `law/adr/`. These two path spellings
conflict.

The canonical import path is `adr/`, following the explicit LAW glob and the
opening pointer. The eight source ADR and audit documents are byte-for-byte imports
from `KodBena/gomecsolver` at `refactor-1`. No source law text has been edited to
resolve the path discrepancy. There is only one copy of each record.
