# Vendored third-party dependencies

## nlohmann/json

`nlohmann/json.hpp` is the single-header amalgamation of the
[nlohmann/json](https://github.com/nlohmann/json) C++ JSON library,
version **3.11.3**.

Vendored here rather than fetched at build time to guarantee cross-platform
reproducibility and avoid dependence on distro package managers or external
package registries.

**License**: MIT (see the copyright notice at the top of `json.hpp`).
The MIT license text is compatible with the KataGo MIT license that covers
the rest of `goboard_transposition/`; no additional license file is
required at the parent level.

**Source URL**:
    https://github.com/nlohmann/json/releases/download/v3.11.3/json.hpp

**SHA-256** (for integrity verification):
    9bea4c8066ef4a1c206b2be5a36302f8926f7fdc6087af5d20b417d0cf103ea6

To verify locally:
    shasum -a 256 json.hpp

To update the vendored copy: download the new amalgamated header from
the upstream releases page, update this README's version/hash fields,
and commit the change as a single atomic commit with a `deps:` prefix.
