# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project follows
[Semantic Versioning](https://semver.org/).

## [0.4.0] - 2026-06-30

### Changed
- Updated dependencies, notably **Aspire.Hosting 9 → 13**. Consumers of
  `AlmostServiceBus.Aspire.Hosting` must move their Aspire stack to 13.

### Fixed
- Emulator now rejects a receiver on a second top-level entity over a
  cross-entity-transaction connection, matching real Azure Service Bus
  ("Local transactions cannot span multiple top-level entities").

### Security
- Resolved a high-severity `MessagePack` advisory (pulled transitively via
  `Aspire.Hosting`).
