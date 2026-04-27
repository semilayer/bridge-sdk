# Changelog

## [1.2.0](https://github.com/semilayer/bridge-sdk/compare/bridge-firestore-v1.1.0...bridge-firestore-v1.2.0) (2026-04-27)


### Features

* **bridge-postgres:** default to IPv4-only DNS + 10s connect timeout ([e676785](https://github.com/semilayer/bridge-sdk/commit/e6767858c9839c6a5508e6ddf9b73f33b7efa1b7))
* **bridges:** aggregate facet across all 23 bridges ([a70ee78](https://github.com/semilayer/bridge-sdk/commit/a70ee781344295abcf55407d528103408c959bcc))
* **bridges:** aggregate facet across all 23 bridges ([d646f91](https://github.com/semilayer/bridge-sdk/commit/d646f91e9140a445ad878d3f52a8241ddd518224))


### Bug Fixes

* **bridge-mongodb:** coerce ISO date strings to Date in query() operators ([998fa71](https://github.com/semilayer/bridge-sdk/commit/998fa71c6ba3a3a1c683c9001459b31d69509684))
* **bridge-mysql:** pool.query for LIMIT + alias info_schema columns ([4b8b1db](https://github.com/semilayer/bridge-sdk/commit/4b8b1dbcd155f9dec223dfc04f8fcc88d7279ad1))
* **bridge-postgres:** handle schema-qualified table names ([76fe875](https://github.com/semilayer/bridge-sdk/commit/76fe87556565b7a05df57936b8008046cbb9e42b))
* **bridge-postgres:** pre-resolve DNS to enforce ipFamily ([c3a6828](https://github.com/semilayer/bridge-sdk/commit/c3a68280878db3d748cf22ebec366545c0a40bf5))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @semilayer/bridge-sdk bumped to 1.3.0

## [1.1.0](https://github.com/semilayer/bridge-sdk/compare/bridge-firestore-v1.0.1...bridge-firestore-v1.1.0) (2026-04-18)


### Features

* **bridges:** batchRead + capabilities across every adapter (Gate A.1) ([85d08af](https://github.com/semilayer/bridge-sdk/commit/85d08afc92b9c3687426e2c60327ec1799c23139))


### Dependencies

* The following workspace dependencies were updated
  * devDependencies
    * @semilayer/bridge-sdk bumped to 1.2.0

## [1.0.1](https://github.com/semilayer/bridge-sdk/compare/bridge-firestore-v1.0.0...bridge-firestore-v1.0.1) (2026-04-12)


### Dependencies

* The following workspace dependencies were updated
  * devDependencies
    * @semilayer/bridge-sdk bumped to 1.1.0
