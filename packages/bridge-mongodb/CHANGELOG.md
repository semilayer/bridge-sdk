# Changelog

## [1.3.0](https://github.com/semilayer/bridge-sdk/compare/bridge-mongodb-v1.2.0...bridge-mongodb-v1.3.0) (2026-04-29)


### Features

* **bridge-sdk:** add logical/string where operators + count(where) + UnsupportedOperatorError ([21e4d4f](https://github.com/semilayer/bridge-sdk/commit/21e4d4fe81fbb2698e6487d521cee9616b9cc26d))
* **bridges:** roll out logical/string where ops + count(where) across all bridges ([32325ae](https://github.com/semilayer/bridge-sdk/commit/32325aeda71313eaaa066795023ccace9e085b81))
* logical/string where ops + count(where) — Bridge interface extension across all bridges ([97617e3](https://github.com/semilayer/bridge-sdk/commit/97617e3cba8e0e481ab26584951c7b2adee2a463))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @semilayer/bridge-sdk bumped to 1.4.0

## [1.2.0](https://github.com/semilayer/bridge-sdk/compare/bridge-mongodb-v1.1.1...bridge-mongodb-v1.2.0) (2026-04-27)


### Features

* **bridges:** aggregate facet across all 23 bridges ([a70ee78](https://github.com/semilayer/bridge-sdk/commit/a70ee781344295abcf55407d528103408c959bcc))
* **bridges:** aggregate facet across all 23 bridges ([d646f91](https://github.com/semilayer/bridge-sdk/commit/d646f91e9140a445ad878d3f52a8241ddd518224))


### Bug Fixes

* **bridges:** clear lint warnings — drop unused formatTimeBucket import in mongo aggregate, drop stale prefer-const disable in duckdb, mark unused ts arg in sqlite firstLast ([4e25afc](https://github.com/semilayer/bridge-sdk/commit/4e25afc7d1087f108bcf8c0efc7ab913511f5f97))
* **bridges:** nip CI integration failures ([aeff643](https://github.com/semilayer/bridge-sdk/commit/aeff6432051926fc3b1903497de1ddcc571b9efd))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @semilayer/bridge-sdk bumped to 1.3.0

## [1.1.1](https://github.com/semilayer/bridge-sdk/compare/bridge-mongodb-v1.1.0...bridge-mongodb-v1.1.1) (2026-04-25)


### Bug Fixes

* **bridge-mongodb:** coerce ISO date strings to Date in query() operators ([998fa71](https://github.com/semilayer/bridge-sdk/commit/998fa71c6ba3a3a1c683c9001459b31d69509684))
* **bridge-mongodb:** coerce ISO date strings to Date in query() operators ([fce25cf](https://github.com/semilayer/bridge-sdk/commit/fce25cffbaa4057e27e25ed804903eaf846794c3))

## [1.1.0](https://github.com/semilayer/bridge-sdk/compare/bridge-mongodb-v1.0.1...bridge-mongodb-v1.1.0) (2026-04-18)


### Features

* **bridges:** batchRead + capabilities across every adapter (Gate A.1) ([85d08af](https://github.com/semilayer/bridge-sdk/commit/85d08afc92b9c3687426e2c60327ec1799c23139))


### Dependencies

* The following workspace dependencies were updated
  * devDependencies
    * @semilayer/bridge-sdk bumped to 1.2.0

## [1.0.1](https://github.com/semilayer/bridge-sdk/compare/bridge-mongodb-v1.0.0...bridge-mongodb-v1.0.1) (2026-04-12)


### Dependencies

* The following workspace dependencies were updated
  * devDependencies
    * @semilayer/bridge-sdk bumped to 1.1.0
