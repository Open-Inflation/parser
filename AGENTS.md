# AGENTS

Локальную документацию по парсерам нужно искать рядом с рабочими файлами в `.md`:

- общий контракт парсеров: `src/openinflation_parser/parsers/PARSER_PIPELINE.md`
- FixPrice: `src/openinflation_parser/parsers/fixprice/PIPELINE.md`
- Chizhik: `src/openinflation_parser/parsers/chizhik/PIPELINE.md`

Контракты upstream API нужно сверять по снапшотам соседних репозиториев:

- Chizhik API: `../chizhik_api/tests/__snapshots__/`
- FixPrice API: `../fixprice_api/tests/__snapshots__/`

Если меняется поведение `mapper.py`, `parser.py` или runtime-контракт парсера, нужно обновить соответствующий соседний `.md` в той же директории и при необходимости кратко синхронизировать корневой `README.md`.
