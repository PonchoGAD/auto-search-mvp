#!/bin/bash
docker compose -f infra/compose.yml exec api python -c "from data_pipeline.index import run_index; run_index()"
