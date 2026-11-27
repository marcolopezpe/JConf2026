#!/bin/bash
# Vacia SOLO vehicle_telemetry antes de una corrida.
# NO toca el topic (los eventos siguen en Redpanda) ni benchmark_run (conserva el historial).
docker exec postgres psql -U postgres -d db_vehicle_telemetry -c "TRUNCATE TABLE vehicle_telemetry;"