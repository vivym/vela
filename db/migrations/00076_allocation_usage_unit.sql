-- +goose Up
-- Authority occupancy is one exclusive WorkerInstance allocation, independent
-- of device count. It is not a hardware utilization or CPU process-time sample.
ALTER TYPE usage_resource_kind ADD VALUE IF NOT EXISTS 'ALLOCATION_NANOSECOND';

-- +goose Down
-- PostgreSQL cannot remove an enum label without rewriting dependent history.
-- Keep this additive label when rolling back the runtime receipt producer.
SELECT 1;
