WITH expected(stable_id) AS (
    VALUES
        ('h3-encoder-lab-v2'),
        ('h3-dit-lab-v2'),
        ('h3-vae-lab-v2'),
        ('h3-cpu-thumbnail-lab-v2')
)
SELECT count(*)
FROM expected
WHERE EXISTS (
    SELECT 1
    FROM capacity_pools AS pool
    JOIN model_runtime_capacity_routes AS route
      ON route.capacity_pool_id = pool.id
     AND route.stage_profile_revision_id = pool.stage_profile_revision_id
    JOIN stage_profile_revisions AS profile
      ON profile.id = route.stage_profile_revision_id
    JOIN worker_instances AS worker
      ON worker.id = route.worker_instance_id
    JOIN model_residencies AS residency
      ON residency.id = route.model_residency_id
     AND residency.worker_instance_id = worker.id
     AND residency.worker_instance_epoch = worker.instance_epoch
     AND residency.model_component_revision = profile.model_component_revision
    WHERE pool.stable_id = expected.stable_id
      AND pool.state = 'ACTIVE'
      AND worker.lifecycle_state = 'READY'
      AND worker.reachability_state = 'CONNECTED'
      AND residency.state = 'READY'
      AND EXISTS (
          SELECT 1
          FROM capacity_observations AS observation
          WHERE observation.worker_instance_id = worker.id
            AND observation.worker_instance_epoch = worker.instance_epoch
            AND observation.expires_at > statement_timestamp()
            AND observation.capacity_vector ->> 'concurrency' ~ '^[1-9][0-9]*$'
            AND NOT EXISTS (
                SELECT 1
                FROM capacity_observations AS newer
                WHERE newer.worker_instance_id = observation.worker_instance_id
                  AND newer.worker_instance_epoch = observation.worker_instance_epoch
                  AND newer.observation_sequence > observation.observation_sequence
            )
      )
      AND vela_worker_instance_authority_matches(
          worker.id,
          worker.instance_epoch,
          worker.device_set_digest,
          worker.membership_digest,
          residency.id,
          residency.model_runtime_epoch
      )
);
