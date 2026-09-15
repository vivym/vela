-- +goose Up
-- A new OutputSpec revision opts in; existing certified outputs retain their contract.
ALTER TABLE output_specs
    ADD COLUMN media_contract text NOT NULL DEFAULT 'exact-video-v1'
        CHECK (media_contract IN ('exact-video-v1', 'h3-native-av-v1')),
    ADD CONSTRAINT output_specs_h3_native_media CHECK (
        media_contract <> 'h3-native-av-v1' OR (
            frame_rate_milli = 24000 AND codec = 'h264' AND container = 'mp4'
            AND duration_milliseconds BETWEEN 4000 AND 15000
            AND (duration_milliseconds::bigint * frame_rate_milli) % 1000000 = 0
        )
    );

-- +goose Down
ALTER TABLE output_specs DROP CONSTRAINT output_specs_h3_native_media;
ALTER TABLE output_specs DROP COLUMN media_contract;
