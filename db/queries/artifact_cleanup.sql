-- name: IsArtifactMultipartUploadRecorded :one
SELECT EXISTS (
    SELECT 1
    FROM artifact_uploads AS upload
    JOIN artifacts AS artifact ON artifact.id = upload.artifact_id
    WHERE artifact.object_key = sqlc.arg(object_key)
      AND upload.multipart_upload_id = sqlc.arg(multipart_upload_id)
);
