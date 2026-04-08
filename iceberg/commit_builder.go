package iceberg

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/utils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var cbTracer = otel.Tracer("pg2iceberg/commit")

// ManifestGroup holds entries for a single manifest file (data or deletes).
type ManifestGroup struct {
	Entries []ManifestEntry
	Content int // 0 = data, 1 = deletes
}

// PendingData holds a file to upload (data, manifest, or manifest list).
type PendingData struct {
	Key  string
	Data []byte
}

// CommitBundle holds all assembled metadata and pending uploads for a snapshot
// commit. Built by BuildCommit, uploaded by UploadAll.
type CommitBundle struct {
	ManifestListURI string
	AllManifests    []ManifestFileInfo
	NewManifests    []ManifestFileInfo

	// Everything to upload in one parallel batch.
	Uploads []PendingData
}

// BuildCommitConfig holds the parameters for BuildCommit.
type BuildCommitConfig struct {
	S3                ObjectStorage
	Schema            *postgres.TableSchema
	PartSpec          *PartitionSpec
	BasePath          string
	SnapshotID        int64
	SeqNum            int64
	ExistingManifests []ManifestFileInfo
}

// BuildCommit assembles manifests and a manifest list from entry groups,
// computing all URIs upfront via URIForKey. Returns a CommitBundle whose
// Uploads slice can be uploaded in one parallel batch — no sequential
// dependencies between data files, manifests, and the manifest list.
func BuildCommit(cfg BuildCommitConfig, groups []ManifestGroup) (*CommitBundle, error) {
	var newManifests []ManifestFileInfo
	var uploads []PendingData

	for _, g := range groups {
		if len(g.Entries) == 0 {
			continue
		}
		manifestBytes, err := WriteManifest(cfg.Schema, g.Entries, cfg.SeqNum, g.Content, cfg.PartSpec)
		if err != nil {
			return nil, fmt.Errorf("write manifest (content=%d): %w", g.Content, err)
		}

		suffix := "data"
		if g.Content == 1 {
			suffix = "deletes"
		}
		key := fmt.Sprintf("%s/metadata/%s-mat-%s.avro", cfg.BasePath, uuid.New().String(), suffix)

		var totalRows int64
		for _, e := range g.Entries {
			totalRows += e.DataFile.RecordCount
		}
		newManifests = append(newManifests, ManifestFileInfo{
			Path:           cfg.S3.URIForKey(key),
			Length:         int64(len(manifestBytes)),
			Content:        g.Content,
			SnapshotID:     cfg.SnapshotID,
			AddedFiles:     len(g.Entries),
			AddedRows:      totalRows,
			SequenceNumber: cfg.SeqNum,
		})
		uploads = append(uploads, PendingData{Key: key, Data: manifestBytes})
	}

	allManifests := append(cfg.ExistingManifests, newManifests...)
	mlBytes, err := WriteManifestList(allManifests)
	if err != nil {
		return nil, fmt.Errorf("write manifest list: %w", err)
	}
	mlKey := fmt.Sprintf("%s/metadata/snap-%d-0-manifest-list.avro", cfg.BasePath, cfg.SnapshotID)
	uploads = append(uploads, PendingData{Key: mlKey, Data: mlBytes})

	return &CommitBundle{
		ManifestListURI: cfg.S3.URIForKey(mlKey),
		AllManifests:    allManifests,
		NewManifests:    newManifests,
		Uploads:         uploads,
	}, nil
}

// UploadAll uploads all pending data (data files + manifests + manifest list)
// in one parallel batch. Since all URIs are pre-computed, there are no
// sequential dependencies between uploads.
func UploadAll(ctx context.Context, s3 ObjectStorage, dataFiles []PendingData, bundle *CommitBundle, concurrency int) error {
	all := make([]PendingData, 0, len(dataFiles)+len(bundle.Uploads))
	all = append(all, dataFiles...)
	all = append(all, bundle.Uploads...)

	if len(all) == 0 {
		return nil
	}

	uploadCtx, span := cbTracer.Start(ctx, "pg2iceberg.upload", trace.WithAttributes(
		attribute.Int("data_files", len(dataFiles)),
		attribute.Int("meta_files", len(bundle.Uploads)),
	))
	defer span.End()

	tasks := make([]utils.Task, len(all))
	for i, f := range all {
		f := f
		tasks[i] = utils.Task{
			Name: f.Key,
			Fn: func(ctx context.Context, _ *utils.Progress) error {
				_, err := s3.Upload(ctx, f.Key, f.Data)
				return err
			},
		}
	}

	if concurrency <= 0 || concurrency > len(tasks) {
		concurrency = len(tasks)
	}
	pool := utils.NewPool(concurrency)
	if _, err := pool.Run(uploadCtx, tasks); err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	return nil
}
