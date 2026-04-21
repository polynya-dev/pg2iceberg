package iceberg

import "strconv"

// Standard Iceberg snapshot summary keys.
// https://iceberg.apache.org/spec/#snapshot-summary
const (
	SummaryOperation            = "operation"
	SummaryAddedDataFiles       = "added-data-files"
	SummaryAddedRecords         = "added-records"
	SummaryAddedFilesSize       = "added-files-size"
	SummaryDeletedDataFiles     = "deleted-data-files"
	SummaryDeletedRecords       = "deleted-records"
	SummaryRemovedFilesSize     = "removed-files-size"
	SummaryAddedDeleteFiles     = "added-delete-files"
	SummaryAddedEqualityDeletes = "added-equality-deletes"
	SummaryRemovedDeleteFiles   = "removed-delete-files"
	SummaryTotalDataFiles       = "total-data-files"
	SummaryTotalRecords         = "total-records"
	SummaryTotalFilesSize       = "total-files-size"
	SummaryTotalDeleteFiles     = "total-delete-files"
	SummaryTotalEqualityDeletes = "total-equality-deletes"
)

// SummaryDelta captures how a commit changes the table's physical footprint.
// Positive fields = added, negative semantics are represented by the Removed
// counterparts. Used to compute post-commit totals from the previous
// snapshot's summary.
type SummaryDelta struct {
	// Data files (content=0).
	AddedDataFiles  int64
	AddedRecords    int64
	AddedFilesSize  int64
	RemovedDataFiles int64
	RemovedRecords   int64
	RemovedFilesSize int64

	// Delete files (content=1/2).
	AddedDeleteFiles     int64
	AddedEqualityDeletes int64
	RemovedDeleteFiles   int64
	RemovedEqualityDeletes int64
}

// BuildSummary returns a snapshot summary map populated with standard Iceberg
// keys. Totals carry forward from prevSummary (if present) and apply the
// delta. Zero-valued fields are omitted to keep summaries compact.
func BuildSummary(operation string, prevSummary map[string]string, delta SummaryDelta) map[string]string {
	summary := map[string]string{SummaryOperation: operation}

	setNonZero := func(key string, v int64) {
		if v != 0 {
			summary[key] = strconv.FormatInt(v, 10)
		}
	}

	setNonZero(SummaryAddedDataFiles, delta.AddedDataFiles)
	setNonZero(SummaryAddedRecords, delta.AddedRecords)
	setNonZero(SummaryAddedFilesSize, delta.AddedFilesSize)
	setNonZero(SummaryDeletedDataFiles, delta.RemovedDataFiles)
	setNonZero(SummaryDeletedRecords, delta.RemovedRecords)
	setNonZero(SummaryRemovedFilesSize, delta.RemovedFilesSize)
	setNonZero(SummaryAddedDeleteFiles, delta.AddedDeleteFiles)
	setNonZero(SummaryAddedEqualityDeletes, delta.AddedEqualityDeletes)
	setNonZero(SummaryRemovedDeleteFiles, delta.RemovedDeleteFiles)

	// Totals: carry forward from prev summary + delta.
	prev := parsePrevSummary(prevSummary)
	totalDataFiles := prev.totalDataFiles + delta.AddedDataFiles - delta.RemovedDataFiles
	totalRecords := prev.totalRecords + delta.AddedRecords - delta.RemovedRecords
	totalFilesSize := prev.totalFilesSize + delta.AddedFilesSize - delta.RemovedFilesSize
	totalDeleteFiles := prev.totalDeleteFiles + delta.AddedDeleteFiles - delta.RemovedDeleteFiles
	totalEqualityDeletes := prev.totalEqualityDeletes + delta.AddedEqualityDeletes - delta.RemovedEqualityDeletes

	// Always emit totals (including 0) — consumers expect these to always be present.
	summary[SummaryTotalDataFiles] = strconv.FormatInt(totalDataFiles, 10)
	summary[SummaryTotalRecords] = strconv.FormatInt(totalRecords, 10)
	summary[SummaryTotalFilesSize] = strconv.FormatInt(totalFilesSize, 10)
	summary[SummaryTotalDeleteFiles] = strconv.FormatInt(totalDeleteFiles, 10)
	summary[SummaryTotalEqualityDeletes] = strconv.FormatInt(totalEqualityDeletes, 10)

	return summary
}

type prevTotals struct {
	totalDataFiles       int64
	totalRecords         int64
	totalFilesSize       int64
	totalDeleteFiles     int64
	totalEqualityDeletes int64
}

func parsePrevSummary(s map[string]string) prevTotals {
	if s == nil {
		return prevTotals{}
	}
	parse := func(k string) int64 {
		if v, ok := s[k]; ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				return n
			}
		}
		return 0
	}
	return prevTotals{
		totalDataFiles:       parse(SummaryTotalDataFiles),
		totalRecords:         parse(SummaryTotalRecords),
		totalFilesSize:       parse(SummaryTotalFilesSize),
		totalDeleteFiles:     parse(SummaryTotalDeleteFiles),
		totalEqualityDeletes: parse(SummaryTotalEqualityDeletes),
	}
}

// PrevSnapshotSummary returns the summary map of the snapshot with the given
// ID, or nil if not found.
func PrevSnapshotSummary(tm *TableMetadata, snapshotID int64) map[string]string {
	if tm == nil || snapshotID == 0 {
		return nil
	}
	for _, s := range tm.Metadata.Snapshots {
		if s.SnapshotID == snapshotID {
			return s.Summary
		}
	}
	return nil
}
