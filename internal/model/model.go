// Package model defines the shared inspection contract used by adapters.
package model

import (
	"fmt"
	"time"
)

type Harness string
type Profile string

const Codex Harness = "codex"
const CodexPaginated Profile = "codex/paginated@0.160.0"

type Source struct {
	Harness  Harness
	Root     string
	ThreadID string
}
type Budget struct {
	MaxBytes     int64
	MaxLineBytes int64
	MaxRecords   int64
	MaxDepth     int
	MaxNodes     int64
	MaxTempBytes int64
	Timeout      time.Duration
}
type Warning struct {
	Code      string `json:"code"`
	Component string `json:"component,omitempty"`
	Ordinal   uint64 `json:"ordinal,omitempty"`
	Message   string `json:"message"`
}
type Rejection struct {
	Code      string `json:"code"`
	Component string `json:"component,omitempty"`
	Ordinal   uint64 `json:"ordinal,omitempty"`
	Message   string `json:"message"`
}
type RecordSummary struct {
	Total         int64            `json:"total"`
	ByType        map[string]int64 `json:"byType"`
	ResponseItems map[string]int64 `json:"responseItems"`
	FirstOrdinal  uint64           `json:"firstOrdinal"`
	LastOrdinal   uint64           `json:"lastOrdinal"`
	OrdinalGaps   int64            `json:"ordinalGaps"`
	ToolCalls     int64            `json:"toolCalls"`
	ToolOutputs   int64            `json:"toolOutputs"`
	ToolPairs     int64            `json:"toolPairs"`
}
type CompactionSummary struct {
	Count                         int64   `json:"count"`
	NewestComplete                bool    `json:"newestComplete"`
	NewestCompleteBoundaryOrdinal *uint64 `json:"newestCompleteBoundaryOrdinal,omitempty"`
}
type Inspection struct {
	Profile               Profile           `json:"profile"`
	ThreadID              string            `json:"threadID"`
	CLIVersion            string            `json:"cliVersion"`
	HistoryMode           string            `json:"historyMode"`
	RecordedCWD           string            `json:"recordedCWD"`
	LatestCWD             string            `json:"latestCWD"`
	RuntimeWorkspaceRoots []string          `json:"runtimeWorkspaceRoots"`
	ModelProvider         string            `json:"modelProvider"`
	Records               RecordSummary     `json:"records"`
	Compaction            CompactionSummary `json:"compaction"`
	Omitted               []string          `json:"omitted"`
	Warnings              []Warning         `json:"warnings"`
	Rejections            []Rejection       `json:"rejections"`
	SourceDigest          string            `json:"sourceDigest"`
	SourceSizeBytes       int64             `json:"sourceSizeBytes"`
}
type RejectionError struct{ Rejections []Rejection }

func (e *RejectionError) Error() string {
	if len(e.Rejections) == 0 {
		return "session rejected"
	}
	return "session rejected: " + e.Rejections[0].Code + ": " + e.Rejections[0].Message
}

type ActiveWriterError struct{ ThreadID string }

func (e *ActiveWriterError) Error() string { return "thread has an active writer: " + e.ThreadID }

type CollisionError struct {
	ThreadID   string
	TargetPath string
}

func (e *CollisionError) Error() string { return "destination already contains thread " + e.ThreadID }

type BudgetError struct{ Limit string }

func (e *BudgetError) Error() string { return "operation budget exceeded: " + e.Limit }

type IntegrityError struct {
	Component string
	Reason    string
}

func (e *IntegrityError) Error() string {
	return fmt.Sprintf("integrity check failed for %s: %s", e.Component, e.Reason)
}

type UnknownOutcomeError struct {
	OperationID string
	Err         error
}

func (e *UnknownOutcomeError) Error() string {
	return "install outcome unknown for " + e.OperationID + ": " + e.Err.Error()
}
func (e *UnknownOutcomeError) Unwrap() error { return e.Err }
