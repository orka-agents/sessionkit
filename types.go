package sessionkit

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/jsonl"
	"github.com/orka-agents/sessionkit/internal/model"
)

type Harness = model.Harness
type Profile = model.Profile

const Codex = model.Codex
const CodexPaginated = model.CodexPaginated

type Source = model.Source

// Budget bounds all reads, parsed records/nodes, temporary bytes and elapsed time
// across an operation. Zero fields select defaults; negative limits are invalid.
type Budget = model.Budget
type Inspection = model.Inspection
type RecordSummary = model.RecordSummary
type CompactionSummary = model.CompactionSummary
type Warning = model.Warning
type Rejection = model.Rejection
type RejectionError = model.RejectionError
type ActiveWriterError = model.ActiveWriterError
type CollisionError = model.CollisionError
type BudgetError = model.BudgetError
type IntegrityError = model.IntegrityError
type UnknownOutcomeError = model.UnknownOutcomeError

type CaptureOptions struct {
	BundleDir string
	Budget    Budget
}
type Component struct {
	LogicalPath string `json:"logicalPath"`
	Role        string `json:"role"`
	SizeBytes   int64  `json:"sizeBytes"`
	SHA256      string `json:"sha256"`
}
type Manifest struct {
	BundleFormat          int         `json:"bundleFormat"`
	Harness               Harness     `json:"harness"`
	Profile               Profile     `json:"profile"`
	AdapterVersion        string      `json:"adapterVersion"`
	SourceCLIVersion      string      `json:"sourceCLIVersion"`
	ThreadID              string      `json:"threadID"`
	SourceRelativePath    string      `json:"sourceRelativePath"`
	RecordedCWD           string      `json:"recordedCWD"`
	LatestCWD             string      `json:"latestCWD"`
	RuntimeWorkspaceRoots []string    `json:"runtimeWorkspaceRoots"`
	ModelProvider         string      `json:"modelProvider"`
	Omitted               []string    `json:"omitted"`
	CreatedAt             string      `json:"createdAt"`
	Components            []Component `json:"components"`
	Inspection            Inspection  `json:"inspection"`
}
type Bundle struct {
	Dir      string
	Manifest Manifest
}
type Destination struct {
	Harness    Harness `json:"harness"`
	Root       string  `json:"root"`
	WorkingDir string  `json:"workingDir"`
	JournalDir string  `json:"journalDir"`
}
type ResumeHints struct {
	CodexHome             string         `json:"codexHome"`
	CWDOverride           string         `json:"cwdOverride"`
	RuntimeWorkspaceRoots []string       `json:"runtimeWorkspaceRoots"`
	RecordedProvider      string         `json:"recordedProvider"`
	SQLiteNote            string         `json:"sqliteNote"`
	NativeCommand         []string       `json:"nativeCommand"`
	AppServerParams       map[string]any `json:"appServerParams"`
}

// Plan is a preview bound to verified bundle bytes and a destination. Its exported
// fields are informational. Install rejects an altered plan. JSON serialization
// retains the bundle and destination binding for retries after a process restart.
type Plan struct {
	OperationID  string      `json:"operationID"`
	BundleDigest string      `json:"bundleDigest"`
	Profile      Profile     `json:"profile"`
	ThreadID     string      `json:"threadID"`
	TargetPath   string      `json:"targetPath"`
	ResumeHints  ResumeHints `json:"resumeHints"`
	Omitted      []string    `json:"omitted"`
	Preserved    []Component `json:"preserved"`
	bundleDir    string
	destination  Destination
	seal         string
}
type Outcome string

const (
	RejectedBeforeMutation Outcome = "RejectedBeforeMutation"
	Installed              Outcome = "Installed"
	Unknown                Outcome = "Unknown"
)

type Receipt struct {
	OperationID  string  `json:"operationID"`
	Outcome      Outcome `json:"outcome"`
	TargetPath   string  `json:"targetPath"`
	TargetDigest string  `json:"targetDigest"`
	Phase        string  `json:"phase"`
}
type Verification struct {
	Valid        bool
	TargetPath   string
	TargetDigest string
	SizeBytes    int64
}

type planFields Plan
type planEnvelope struct {
	planFields
	BundleDir   string      `json:"bundleDir"`
	Destination Destination `json:"destination"`
	PlanDigest  string      `json:"planDigest"`
}

func (p Plan) MarshalJSON() ([]byte, error) {
	return json.Marshal(planEnvelope{planFields: planFields(p), BundleDir: p.bundleDir, Destination: p.destination, PlanDigest: p.seal})
}
func (p *Plan) UnmarshalJSON(data []byte) error {
	if len(data) > 1<<20 {
		return &BudgetError{Limit: "plan bytes"}
	}
	if _, err := jsonl.Decode(data, budget.New(context.Background(), Budget{MaxLineBytes: 1 << 20})); err != nil {
		return err
	}
	var wire planEnvelope
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&wire); err != nil {
		return &IntegrityError{Component: "plan", Reason: "invalid schema"}
	}
	decoded := Plan(wire.planFields)
	decoded.bundleDir = wire.BundleDir
	decoded.destination = wire.Destination
	decoded.seal = wire.PlanDigest
	if decoded.seal == "" || planSeal(decoded) != decoded.seal {
		return &IntegrityError{Component: "plan", Reason: "plan digest mismatch"}
	}
	*p = decoded
	return nil
}
