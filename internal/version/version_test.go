package version

import "testing"

func TestCurrentAndString(t *testing.T) {
	oldVersion, oldCommit, oldDate := Version, Commit, Date
	t.Cleanup(func() {
		Version, Commit, Date = oldVersion, oldCommit, oldDate
	})

	Version = "v0.1.0"
	Commit = "abc1234"
	Date = "2026-09-22T00:00:00Z"

	info := Current()
	if info.Version != Version || info.Commit != Commit || info.Date != Date {
		t.Fatalf("Current() = %#v, want build variables", info)
	}

	want := "open-aspm v0.1.0 (commit=abc1234, date=2026-09-22T00:00:00Z)"
	if got := info.String(); got != want {
		t.Fatalf("Info.String() = %q, want %q", got, want)
	}
}
