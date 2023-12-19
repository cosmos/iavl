package iavl_test

import (
	"strings"
	"testing"

	"github.com/cosmos/iavl/v2"
)

func Test_versionRange(t *testing.T) {
	cases := []struct {
		name     string
		versions []int64
		find     int64
		want     int64
		wantErr  string
	}{
		{
			name:     "naive",
			versions: []int64{1, 2, 3, 4, 5},
			find:     3,
			want:     3,
		},
		{
			name:     "unordered",
			versions: []int64{5, 3},
			wantErr:  "unordered insert: version 3 is not greater than 5",
		},
		{
			name:     "typical",
			versions: []int64{1, 2, 10},
			find:     3,
			want:     2,
		},
		{
			name:     "past last",
			versions: []int64{1, 2, 10},
			find:     11,
			want:     10,
		},
		{
			name:     "before start",
			versions: []int64{5, 10},
			find:     3,
			want:     -1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &iavl.VersionRange{}
			var addErr error
			for _, v := range tc.versions {
				addErr = r.Add(v)
				if addErr != nil {
					if tc.wantErr == "" {
						t.Fatalf("unexpected error: %v", addErr)
					}
					if !strings.Contains(addErr.Error(), tc.wantErr) {
						t.Fatalf("want error %q, got %v", tc.wantErr, addErr)
					} else {
						return
					}
				}
			}
			if addErr == nil && tc.wantErr != "" {
				t.Fatalf("want error %q, got nil", tc.wantErr)
			}
			got := r.Find(tc.find)
			if got != tc.want {
				t.Fatalf("want %d, got %d", tc.want, got)
			}
		})
	}
}
