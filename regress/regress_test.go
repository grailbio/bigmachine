// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package regress

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRegress(t *testing.T) {
	t.Parallel()
	tests, err := filepath.Glob("tests/*/*")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command("go", "run", test)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("%s\n%s", err, string(out))
			}
		})
	}
}
