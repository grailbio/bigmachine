// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2system

import (
	"bytes"
	"testing"
)

func TestCloudconfig(t *testing.T) {
	var c cloudConfig
	c.Flavor = CoreOS
	c.CoreOS.Update.RebootStrategy = "off"
	c.AppendFile(CloudFile{"/tmp/x", "0644", "root", "a test file"})
	c.AppendUnit(CloudUnit{Name: "reflowlet", Command: "command", Enable: true, Content: "unit content"})
	var d cloudConfig
	d.AppendUnit(CloudUnit{Name: "xxx", Command: "xxxcommand", Enable: false, Content: "xxx content"})
	d.AppendFile(CloudFile{"/tmp/myfile", "0644", "root", "another test file"})
	c.Merge(&d)
	out, err := c.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := out, []byte(`#cloud-config
write_files:
- path: /tmp/x
  permissions: "0644"
  owner: root
  content: a test file
- path: /tmp/myfile
  permissions: "0644"
  owner: root
  content: another test file
coreos:
  update:
    reboot-strategy: "off"
  units:
  - name: reflowlet
    command: command
    enable: true
    content: unit content
  - name: xxx
    command: xxxcommand
    content: xxx content
`); !bytes.Equal(got, want) {
		t.Errorf("got %s, want %s", got, want)
	}
}
