#!/bin/bash

set -e
set -x

VERSION=ec2boot0.3

GOOS=linux GOARCH=amd64 go build -o /tmp/$VERSION .
cloudkey ti-apps/admin aws s3 cp --acl public-read /tmp/$VERSION s3://grail-public-bin/linux/amd64/$VERSION
