#!/bin/bash

set -e
set -x

GOOS=linux GOARCH=amd64 go build -o /tmp/ec2boot0.1 .
cloudkey eng/dev aws s3 cp --acl public-read /tmp/ec2boot0.1 s3://grail-bin/linux/amd64/ec2boot0.1
