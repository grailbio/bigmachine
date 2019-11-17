# Bigmachine: implementation notes

This document contains a set of notes about the design choices
in the Bigmachine implementation.
It is a work in progress.

  * [Bootstrapping](#bootstrapping)

## Bootstrapping

Bigmachine manages machines started by the user.
The machines are provisioned by a cloud provider (1, 2, 3),
and then must be bootstrapped to run the user's code.
Machines boot with a simple bootstrap process:
its only task is to receive a binary and execute it.
The driver process uploads itself
(or the [fatbin](https://godoc.org/github.com/grailbio/base/fatbin)
binary that corresponds to the target GOOS/GOARCH combination) (4),
which is then run by the bootstrap server (5).
Finally, when the server has started,
the Bigmachine driver maintains keepalives
to the server (6).

![the Bigmachine bootstrap process](https://raw.github.com/grailbio/bigmachine/master/docs/bootstrap.svg?sanitize=true)

When the Bigmachine driver process fails to maintain its keepalive
(for example, because it was shut down, it crashed, or experienced a network partition),
the target machine shuts itself down.
