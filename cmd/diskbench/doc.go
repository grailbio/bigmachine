/*
diskbench runs disk benchmarks on a bigmachine machine.  A single bigmachine
machine is started on which benchmarks are run, and results are reported to
stdout.  Configuration is read from $HOME/grail/profile and command-line flags.

Here are current results:

Configuration:
	ami = "ami-1ee65166" // Ubuntu 16.04
	instance = "m5.2xlarge"
	dataspace = 256

Results:
	$TMPDIR is /mnt/data
	===
	=== benchmark run 1 of 3
	===
	= writing and reading with dd
	1024+0 records in
	1024+0 records out
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 4.99248 s, 215 MB/s
	1024+0 records in
	1024+0 records out
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 0.158806 s, 6.8 GB/s
	= running hdparm -Tt

	/dev/nvme1n1:
	 Timing cached reads:   17766 MB in  2.00 seconds = 8904.34 MB/sec
	 Timing buffered disk reads: 876 MB in  3.00 seconds = 291.65 MB/sec
	===
	=== benchmark run 2 of 3
	===
	= writing and reading with dd
	1024+0 records in
	1024+0 records out
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 5.22627 s, 205 MB/s
	1024+0 records in
	1024+0 records out
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 0.153792 s, 7.0 GB/s
	= running hdparm -Tt

	/dev/nvme1n1:
	 Timing cached reads:   17054 MB in  2.00 seconds = 8548.11 MB/sec
	 Timing buffered disk reads: 556 MB in  3.00 seconds = 185.23 MB/sec
	===
	=== benchmark run 3 of 3
	===
	= writing and reading with dd
	1024+0 records in
	1024+0 records out
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 4.31686 s, 249 MB/s
	1024+0 records in
	1024+0 records out
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 0.153069 s, 7.0 GB/s
	= running hdparm -Tt

	/dev/nvme1n1:
	 Timing cached reads:   17566 MB in  1.99 seconds = 8805.41 MB/sec
	 Timing buffered disk reads: 444 MB in  3.00 seconds = 147.95 MB/sec
*/
package main
