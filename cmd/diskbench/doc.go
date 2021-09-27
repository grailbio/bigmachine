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
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 1.41673 s, 758 MB/s
	1024+0 records in
	1024+0 records out
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 0.168252 s, 6.4 GB/s
	= running hdparm -Tt

	/dev/md0:
	 Timing cached reads:   16824 MB in  2.00 seconds = 8432.85 MB/sec
	 Timing buffered disk reads: 2176 MB in  3.00 seconds = 724.15 MB/sec
	===
	=== benchmark run 2 of 3
	===
	= writing and reading with dd
	1024+0 records in
	1024+0 records out
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 1.68509 s, 637 MB/s
	1024+0 records in
	1024+0 records out
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 0.16413 s, 6.5 GB/s
	= running hdparm -Tt

	/dev/md0:
	 Timing cached reads:   18140 MB in  1.99 seconds = 9093.72 MB/sec
	 Timing buffered disk reads: 2172 MB in  3.01 seconds = 722.48 MB/sec
	===
	=== benchmark run 3 of 3
	===
	= writing and reading with dd
	1024+0 records in
	1024+0 records out
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 1.69384 s, 634 MB/s
	1024+0 records in
	1024+0 records out
	1073741824 bytes (1.1 GB, 1.0 GiB) copied, 0.16817 s, 6.4 GB/s
	= running hdparm -Tt

	/dev/md0:
	 Timing cached reads:   16820 MB in  2.00 seconds = 8430.45 MB/sec
	 Timing buffered disk reads: 2178 MB in  3.00 seconds = 725.04 MB/sec
*/
package main
