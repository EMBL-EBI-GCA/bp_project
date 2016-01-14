#utility used to parse the log file produced by webin-data-streamer-Upload-Client.jar tool (Webin tool used to encrypt files).
#It generates a tabular file: Filename<\t>encrypted_md5<\t>unencrypted_md5

use strict;
use warnings;
use File::Basename;

my $ifile=$ARGV[0];

die("[USAGE] perl $0 conversion_log_12_Jan_2016_16_51_36.log!\n") if !$ifile;

my ($unencrypted_md5,$filename);

open FH,"<$ifile" or die("Cannot open $ifile:$!\n");
while(<FH>) {
    chomp;
    my $line=$_;
    if ($line=~/^Converted file/) {
	my @elms=split/\t/,$line;
	my ($a,$fpath)=split/:/,$elms[0];
	($a,$unencrypted_md5)=split/:/,$elms[1];
	$unencrypted_md5=~s/ //;
	my $directories;
	($filename, $directories) = fileparse($fpath);
	$filename=~s/.$//;
    } elsif ($line=~/PGP Encrypted MD5:/) {
	my ($a,$encrypted_md5)=split/:/,$line;
	print "$filename\t$encrypted_md5\t$unencrypted_md5\n";
    }
}
close FH;
