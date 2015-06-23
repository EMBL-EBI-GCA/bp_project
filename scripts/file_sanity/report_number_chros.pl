#Utility to report the number of chros in bigWig and bigBed files

use strict;
use warnings;
use List::MoreUtils qw(uniq);
use Getopt::Long;

my $ifile="/nfs/1000g-work/ihec/drop/bp-raw-data/blueprint/results.index";
my $base_dir="/nfs/1000g-work/ihec/drop/";
my $bin_dir="/nfs/1000g-work/G1K/work/davidr/pipeline-deps/bin/";
my $help;

my $usage = "
  [--help]          this menu
   --index          .index file containing the paths of the files to be analyzed,
   --base_dir       base_dir used to construct the paths
   --bin_dir        dir containing the binaries 

   [USAGE] perl report_number_chros.pl --index /path/to/results.index --base_dir /nfs/1000g-work/ihec/drop/ --bin_dir /nfs/1000g-work/G1K/work/davidr/pipeline-deps/bin/
";

&GetOptions(
    'index=s'              => \$ifile,
    'base_dir=s'           => \$base_dir,
    'bin_dir=s'            => \$bin_dir
);

if ($help) {
    print $usage;
    exit 0;
}

#array containing the FILE_TYPEs to be analized
my @valid_files=qw(CHIP_MACS2_BROAD_BB CHIP_WIGGLER CHIP_MACS2_BB);

#array containing chros to be considered for a file to be valid
my @valid_chros= qw(chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10 chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20 chr21 chr22 chrX chrY chrM);

my %valid = map { $_ => 1 } @valid_files;


open FILE,"<$ifile" or die("Cant open $ifile:$!\n");
my $header=<FILE>;
close FILE;
my @fields=split/\t/,$header;

#get indexes for useful columns
my $file_ix = List::MoreUtils::first_index {$_ eq 'FILE'} @fields;
my $file_type_ix = List::MoreUtils::first_index {$_ eq 'FILE_TYPE'} @fields;

#header
print "#file\tchrNo\tvalid\n";

my $first=0;
open FH,"<$ifile" or die("Cant open $ifile:$!\n");
while(<FH>) {
    chomp;
    if ($first==0) {
	$first=1;
	next;
    }
    my $line=$_;
    my @fields=split/\t/,$line;
    #check if this FILE_TYPE is declared in @valid
    next if !(exists($valid{$fields[$file_type_ix]}));
    my $filepath=$fields[$file_ix];
    my $cmd;
    #check if file is .bw or .bb
    if ($filepath=~/\.bw$/) {
	#create command
	$cmd="$bin_dir/bigWigInfo -chroms $base_dir/bp-raw-data/$filepath";
    } elsif ($filepath=~/\.bb$/) {
	$cmd="$bin_dir/bigBedInfo -chroms $base_dir/bp-raw-data/$filepath";
    }
    #execute command
    my $res=`$cmd`;
    #parse output
    my $valid=1;
    my @lines=split/\s/,$res;
    my %chros= map { $_ => 1 } (grep {/chr/} @lines);
    #check if this file has all the chros in @valid_chros
    foreach my $chr (@valid_chros) {
	if (!exists($chros{$chr})) {
	    $valid=0;
	}
    }
    my $number=scalar(keys(%chros));
    print "$filepath\t$number\t$valid\n";
}
close FH;

