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
my @valid=qw(BS_METH_CALL_CNAG BS_METH_SD_CNAG CS_FILTERED_BW CS_WIGGLER DS_FILTERED_BW DS_WIGGLER RNA_SIGNAL_CRG BS_HYPER_METH_BB_CNAG BS_HYPO_METH_BB_CNAG CS_MACS_TRACK_BB CS_MACS_TRACK_BB_WP10 CS_MACS_TRACK_BROAD_BB CS_MACS_TRACK_BROAD_BB_WP10 DNASE_TRACK_BB_NCMLS DS_HOTSPOT_TRACK_BB);

my %valid = map { $_ => 1 } @valid;


open FILE,"<$ifile" or die("Cant open $ifile:$!\n");
my $header=<FILE>;
close FILE;
my @fields=split/\t/,$header;

#get indexes for useful columns
my $file_ix = List::MoreUtils::first_index {$_ eq 'FILE'} @fields;
my $file_type_ix = List::MoreUtils::first_index {$_ eq 'FILE_TYPE'} @fields;

#header
print "#file\tchrNo\n";

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
	$cmd="$bin_dir/bigWigInfo $base_dir/bp-raw-data/$filepath";
    } elsif ($filepath=~/\.bb$/) {
	$cmd="$bin_dir/bigBedInfo $base_dir/bp-raw-data/$filepath";
    }
    #execute command
    my $res=`$cmd`;
    #get chromosome count from $res
    my $chr_count=$1 if $res=~/chromCount: (\d+)/;
    print $filepath,"\t",$chr_count,"\n";
}
close FH;

