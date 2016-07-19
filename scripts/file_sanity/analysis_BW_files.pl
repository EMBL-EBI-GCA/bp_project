#Utility to report the number of chros and what chros we have in bigWig
#and bigBed files

use strict;
use warnings;
use List::MoreUtils qw(uniq);
use Getopt::Long;

my $ifile;
my $base_dir;
my $bin_dir;
my $help;

my $usage = "
  [--help]          this menu
   --index          .index file containing the paths of the files to be analyzed,
   --base_dir       base_dir used to construct the paths
   --bin_dir        dir containing the binaries 

   [USAGE] perl $0 --index /path/to/results.index --base_dir /nfs/1000g-work/ihec/drop/ --bin_dir /nfs/1000g-work/G1K/work/davidr/pipeline-deps/bin/
";

&GetOptions(
    'index=s'              => \$ifile,
    'base_dir=s'           => \$base_dir,
    'bin_dir=s'            => \$bin_dir,
    'help'               => \$help
);

if ($help || !$ifile || !$base_dir || !$bin_dir) {
    print $usage;
    exit 0;
}

#array containing the FILE_TYPEs to be analized
my @valid_files=qw(
CHIP_WIGGLER
DNASE_WIGGLER
RNA_SIGNAL_STAR_CRG
BS_METH_CALL_CNAG
BS_METH_COV_CNAG
);

#array containing chros to be checked
my @check_chros= qw(chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10 chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20 chr21 chr22 chrX);

my %valid = map { $_ => 1 } @valid_files;


open FILE,"<$ifile" or die("Cant open $ifile:$!\n");
my $header=<FILE>;
close FILE;
my @fields=split/\t/,$header;

#get indexes for useful columns
my $file_ix = List::MoreUtils::first_index {$_ eq 'FILE'} @fields;
my $file_type_ix = List::MoreUtils::first_index {$_ eq 'FILE_TYPE'} @fields;

my %count_chros;

#header
print "#file\tchrNo\tmissing\ttype\n";

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
    my $cmd="$bin_dir/bigWigInfo -chroms $base_dir/bp-raw-data/$filepath";
    #execute command
    my $res=`$cmd`;
    #parse output
    my @lines=split/\s/,$res;
    my %chros= map { $_ => 1 } (grep {/chr/} @lines);
    my $missing_chr="";
    #check if this file has all the chros in @check_chros
    foreach my $chr (@check_chros) {
	if (!exists($chros{$chr})) {
	    $missing_chr.="$chr,";
	    $count_chros{$fields[$file_type_ix]}{$chr}++;
	}
    }
    $missing_chr=~s/,$//;
    $missing_chr="OK" if $missing_chr eq "";
    my $number=scalar(keys(%chros));
    print "$filepath\t$number\t$missing_chr\t$fields[$file_type_ix]\n";
}
close FH;

open OUTFH,">report_chros.bw.txt";
print OUTFH "\n##Number of files missing a certain chromosome:\n";
print OUTFH "#type\tchr\tnumber\n";
foreach my $ftype (keys %count_chros) {
    my @sorted_chros=sort {$count_chros{$ftype}{$b}<=>$count_chros{$ftype}{$a}} keys %{$count_chros{$ftype}};
    foreach my $chr (@sorted_chros) {
	print OUTFH "$ftype\t$chr\t$count_chros{$ftype}{$chr}\n";
    }
}

close OUTFH;
