# Script to check integrity of .bed, .bb and MAC'S xls files
# For each file, it will check what chrs have peaks and its numbers

use strict;
use warnings;
use List::MoreUtils qw(uniq);
use Getopt::Long;
use File::Basename;
use Data::Dumper;

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

#array containing the FILE_TYPEs to be analized. Only .bed/.bed.gz/xls.gz,/.bb are permitted
my @valid_files=qw(
CHIP_MACS2_BROAD_BED
CHIP_MACS2_BED
CHIP_MACS2_BB
CHIP_MACS2_BROAD_BB
DNASE_HOTSPOT_BB
DNASE_HOTSPOT_BED
);
my %valid = map { $_ => 1 } @valid_files;

open FILE,"<$ifile" or die("Cant open $ifile:$!\n");
my $header=<FILE>;
close FILE;
my @fields=split/\t/,$header;

#get indexes for useful columns
my $file_ix = List::MoreUtils::first_index {$_ eq 'FILE'} @fields;
my $file_type_ix = List::MoreUtils::first_index {$_ eq 'FILE_TYPE'} @fields;

my ($file_hash,$type_hash)=parse_index($ifile);

my %chr_per_type; #hash containing the number of files having a certain chr for each file type

print "#path\ttype\tstatus\n";

foreach my $key (keys %$file_hash) {
    my @files=@{$file_hash->{$key}};
    my (%first_peak_counts,%peak_counts); #will store info on number of peaks per chr
    foreach my $fpath (@files) {
	my $success="OK";
	my $ftype=$type_hash->{$fpath}; #file type for this $fpath
	my $res;
	#get filename and suffix from $filepath
	my($filename, $directories,$suffix) = fileparse($fpath,qr/\.[^.]*/);
	if ($suffix eq '.bb') {
	    #this cmd will count the number of peaks per chro
	    my $cmd="$bin_dir/bigBedToBed $base_dir/bp-raw-data/$fpath /dev/stdout |cut -f1 |sort |uniq -c";
	    $res=`$cmd`;
	} elsif ($suffix eq '.gz') {   
	    my $cmd="zcat $base_dir/bp-raw-data/$fpath |cut -f1 |sort |uniq -c";
	    $res=`$cmd`;
	}
	$res=~s/^\s+//g;
	#sometimes, files have not results. Skip/report those cases
	if ($res!~/chr.+/) {
	    print "$fpath\t$ftype\tEMPTY\n";
	    next;
	}
	if (!%first_peak_counts) {
	    #initialize %first_peak_counts
	    %first_peak_counts=%{populate_hash($res,$ftype)};
	} else {
	    %peak_counts=%{populate_hash($res,$ftype)};
	    $success=compare_2_hashes(\%first_peak_counts,\%peak_counts,$ftype);
	}
	print "$fpath\t$ftype\t$success\n";
    }
}

open OUTFH,">report_chros.bed.txt";
print OUTFH "#type\tchr\tnumber\n";
#print out number of files having a certain chr\n";
foreach my $type (keys %chr_per_type) {
    foreach my $chr (keys %{$chr_per_type{$type}}) {
	print OUTFH $type,"\t",$chr,"\t",$chr_per_type{$type}{$chr},"\n";
    }
}
close OUTFH;

sub compare_2_hashes {
    my ($hash1,$hash2,$ftype)=@_;
    for ( keys %$hash1 ) {
	if (!exists($hash2->{$_})) {
	    return 'NOT_OK';
	    next;
	}
	if ($ftype eq 'DNASE_HOTSPOT_PEAK_BED') {
	    #this file type does not contain the same number of peaks than the rest
	    #of DNAse file types. This means, that only chr existence is checked
	    return 'OK';
	} else {
	    if ( $hash1->{$_} != $hash2->{$_} ) {
		return 'NOT_OK';
	    }
	}
    }
    return 'OK';
}

sub populate_hash {
    my ($res,$ftype)=@_;
    my %hash;
    my @lines=split/\n/,$res;
    foreach my $l (@lines) {
	next unless $l=~/chr.+/;
	my ($number,$chr)=($1,$2) if $l=~/(\d+) (chr.+)/;
	$hash{$chr}=$number;
	$chr_per_type{$ftype}{$chr}++;
    }
    return \%hash;
}

sub parse_index {
    my $file=shift;
    my (%hash,%files);
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
	#get filename and suffix from $filepath
	my($filename, $directories,$suffix) = fileparse($filepath,qr/\.[^.]*/);
	$files{$filepath}=$fields[$file_type_ix];
	#construct key based on sample id and experiment id
	my @bits=split/\./,$filename;
	my $key=$bits[0]."_".$bits[1];
	push @{$hash{$key}},$filepath;
    }
    close FH;
    return (\%hash,\%files);
}
