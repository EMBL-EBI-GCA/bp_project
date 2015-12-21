# script to parse *.index files to get list for sequencing centers on the samples for which we data
use strict;
use warnings;
use Getopt::Long;
use List::MoreUtils qw(uniq);

my ($help,$index,$dir,$wp10);

my $usage = "
  [--help]          this menu
   --dir            dir containing *.index files,
   --wp10           Boolean (1 or 0). Generate list of samples either for wp10 or for the entire BP. Default=0 (Entire BP)

   [USAGE] perl getWP10sampleStatus.pl --dir /path/to/*.index --wp10 1 
";

GetOptions('help' => \$help,
	   'dir:s' => \$dir,
	   'wp10:i' => \$wp10
);

$wp10=0 if !$wp10;

if ($help || !$dir) {
    print $usage;
    exit 0;
}

my $hash=parseDir($dir);

foreach my $center (keys %$hash) {
    my $outfile;
    if ($wp10==0) {$outfile="samplestatus.$center.tsv"} else {$outfile="wp10samplestatus.$center.tsv"};
    open OUTFH,">$outfile";
    #header
    print OUTFH "#sample\tlibr_str\texp_type\n";
    foreach my $sample (keys %{$hash->{$center}}) {
	my @lines=@{$hash->{$center}->{$sample}};
	# remove duplicated lines in @samples
	my @uniq_lines=uniq @lines;
	foreach my $l (@uniq_lines) {
	    print OUTFH "$l\n";
	}
   }
    close OUTFH;
} 

# function to parse index file and return a hash with necessary information
sub parseDir {
    my $idir=shift;

    opendir DH, $idir or die "Cannot open $idir: $!";
    my @ixfiles;
    #get all *.index files from $idir
    if ($wp10==0) {
	#index files to parse
	@ixfiles=qw(array_data.index fastq_files.index alignments.index results.index);
    } else {
	#index files to parse
	@ixfiles=qw(wp10_array_data.index wp10_fastq_files.index wp10_alignments.index wp10_results.index);
    }
    closedir DH;

    my %hash;

    foreach my $ifile (@ixfiles) {

	open FILE,"<$idir/$ifile" or die("Cant open $idir/$ifile:$!\n");
	my $header=<FILE>;
	close FILE;
	my @fields=split/\t/,$header;
    
	#get indexes for useful columns
	my $sample_ix = List::MoreUtils::first_index {$_ eq 'SAMPLE_NAME'} @fields;
	my $libr_str_ix = List::MoreUtils::first_index {$_ eq 'LIBRARY_STRATEGY'} @fields;
	my $exptype_ix = List::MoreUtils::first_index {$_ eq 'EXPERIMENT_TYPE'} @fields;
	my $center_ix = List::MoreUtils::first_index {$_ eq 'CENTER_NAME'} @fields;
	my $file_ix = List::MoreUtils::first_index {$_ eq 'FILE'} @fields;
    
	#parse ix file
	my $first=0;
	open FH,"<$idir/$ifile" or die("Cant open $idir/$ifile:$!\n");
	while(<FH>) {
	    chomp;
	    if ($first==0) {
		$first=1;
		next;
	    }
	    my $line=$_;
	    my @elms=split/\t/,$line;

	    # McGill has some upper-case letters, this if-else unify this
	    my $center;
	    if ($elms[$center_ix]=~/McGill/i) {
		$center="McGill";
	    } else {
		$center=$elms[$center_ix];
	    }

	    my $libr_str;
	    if ($libr_str_ix==-1) {$libr_str="n.a."} else {$libr_str=$elms[$libr_str_ix]};

	    push @{$hash{$center}{$elms[$sample_ix]}},"$elms[$sample_ix]\t$libr_str\t$elms[$exptype_ix]";

	}
	close FH;
    }
    return \%hash;
}
