#Utility to check integrity of BAM files

use strict;
use warnings;
use List::MoreUtils qw(uniq);
use File::Basename;
use Getopt::Long;
use Parallel::ForkManager;
use Data::Dumper;

my $ifile;
my $base_dir;
my $bin_dir;
my $threads=1;
my $help;

my $usage = "
  [--help]          this menu
   --index          .index file containing the paths of the files to be analyzed,
   --base_dir       base_dir used to construct the paths
   --bin_dir        dir containing the binaries
   --threads        number of parallel processes that the script will create

   [USAGE] perl $0 --index /path/to/results.index --base_dir /nfs/1000g-work/ihec/drop/ --bin_dir /nfs/1000g-work/G1K/work/davidr/pipeline-deps/bin/
";

&GetOptions(
    'index=s'              => \$ifile,
    'base_dir=s'           => \$base_dir,
    'bin_dir=s'            => \$bin_dir,
    'threads=i'          => \$threads,
    'help'               => \$help
);

# Max 5 processes for parallel download
my $pm = new Parallel::ForkManager($threads); 

if ($help || !$ifile || !$base_dir || !$bin_dir) {
    print $usage;
    exit 0;
}

#array containing the FILE_TYPEs to be analized
my @valid_files=qw(
BS_BAM_CNAG
CHIP_DEDUP_BAM
CHIP_QUAL_FILTER_BAM
CHIP_RUN_BAM
CHIP_UNFILTER_BAM
DNASE_DEDUP_BAM
DNASE_QUAL_FILTER_BAM
DNASE_RUN_BAM
DNASE_UNFILTER_BAM
RNA_BAM_STAR_CRG
);

#restrict tests in ValidateSamFile.jar
my @restrict=qw(
INVALID_VERSION_NUMBER
INVALID_MAPPING_QUALITY
);

my $restrict_str=join",",@restrict;

my %valid = map { $_ => 1 } @valid_files;

open FILE,"<$ifile" or die("Cant open $ifile:$!\n");
my $header=<FILE>;
close FILE;
my @fields=split/\t/,$header;

#get indexes for useful columns
my $file_ix = List::MoreUtils::first_index {$_ eq 'FILE'} @fields;
my $file_type_ix = List::MoreUtils::first_index {$_ eq 'FILE_TYPE'} @fields;
$file_type_ix= List::MoreUtils::first_index {$_ eq 'type'} @fields if $file_type_ix==-1;

#parse file
my $files=parse_index($ifile);

my %results;

# data structure retrieval and handling
# called BEFORE the first call to start()
$pm -> run_on_finish (
    sub {
	my ($pid, $exit_code, $ident, $exit_signal, $core_dump, $data_structure_reference) = @_;
	
	# retrieve data structure from child
	if (defined($data_structure_reference)) {  # children are not forced to send anything
	    my $string = ${$data_structure_reference};  # child passed a string reference
	    my @lines=split/\n/,$string;
	    my $filename=shift(@lines);
	    $results{$filename}=\@lines;
	} else {  # problems occuring during storage or retrieval will throw a warning
	    print qq|No message received from child process $pid!\n|;
	}
    }
);

foreach my $file (@$files) {
    $pm->start and next; # do the fork
    print STDOUT "[INFO] processing $file\n";
    my $cmd="java -jar $bin_dir/ValidateSamFile.jar I=$base_dir/$file IGNORE={$restrict_str}";
    my $res=`$cmd`;
    my ($filename, $directories,$suffix) = fileparse($file,qr/\.[^.]*/);
    my $fline=$filename."\n".$res;
    $pm->finish(0,\$fline); 
}

$pm->wait_all_children;


#print results
print "#filename error\n";
foreach my $filename (keys %results) {
    my @lines=@{$results{$filename}};
    foreach my $l (@lines) {
	print "$filename\t$l\n";
    }
}



sub parse_index {
    my $file=shift;
    my @files;
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
	push @files,$filepath;
    }
    close FH;
    return \@files;
}
