use strict;
use warnings;
use ReseqTrack::Tools::ERAUtils;
use utf8;
use autodie;
use Data::Dumper;
use XML::Twig;
use Getopt::Long;

my ($analysis_file,$era_user,$era_pass);

GetOptions( 'analysis=s'     => \$analysis_file,
            'era_user=s'   => \$era_user,
            'era_pass=s'   => \$era_pass,
    );

die("[USAGE] perl $0 --analysis analysis_file.txt --era_user \$ERA_USER --era_pass \$ERA_PASS") if !$analysis_file || !$era_user || !$era_pass;

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

my $hash=parse_list($analysis_file);

my $twig = XML::Twig->new(
    pretty_print             => 'indented',
    twig_handlers =>
    { 
	'ANALYSIS/ANALYSIS_TYPE/REFERENCE_ALIGNMENT'=>\&add_new_element,
 }
    );

my $xml_sth = $era->dbc->prepare("select xmltype.getclobval(analysis_xml) xml from analysis where ega_id = ?");

foreach my $analysis_id ( keys %$hash ) {
    $xml_sth->execute( $analysis_id );
    my $xr = $xml_sth->fetchrow_arrayref();
    my ($xml) = @$xr;
    $twig->parse($xml);
    $twig->print;
}

sub add_new_element {
    my ($twig,$seq)=@_;
    my @list=$seq->children;
    foreach my $l (@list) {
	if ($l->name eq 'ASSEMBLY') {
	    my $standard=$l->first_child;
	    $standard->set_att('refname','GRCh37');
	    $standard->set_att('accession','GCA_000001405.14');
	} elsif ($l->name eq 'SEQUENCE') {
	    $l->delete;
	}
    }
    &insert_chr($seq);
}

sub insert_chr {
    my $seq=shift;

    my $chr = [
	{ label=>"chr1",accession=>"CM000663.1",},
	{ label=>"chr2",accession=>"CM000664.1",},
	{ label=>"chr3",accession=>"CM000665.1",},
	{ label=>"chr4",accession=>"CM000666.1",},
	{ label=>"chr5",accession=>"CM000667.1",},
	{ label=>"chr6",accession=>"CM000668.1",},
	{ label=>"chr7",accession=>"CM000669.1",},
	{ label=>"chr8",accession=>"CM000670.1",},
	{ label=>"chr9",accession=>"CM000671.1",},
	{ label=>"chr10",accession=>"CM000672.1",},
	{ label=>"chr11",accession=>"CM000673.1",},
	{ label=>"chr12",accession=>"CM000674.1",},
	{ label=>"chr13",accession=>"CM000675.1",},
	{ label=>"chr14",accession=>"CM000676.1",},
	{ label=>"chr15",accession=>"CM000677.1",},
	{ label=>"chr16",accession=>"CM000678.1",},
	{ label=>"chr17",accession=>"CM000679.1",},
	{ label=>"chr18",accession=>"CM000680.1",},
	{ label=>"chr19",accession=>"CM000681.1",},
	{ label=>"chr20",accession=>"CM000682.1",},
	{ label=>"chr21",accession=>"CM000683.1",},
	{ label=>"chr22",accession=>"CM000684.1",},
	{ label=>"chrX",accession=>"CM000685.1",},
	{ label=>"chrY",accession=>"CM000686.1",},
	{ label=>"chrM",accession=>"J01415.2",},
	];

    foreach my $c (@$chr) {

	my $seq2= new XML::Twig::Elt( 'SEQUENCE', $c);
	$seq2->paste( 'last_child', $seq );
    }
}

sub parse_list {
    my $file=shift;
    my %hash;
    open FH,"<$file" or die("Cant open $file:$!\n");
    while(<FH>) {
        chomp;
        my $line=$_;
        next if $line=~/^#/ || $line=~/^\s/ || $line=~/^\n/;
	$hash{$_}=0;
     }
    close FH;
    return \%hash;
}
