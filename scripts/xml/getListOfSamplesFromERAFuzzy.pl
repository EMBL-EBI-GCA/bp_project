# Script used to retrieve a xml file with the samples that are already in ERAPRO for
# donors from the samples that are trying to be registered.

use strict;
use warnings;
use ReseqTrack::Tools::ERAUtils;
use utf8;
use autodie;
use Data::Dumper;
use XML::Twig;
use Getopt::Long;

my ($sample_file,$era_user,$era_pass);

GetOptions( 'samples=s'     => \$sample_file,
            'era_user=s'   => \$era_user,
            'era_pass=s'   => \$era_pass,
          );

die("[USAGE] perl $0 --samples sample_file.txt --era_user \$ERA_USER --era_pass \$ERA_PASS 1> samples_registered.xml 2> sample_info.txt") if !$sample_file || !$era_user || !$era_pass;


my ($sample_id_list,$sample_dict) = get_list($sample_file);

my @era_conn = ( 'ops$laura', 'thousandgenomes' );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

my $twig = XML::Twig->new(
    twig_roots => 
    { 'SAMPLE'=>\&process_sample },
    pretty_print             => 'indented',
    );

my $xml_sth = $era->dbc->prepare("select xmltype.getclobval(sample_xml) xml from sample where sample_alias like ?");

foreach my $sample_id ( @{ $sample_id_list } ) {
    $xml_sth->execute( $sample_id );
    my $xr = $xml_sth->fetchrow_arrayref();
    if (!$xr) {
	print STDERR "[WARN] No info for $sample_id $sample_dict->{$sample_id}\n";
	next;
    } else {
	print STDERR "[WARN] Sample registered for same donor for $sample_id $sample_dict->{$sample_id}\n";
    }
    my ($xml) = @$xr;
    $twig->parse($xml);
}

sub process_sample {
    my ($twig,$sample)=@_;
    $sample->print();
}

sub get_list{
    my ($file) = @_;
    my @id_list;

    my %mapping;
    open my $fh ,'<', $file;
    while ( <$fh> ) {
	chomp;
	my $id=$_;
	$mapping{$id}=
	my $length=length($id);
	my $substring=substr($id,0,$length-2);
	$substring.='%';
	$mapping{$substring}=$id;
	push (@id_list, $substring);
    }
    close( $fh );
    return (\@id_list,\%mapping);
}
