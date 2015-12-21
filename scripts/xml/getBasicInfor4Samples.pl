#Script that receives a list of sample aliases and fetches from ERAPRO the basic 
#information for each sample. This information can be used to ask for metadata confirmation to the biosample providers.
#Example of the output:
#S005FH41	genomic DNA	Primary Cell	Acute Myeloid Leukemia
#S005FHA1	genomic DNA	Primary Cell	Acute Myeloid Leukemia
#S005EJ11	polyA RNA	Primary cell	Acute Myeloid Leukemia
#...

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

die("[USAGE] perl $0 --samples sample_file.txt --era_user \$ERA_USER --era_pass \$ERA_PASS") if !$sample_file || !$era_user || !$era_pass;

my $sample_id_list = get_list($sample_file);

my @era_conn = ( $era_user,$era_pass );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

my $xml_sth = $era->dbc->prepare("select s.sample_alias,sx.molecule,sx.biomaterial,sx.disease,sx.tissue_type,sx.cell_type from sample s,
xmltable('/SAMPLE_SET/SAMPLE' passing s.sample_xml
columns
     molecule varchar2(512) path '//SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG[text()=\"MOLECULE\"]]/VALUE',
          biomaterial varchar2(512) path '//SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG[text()=\"BIOMATERIAL_TYPE\"]]/VALUE',
          disease varchar2(512) path '//SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG[text()=\"DISEASE\"]]/VALUE',
          cell_type varchar2(512) path '//SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG[text()=\"CELL_TYPE\"]]/VALUE',
          tissue_type varchar2(512) path '//SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[TAG[text()=\"TISSUE_TYPE\"]]/VALUE'
)sx
where
s.sample_alias like ?");

print "#ID\tmolecule\tbiomaterial\tdisease\n";

foreach my $sample_id ( @{ $sample_id_list } ) {
    $xml_sth->execute( $sample_id );
    my $xr = $xml_sth->fetchrow_arrayref();
    print $xr->[0],"\t",$xr->[1],"\t",$xr->[2],"\t",$xr->[3],"\n";

}

sub get_list{
  my ($file) = @_;
  my @id_list;

  open my $fh ,'<', $file;
  while ( <$fh> ) {
    chomp;
    push (@id_list, $_);
  }
  close( $fh );
  return \@id_list;
}
