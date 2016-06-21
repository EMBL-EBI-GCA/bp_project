#!/usr/bin/env perl
use strict;
use warnings;
use XML::Twig;
use ReseqTrack::Tools::ERAUtils;
use utf8;
use autodie;
use Data::Dumper;
use Getopt::Long;

my ( $in_file, $table_name, $id_type, $era_user, $era_pass );
my $center_name = 'BLUEPRINT';

GetOptions( 'infile=s'     => \$in_file,
            'table_name=s' => \$table_name,
            'id_type=s'    => \$id_type,
            'era_user=s'   => \$era_user,
            'era_pass=s'   => \$era_pass,
          );

die `perldoc -t $0` if !$in_file || !$table_name || !$id_type || !$era_user || !$era_pass;

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

my %cv_table = map{ $_ => 1 } qw/ sample
                                  experiment
                                  run
                                  analysis
                                /;

my $id_list = get_list( $in_file );

my $twig_roo_string = uc($table_name);

my $twig = XML::Twig->new(
             twig_roots      => { $twig_roo_string => 
                                     sub{ my ($twig, $e) = @_; 
                                          $e->print(); 
                                       }},
             pretty_print             => 'indented',
             keep_encoding            => 1,
             twig_print_outside_roots => 0,        # print the rest
           );


die "ID type $id_type not supported" 
    unless $id_type eq 'alias' || $id_type eq 'id';

die "table $table_name not supported"
    unless exists $cv_table{ $table_name };

my $db_string = 'select xmltype.getclobval(' . $table_name . '_xml) xml from ' . $table_name . ' where ' . $table_name . '_' . $id_type . '= ? and center_name = ?'  ; 
my $xml_sth = $era->dbc->prepare( $db_string );


$table_name = uc($table_name);

print "<$table_name\_SET>".$/;

foreach my $id ( @$id_list ) {    
  $xml_sth->execute( $id, $center_name );
  while( my ($xml) = $xml_sth->fetchrow_array()){
    $twig->parse($xml);
  }
}
                    
print "</$table_name\_SET>".$/;  



sub get_list {
  my ( $in_file ) = @_;
  my @list;

  open my $fh,'<', $in_file;
  while( <$fh> ){
    chomp;
    next if /^#/;
   
    my @vals = split "\t";
    my $column_count = scalar @vals;

    die "one column expected in $in_file, found $column_count"
      if $column_count > 1;
    
    push @list, $vals[0];
  }
  close( $fh );
  return \@list;
} 

=head1 Description

Script for fetching XML content from ERAPRO 

=head1 Options

  --infile      : lists of unique ids or aliases
  --table_name  : ERAPRO table name (supported tables: sample, experiment, run, analysis)
  --id_type     : types of the ids listed in infile (supported type: id or alias)
  --era_user    : ERAPRO user name
  --era_pass    : ERAPRO password

=head1 Examples

Run it like this for the Blueprint project:

  perl dump_erapro_xml.pl  -infile FILE -id_type TYPE -table_name TABLE_NAME -era_user USER -era_pass PASSWORD

=cut

