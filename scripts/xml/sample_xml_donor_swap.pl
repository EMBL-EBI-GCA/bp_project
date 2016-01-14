#!/usr/bin/env perl
use strict;
use warnings;
use XML::Twig;
use ReseqTrack::Tools::ERAUtils;
use utf8;
use autodie;
use Data::Dumper;
use Getopt::Long;

my ( $in_file, $era_user, $era_pass );

GetOptions( 'infile=s'   => \$in_file,
            'era_user=s' => \$era_user,
            'era_pass=s' => \$era_pass,
          );

die `perldoc -t $0`  unless $in_file && $era_user && $era_pass;

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;
my $sample_list = get_list( $in_file );

my ( $sample_alias, $gender, $subject_id_flag, $gender_flag );

my $twig = XML::Twig->new(
             twig_roots      => { 'SAMPLE' => 
                                     sub{ my ($twig, $e) = @_; 
                                          my $acc_id = $e->att('accession');
                                          my $ega_id = get_ega_id( $acc_id, 'sample', 'id' );
                                          $e->set_att( accession => $ega_id ); 
                                          $e->print(); 
                                       }
                                },
             twig_handlers   => { 'SAMPLE/IDENTIFIERS/PRIMARY_ID' =>
                                     sub{ my ($twig, $e) = @_;
                                          my $acc_id = $e->text;
                                          my $ega_id = get_ega_id( $acc_id, 'sample', 'id' );
                                          $e->set_text( $ega_id ); 
                                        },
                                  'SAMPLE/IDENTIFIERS/SUBMITTER_ID' =>
                                     sub{ my ($twig, $e) = @_;
                                          $sample_alias = $e->text;
                                        },
                                  'SAMPLE/SAMPLE_NAME/ANONYMIZED_NAME' => 
                                     sub{  my ($twig, $e) = @_; 
                                           my $donor_id = $e->text;
                                           die $sample_alias,$/ unless exists $$sample_list{ $sample_alias }{NEW_DONOR_ID};
                                           my $new_donor_id = $$sample_list{ $sample_alias }{NEW_DONOR_ID};
                                           $e->set_text( $new_donor_id );
                                        },
                                   'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="DONOR_ID"]' => 
                                      sub{ my ($twig, $e) = @_;
                                           die $sample_alias,$/ unless exists $$sample_list{ $sample_alias }{NEW_DONOR_ID};
                                           my $new_donor_id = $$sample_list{ $sample_alias }{NEW_DONOR_ID};
 
                                           foreach my $child ($e->children){
                                             if ( $child->name eq 'VALUE' ){
                                                my $value = $child->text;
                                                $child->set_text( $new_donor_id );
                                             }
                                           }
                                         },
                                     'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="DONOR_SEX"]' =>
                                        sub{ my ($twig, $e) = @_;
                                             foreach my $child ($e->children){
                                               $gender = $child->text
                                                 if ( $child->name eq 'VALUE' );
                                             }
                                           },
                                     'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="gender"]' =>
                                        sub{ my ($twig, $e) = @_;
                                             foreach my $child ($e->children){
                                               $gender_flag++
                                                 if ( $child->name eq 'VALUE' );
                                             }
                                           },
                                      'SAMPLE/SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE[string(TAG)="subject_id"]' =>                 
                                         sub{ my ($twig, $e) = @_;
                                             die $sample_alias,$/ unless exists $$sample_list{ $sample_alias }{NEW_DONOR_ID};
                                             my $new_donor_id = $$sample_list{ $sample_alias }{NEW_DONOR_ID};

                                             foreach my $child ($e->children){
                                               if ( $child->name eq 'VALUE' ){
                                                  my $value = $child->text;
                                                  $child->set_text( $new_donor_id );
                                                  $subject_id_flag++;
                                               }
                                             }
                                           },
                                       'SAMPLE/SAMPLE_ATTRIBUTES' => 
                                           sub{  my ( $twig, $e ) = @_;
                                                 die $sample_alias,$/ unless exists $$sample_list{ $sample_alias }{NEW_DONOR_ID};
                                                 my $new_donor_id = $$sample_list{ $sample_alias }{NEW_DONOR_ID};
                                                 if( $subject_id_flag  == 0 ){ 
                                                    my $a = XML::Twig::Elt->new( 'SAMPLE_ATTRIBUTE', );
                                                    my $t = XML::Twig::Elt->new( 'TAG', 'subject_id' );
                                                    my $v = XML::Twig::Elt->new( 'VALUE', $new_donor_id );
                                                    $t->move( first_child => $a );
                                                    $v->move( last_child  => $a );
                                                    $a->move( last_child  => $e );
                                                  }
                                   
                                                  if( $gender_flag  == 0 && $gender ){
                                                    my $a = XML::Twig::Elt->new( 'SAMPLE_ATTRIBUTE', );
                                                    my $t = XML::Twig::Elt->new( 'TAG', 'gender' );
                                                    my $v = XML::Twig::Elt->new( 'VALUE', $gender );
                                                    $t->move( first_child => $a );
                                                    $v->move( last_child  => $a );
                                                    $a->move( last_child  => $e );
                                                  } 
                                              },
                                     }, 
             pretty_print             => 'indented',
             keep_encoding            => 1,
             twig_print_outside_roots => 0,        # print the rest
           );


my $xml_sth = $era->dbc->prepare("select xmltype.getclobval(sample_xml) xml from sample where sample_alias = ? ");

print '<SAMPLE_SET>'.$/;
foreach my $sample ( keys %{ $sample_list } ) {    
  $xml_sth->execute( $sample );
  ($subject_id_flag,$gender_flag) = (0,0);
  ($sample_alias,$gender) = (undef,undef);
  my $xr = $xml_sth->fetchrow_arrayref();
  my ($xml) = @$xr;
  $subject_id_flag = 0;
  $twig->parse($xml);
}                    
print '</SAMPLE_SET>'.$/;  

sub get_ega_id {
  my ( $ena_id, $table, $type ) = @_;

  my $column_name = $table .'_'. $type;
  my $xml_sth = $era->dbc->prepare("select ega_id from $table where $column_name = ? ");
  $xml_sth->execute( $ena_id );
  my $xr = $xml_sth->fetchrow_arrayref();
  my $ega_id = $$xr[0];
  return $ega_id;
}

sub get_list {
  my ( $file ) = @_;
  my %id_hash;
  my @header;

  open my $fh ,'<', $file;
  while ( <$fh> ){
    chomp;
    next if /^#/;
    if( @header ){
      my @vals = split "\t";
      die "value missing in the list file: ",join("\t",@vals),$/ unless scalar @vals==2;

      my %val_hash;
      @val_hash{ @header } = @vals;
      my $new_donor_id  = $val_hash{NEW_DONOR_ID};
      my $sample_alias  = $val_hash{SAMPLE_ALIAS};
      
      foreach my $key ( @header ){
        die $sample_alias,$/ unless $val_hash{$key};
      }
      $id_hash{ $sample_alias }{NEW_DONOR_ID} = $new_donor_id;
    }
    else {
      @header = split "\t";
    }
  }
  close( $fh );
  return \%id_hash;
}   

=head1 

This script generates xml file for EGA sample entries with modified donor details (for dono id changes).

Options:

 -infile, tab-delimited file listing the sample alias, new donor ids must have headers

 e.g SAMPLE_ALIAS <TAB> NEW_DONOR_ID 

Parameters ERAPRO connection:

 -era_user, the name of the ERAPRO user
 -era_pass, the password for ERAPRO access

=head1 Examples

 Run it like this for the Blueprint project:

 perl sample_xml_donor_swap.pl -infile list_of_samples_for_donor_id_change -era_user $ERA_USER_NAME -era_pass $ERA_PASSWORD 

=cut

