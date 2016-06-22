#!/usr/bin/env perl
use strict;
use warnings;
use autodie;
use ReseqTrack::Tools::ERAUtils;
use XML::Writer;
use XML::Twig;
use IO::File;
use Data::Dumper;
use Getopt::Long;

my $report_file;
my $debug        = 0;
my $era_user;
my $era_pass;
my $output_xml;

GetOptions( 'report_file=s' => \$report_file,
            'era_user=s'    => \$era_user,
            'era_pass=s'    => \$era_pass,
            'output_xml=s'  => \$output_xml,
            'debug!'        => \$debug,
          );
die `perldoc -t $0` 
  if ( !$report_file || !$era_user || !$era_pass || !$output_xml);

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

my $metadata = read_metadata( $report_file, $debug );
my @samples;

foreach my $sample ( keys %$metadata ){
  my $sample_metadata = load_known_samples( $era, $sample );
  my $issue_count = 0;

  unless (exists ($$sample_metadata{$sample})){                    ## skip samples which doesn't match EGA search
    warn "not found sample: $sample",$/;
    next;
  }

  ATTR:
  foreach my $attr (keys %{$$metadata{$sample}}){
    
    if ( $attr eq 'gender' ){
      unless (exists ($$sample_metadata{$sample}{attributes}{DONOR_SEX})){
        warn "no DONOR_SEX: $sample, can't add gender",$/; 
        $issue_count++; 
        next ATTR;
      }
      my $gender = lc($$sample_metadata{$sample}{attributes}{DONOR_SEX});

      $gender = 'unknown' if $gender ne 'male'   &&
                             $gender ne 'female';                   ## controlled term for EGA

      $$sample_metadata{$sample}{attributes}{gender} = $gender;
    }
    elsif ( $attr eq 'phenotype'){
      next ATTR 
        if exists $$sample_metadata{$sample}{attributes}{PHENOTYPE}; ## nothing to do, rely of EGA auto conversion

      unless (exists ($$sample_metadata{$sample}{attributes}{SAMPLE_ONTOLOGY_URI})){
        warn "no SAMPLE_ONTOLOGY_URI: $sample, can't add phenotype",$/;
        $issue_count++;
        next ATTR;
      }
      my $sample_uri = $$sample_metadata{$sample}{attributes}{SAMPLE_ONTOLOGY_URI};
      my ($sample_ontology) = ( $sample_uri =~ /.*\/(\S+)$/);

      my $disease_ontology = 'PATO_0000461';                         ## healthy sample ontology
      my $disease = $$sample_metadata{$sample}{attributes}{DISEASE};

      unless ( $disease =~ /^(None|NA)$/i ){
        unless (exists ($$sample_metadata{$sample}{attributes}{DISEASE_ONTOLOGY_URI})){
          warn "no DISEASE_ONTOLOGY_URI: $sample, disease: $disease",$/;
          $issue_count++;
          next ATTR; 
        }
        my $disease_uri = $$sample_metadata{$sample}{attributes}{DISEASE_ONTOLOGY_URI}; 
        ($disease_ontology) = ( $disease_uri =~ /.*code=(\S+)$/);
      }

      my $phenotype = $sample_ontology .'; '. $disease_ontology;
      $$sample_metadata{$sample}{attributes}{phenotype} = $phenotype;
    }
    elsif ( $attr eq 'donor_id' ){
      unless (exists ($$sample_metadata{$sample}{attributes}{donor_id}) ||
              exists ($$sample_metadata{$sample}{attributes}{DONOR_ID})){
        my $donor_id = exists $$sample_metadata{$sample}{attributes}{POOL_ID} ?
                              $$sample_metadata{$sample}{attributes}{POOL_ID} : '';
        if ( $donor_id && $donor_id ne ''){
          $$sample_metadata{$sample}{attributes}{donor_id} = $donor_id;
        }
        else {
          warn "no donor_id: $sample",$/;
          $issue_count++;
          next ATTR;
        }
      }
    }
    elsif ( $attr eq 'DONOR_AGE' ){
      my $age;
      if (exists ($$sample_metadata{$sample}{attributes}{DONOR_AGE})){
        $age = $$sample_metadata{$sample}{attributes}{DONOR_AGE};
        unless ($age eq 'NA'){
          $age = reformat_age($age);
        }
      }
      else {
        warn "no DONOR_AGE: $sample, adding age: NA",$/;
        $age = 'NA';        
      }
      $$sample_metadata{$sample}{attributes}{DONOR_AGE} = $age;
    }
    elsif ( $attr eq 'DISEASE' ){
      unless (exists ($$sample_metadata{$sample}{attributes}{DISEASE})){
        warn "no CELL_TYPE: $sample",$/;
        $issue_count++;
        next ATTR;
      }
      my $disease = $$sample_metadata{$sample}{attributes}{DISEASE};
      $disease = join '', map { ucfirst lc } split /(\s+)/, $disease
                    unless $disease eq 'NA';

      $$sample_metadata{$sample}{attributes}{DISEASE} = $disease;
    }
    elsif ( $attr eq 'DONOR_HEALTH_STATUS' ){
      my $health_status = exists $$sample_metadata{$sample}{attributes}{DONOR_HEALTH_STATUS} ?
                                 $$sample_metadata{$sample}{attributes}{DONOR_HEALTH_STATUS} : 'NA';

      my $disease       = exists $$sample_metadata{$sample}{attributes}{DISEASE} ?
                                 $$sample_metadata{$sample}{attributes}{DISEASE} : 'NA';

      my $new_health_status = $health_status;

      if ( $disease =~ /^(None|NA)$/i ){
        $new_health_status = 'Healthy' if $new_health_status =~ /Healthy/i;
      }
      else {
        $disease           = join '', map { ucfirst lc } split /(\s+)/, $disease;
        $new_health_status = join '', map { ucfirst lc } split /(\s+)/, $new_health_status;
        $new_health_status = $disease
          unless $new_health_status eq $disease;
      }
      warn "no DONOR_HEALTH_STATUS: $sample, set to: $new_health_status, from:$health_status",$/;
      $$sample_metadata{$sample}{attributes}{DONOR_HEALTH_STATUS} = $new_health_status;
    }
    elsif ( $attr eq 'DISEASE_ONTOLOGY_URI' ){
      unless (exists ($$sample_metadata{$sample}{attributes}{DISEASE_ONTOLOGY_URI})){
        my $disease = $$sample_metadata{$sample}{attributes}{DISEASE};
        unless ( $disease =~ /^(None|NA)$/i ){
          warn "no DISEASE_ONTOLOGY_URI: $sample, for disease: $disease",$/;
          $issue_count++;
          next ATTR;
        }
      }
    }
    elsif ( $attr eq 'SAMPLE_ONTOLOGY_URI' ){
      unless (exists ($$sample_metadata{$sample}{attributes}{SAMPLE_ONTOLOGY_URI})){
        my ($cell_type, $tissue);
        $cell_type = exists $$sample_metadata{$sample}{attributes}{CELL_TYPE} ?
                            $$sample_metadata{$sample}{attributes}{CELL_TYPE} : 'NA' ;
        $tissue    = exists $$sample_metadata{$sample}{attributes}{TISSUE_TYPE} ?
                            $$sample_metadata{$sample}{attributes}{TISSUE_TYPE} : 'NA' ;
        warn "no SAMPLE_ONTOLOGY_URI: $sample, for cell_type: $cell_type, tissue: $tissue",$/;
        $issue_count++;
        next ATTR;
      }
    }
    elsif ( $attr eq 'CELL_TYPE' ){
      unless (exists ($$sample_metadata{$sample}{attributes}{CELL_TYPE})){
        warn "no CELL_TYPE: $sample",$/;
        $issue_count++;
        next ATTR;
      }
  
      my $cell_type = $$sample_metadata{$sample}{attributes}{CELL_TYPE};
      unless ( $cell_type =~ /CD\d+/){
        $$sample_metadata{$sample}{attributes}{CELL_TYPE} = $cell_type;
        if ( $cell_type =~ s/\b(B|T)\b(\s|-)(\S+)/uc($1).$2.$3/ie ){
          warn "Changing CELL_TYPE: ", $$sample_metadata{$sample}{attributes}{CELL_TYPE}," to: $cell_type",$/; 
        }
      }
      $$sample_metadata{$sample}{attributes}{CELL_TYPE} = $cell_type;
    }
    elsif ( $attr eq 'TISSUE_TYPE' ){
      unless (exists ($$sample_metadata{$sample}{attributes}{TISSUE_TYPE})){
        warn "no TISSUE_TYPE: $sample",$/;  
        $issue_count++;
        next ATTR;
      }
      my $tissue_type = $$sample_metadata{$sample}{attributes}{TISSUE_TYPE};
      $tissue_type = lc($tissue_type);
      $tissue_type = 'venous blood'
        if $tissue_type eq 'peripheral blood';
      $$sample_metadata{$sample}{attributes}{TISSUE_TYPE} = $tissue_type;
    }
    elsif ( $attr eq 'BIOMATERIAL_TYPE' ){
      unless (exists ($$sample_metadata{$sample}{attributes}{BIOMATERIAL_TYPE})){
        warn "no BIOMATERIAL_TYPE: $sample",$/;
        $issue_count++;
        next ATTR;
      }
      my $type = $$sample_metadata{$sample}{attributes}{BIOMATERIAL_TYPE};
      $type = join '', map { ucfirst lc } split /(\s+)/, $type;
      $$sample_metadata{$sample}{attributes}{BIOMATERIAL_TYPE} = $type;
    } 
    elsif ( $attr eq 'BIOMATERIAL_PROVIDER' ){
      unless (exists ($$sample_metadata{$sample}{attributes}{BIOMATERIAL_PROVIDER})){
        warn "no BIOMATERIAL_PROVIDER: $sample",$/;
        $issue_count++;
        next ATTR;
      }
      my $provider = $$sample_metadata{$sample}{attributes}{BIOMATERIAL_PROVIDER};
      $provider =~ s/"//g;
      $$sample_metadata{$sample}{attributes}{BIOMATERIAL_PROVIDER} = $provider;
    }
    elsif ( $attr eq 'MARKERS' ){
      my $marker;
      if (exists ($$sample_metadata{$sample}{attributes}{MARKERS})){
        $marker = $$sample_metadata{$sample}{attributes}{MARKERS};
      }
      else {
        warn "no MARKERS: $sample, adding marker: NA",$/;
        $marker = 'NA';
        
      }
      $marker =~ s/"//g;
      $$sample_metadata{$sample}{attributes}{MARKERS} = $marker;
    }
    else {
      warn "Skipping $attr: $sample",$/
        if $debug;
    }
  }

  if ( $issue_count > 0 ){
    warn "Not adding sample: $sample, issues found: $issue_count",$/;
    next;
  }
  else {
    push @samples, $sample_metadata;
  }
}

output_sample_xml( $output_xml, \@samples);

sub get_ega_id {
  my ( $ena, $ena_id, $table, $type ) = @_;

  my $column_name = $table .'_'. $type;
  my $xml_sth = $ena->dbc->prepare("select ega_id from $table where $column_name = ? ");
  $xml_sth->execute( $ena_id );
  my $xr = $xml_sth->fetchrow_arrayref();
  my $ega_id = $$xr[0];
  return $ega_id;
}

sub output_sample_xml {
  my ( $file_name, $samples ) = @_;
  my $output = new IO::File "> $file_name";
  my $writer = XML::Writer->new(
        OUTPUT      => $output,
        DATA_MODE   => 1,
        DATA_INDENT => 4
  );
  
  $writer->xmlDecl('UTF-8');
    $writer->startTag(
        'SAMPLE_SET',
        'xmlns:xsi' => 'http://www.w3.org/2001/XMLSchema-instance',
        'xsi:noNamespaceSchemaLocation' =>
          'ftp://ftp.sra.ebi.ac.uk/meta/xsd/sra_1_3/SRA.sample.xsd',
  );
 
  for my $sample (@$samples) {
    foreach my $sample_alias (keys % $sample){ 
      my %sample_tag = (
              'alias'       => $sample_alias,
              'center_name' => $$sample{$sample_alias}{center_name},
              'broker_name' => 'EGA',
      );
    
      $sample_tag{accession} = $sample->{$sample_alias}->{accession}
        if ( $sample->{$sample_alias}->{accession} );

       $writer->startTag( 'SAMPLE', %sample_tag );
       {
         $writer->dataElement( "TITLE", $sample->{$sample_alias}->{title} );
         $writer->startTag('SAMPLE_NAME');
         {
           $writer->dataElement( 'TAXON_ID', 
             $sample->{$sample_alias}->{taxon_id} );
           $writer->dataElement( 'SCIENTIFIC_NAME',
             $sample->{$sample_alias}->{scientific_name} );
           $writer->dataElement( 'COMMON_NAME', 
             $sample->{$sample_alias}->{common_name} );
           $writer->dataElement( 'ANONYMIZED_NAME',
             $sample->{$sample_alias}->{anonymized_name} );
          }
          $writer->endTag();    #sample_name

          $writer->dataElement( "DESCRIPTION", $sample->{description} );

           $writer->startTag('SAMPLE_ATTRIBUTES');
           {
           foreach my $attribute ( keys %{ $sample->{$sample_alias}->{attributes} } ) {
             $writer->startTag('SAMPLE_ATTRIBUTE');
             {
               $writer->dataElement( 'TAG',   $attribute );
               $writer->dataElement( 'VALUE', $$sample{$sample_alias}{attributes}{$attribute} );
             }
             $writer->endTag();    #sample attribute
           }
           $writer->endTag();        #sample attributes
          } 
         $writer->endTag();            #sample
       }
    } 
  }
  $writer->endTag();  
}


sub load_known_samples {
  my ( $ena, $sample_id ) = @_;
  my %known_samples;

  my $query_stmt = "select 
  s.sample_xml.getClobVal(),
  s.sample_alias,
  s.anonymized_name,
  s.sample_title
  from
  sample s
  where s.sample_alias = ? and s.center_name = ?";
 
  my $query_sth = $ena->dbc->prepare($query_stmt);
  $query_sth->execute($sample_id, 'BLUEPRINT');
  while ( my $rs = $query_sth->fetchrow_arrayref ) {  
    my ( $sample_xml, $sample_alias, $anonymized_name, $sample_title ) = @$rs;
    my %sample;
    $sample{anonymized_name} = $anonymized_name;
    $sample{title} = $sample_title;

    my $t = XML::Twig->new(
       twig_handlers => {
         SAMPLE => sub {
           my ( $xt, $element ) = @_; 
           $sample{alias}       = $element->att('alias');
           $sample{center_name} = $element->att('center_name');
           my $sample_acc_id    = $element->att('accession');
           my $sample_ega_id    = get_ega_id( $era, $sample_acc_id, 'sample', 'id' );
           $sample{accession}   = $sample_ega_id;
        },
        TAXON_ID => sub {
          my ( $xt, $element ) = @_;
          $sample{taxon_id} = $element->text();
        },
        SCIENTIFIC_NAME => sub {
          my ( $xt, $element ) = @_;
          $sample{scientific_name} = $element->text();
        },
        COMMON_NAME => sub {
          my ( $xt, $element ) = @_;
          $sample{common_name} = $element->text();
        },
        DESCRIPTION => sub {
          my ( $xt, $element ) = @_;
          $sample{description} = $element->text();
        },
        SAMPLE_ATTRIBUTE => sub {
          my ( $xt, $element ) = @_;
          my $tag   = $element->first_child('TAG');
          my $value = $tag->next_sibling('VALUE');
          my $units = $tag->next_sibling('UNITS');

          my $tag_name = $tag->text;
          my $tag_val  = $value->text;

          unless ( $tag->text eq 'ENA-SUBMISSION-TOOL' ||
                   $tag->text eq 'ENA-CHECKLIST' ){
             
            die "$tag_name present twice"
              if exists $sample{attributes}{$tag_name};

            $sample{attributes}{$tag_name} = $tag_val;
          }
        },
      }
    );

    $t->parse($sample_xml);
    $known_samples{$sample_alias} = \%sample;
  }
  $query_sth->finish;
  return \%known_samples;
}



sub reformat_age {
  my ( $age ) = @_;
  my $new_age = undef;
  if ( $age=~/(\d+)\s?-\s?(\d+)/){
    $new_age = $1 . ' - ' . $2;
  }
  elsif (  $age=~/^\d+$/  ){
    $new_age = $age . ' - ' . ( $age + 5 );
  }
  else {
    warn "can't format age: $age",$/;
    $new_age = $age;
  }
  return $new_age;
}

sub read_metadata {
  my ( $metadata_file, $debug ) = @_;
  my %metadata;

  open my $fh, '<', $metadata_file;
  while( <$fh> ){
    chomp;
    next if /^#/;
    if ( my ($sample, $err, $attribute) = (/(\S+)\s+(error|warning)\s+\'(\S+)?:/)){
      $metadata{$sample}{$attribute}++;
    }
    else {
      warn "Isssue:",$_,$/
       if $debug;
    }
  }
  close( $fh );
  return \%metadata;
}

=head1
  Script for metadata correction based on metadata validation report

=head2 Usage:

  perl correct_metadata.pl --report_file validation_report.out --era_user USER --era_pass PASS --output_xml out.xml
 
=head2 Options:
    
  report_file : Input report file from metadata validator
  era_user    : ERA user
  era_pass    : ERA pass
  output_xml  : Output XML file for submission
  debug       : Turn on debugging (default: off)

=cut
