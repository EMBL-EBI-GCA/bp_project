use strict;
use ReseqTrack::Tools::ERAUtils;
use IO::File;
use XML::Writer;
use XML::Twig;
use Data::Compare;
use Getopt::Long;
use Time::gmtime;
use Data::Dumper;
use autodie;
use Carp;

my $col_sep = "\t";
my ( $donations_file, $samples_file,          $era_pass, 
     $output_prefix,  $submission_account_id, $era_user  );
my $check_missing         = 0;
my $spreadsheet_output    = 0;
my $xml_output            = 1;
my $ontology_lookup_file;

&GetOptions(
    'donations=s'             => \$donations_file,
    'samples=s'               => \$samples_file,
    'era_user=s'              => \$era_user,
    'era_pass=s'              => \$era_pass,
    'output_prefix=s'         => \$output_prefix,
    'check_missing!'          => \$check_missing,
    'spreadsheet!'            => \$spreadsheet_output,
    'xml!'                    => \$xml_output,
    'ontology_lookup=s'       => \$ontology_lookup_file,
    'submission_account_id=s' => \$submission_account_id,
);

if ( !defined $output_prefix ) {
    my $gm = gmtime();
    $output_prefix =
      sprintf( "%04u%02u%02u_", $gm->year + 1900, $gm->mon + 1, $gm->mday );
}

my $dbc = get_erapro_conn( $era_user, $era_pass )->dbc;

my $cell_type_to_ontology =
  hash_from_list( parse_file( $ontology_lookup_file, "\t" ), 'cbr_term' );

# what do we have in the ENA db?
my $known_samples = load_known_samples( $dbc, $submission_account_id );

# donation information
my %donations;
for my $donation ( @{ parse_file( $donations_file, $col_sep ) } ) {
    my $id = $donation->{donor_id};
    if ($id) {
        $donations{$id} = $donation;
    }
}

# sample information
my $samples = parse_file( $samples_file, $col_sep );

my @samples_to_add;
my @samples_to_update;

for my $sample (@$samples) {

    next unless $sample->{sample_id};

    my $donors = find_donors_for_samples( $sample, \%donations );

    if ( $known_samples->{ $sample->{sample_id} } ) {
        my $sample_to_update =
          existing_sample_data( $sample, $donors,
            $known_samples->{ $sample->{sample_id} },
            $cell_type_to_ontology );

        push @samples_to_update, $sample_to_update if ($sample_to_update);
    }
    else {
        push @samples_to_add,
          fresh_sample_data( $sample, $donors, $cell_type_to_ontology );
    }
}

#print Dumper(\@samples_to_add);

if ($xml_output) {
    xml_output( $output_prefix, \@samples_to_add, \@samples_to_update );
}
if ($spreadsheet_output) {
    spreadsheet_ouput( $output_prefix, \@samples_to_add, \@samples_to_update );
}

#can we identify the missing samples from the addenbrookes report

if ($check_missing) {
    for my $s (@$samples) {
        my $id = $s->{sample_id};
        delete $known_samples->{$id};
    }

    if ( keys %$known_samples ) {
        print
"The following sample IDs were found in the ENA database, but are missing from the samples list$/";
        print join( $/, keys %$known_samples );
    }
}

sub spreadsheet_ouput {
    my ( $output_prefix, $samples_to_add, $samples_to_update ) = @_;
    my $out_file = $output_prefix . 'samples.txt';
    open my $out, '>', $out_file or die("Could not write to $out_file: $!");

    my %attribute_keys;
    for my $sample (@$samples_to_add, @$samples_to_update){
      $sample->{attribute_hash} = {};
      for my $a (@{$sample->{attributes}}){
        $attribute_keys{$a->[0]}++;
        $sample->{$a->[0]} = $a->[1];
      }
    }

    my @attribute_fields = sort keys %attribute_keys;
    
    print $out join( "\t",
        qw(sample_alias tax_id scientific_name common_name anonymized_name sample_title sample_description),@attribute_fields
    ) . $/;

    my @fields =
      qw(alias taxon_id scientific_name common_name anonymized_name title description);
    @fields = (@fields,@attribute_fields);

    for my $sample (@$samples_to_add, @$samples_to_update) {
        print $out join( "\t", @{$sample}{@fields} ) . $/;
    }

    close $out;
}

sub find_donors_for_samples {
    my ( $sample, $donations ) = @_;

    my @ids = split /,/, $sample->{donor_id};
    my @donors;

    if ( scalar(@ids) == 1 ) {
        my $donation = $donations{ $ids[0] };
        @donors = ($donation);
    }
    else {
        my @donor_ids = map { substr( $_, 0, 6 ) } @ids;

        @donors = map { $donations->{$_} } @donor_ids;

        $sample->{pooled_sample_ids}       = $sample->{donor_id};
        $sample->{number_of_donors_pooled} = scalar(@ids);
        $sample->{pooled_donor_ids}        = join( ',', @donor_ids );
    }

    return \@donors;
}

sub xml_output {
    my ( $output_prefix, $samples_to_add, $samples_to_update ) = @_;

    my $new_samples_output    = $output_prefix . 'new_samples.xml';
    my $update_samples_output = $output_prefix . 'update_samples.xml';
    my $submission_output     = $output_prefix . 'sample_submission.xml';
    my $submission_alias      = $output_prefix . 'sample_submission.xml';

    output_sample_xml( $new_samples_output, $samples_to_add )
      if (@$samples_to_add);
    output_sample_xml( $update_samples_output, $samples_to_update )
      if (@$samples_to_update);

    if (@$samples_to_update) {
        output_submission_xml(
            'VALIDATE',
            'validate_modify' . $submission_alias,
            'validate_update' . $submission_output,
            $update_samples_output,
            $samples_to_update
        );
        output_submission_xml(
            'MODIFY',
            'modify' . $submission_alias,
            'modify' . $submission_output,
            $update_samples_output, $samples_to_update
        );
    }
    if (@$samples_to_add) {
        output_submission_xml(
            'VALIDATE',
            'validate_add' . $submission_alias,
            'validate_add' . $submission_output,
            $new_samples_output, $samples_to_add
        );
        output_submission_xml(
            'ADD',
            'add' . $submission_alias,
            'add' . $submission_output,
            $new_samples_output, $samples_to_add,
        );
    }

}

sub extract_donor_information {
    my ($donors) = @_;

    my ( $holder, $min_age, $max_age );

    for my $donor (@$donors) {
        $holder->{sex}{ $donor->{gender} }++;
        $holder->{tissue}{ $donor->{tissue} }++;
        $holder->{ethnicity}{ $donor->{ethnicity} }++;

        my ( $l, $h );

        if ( $donor->{age_bin} == 0 ) {
            ( $l, $h ) = ( 0, 0 );
        }
        else {
            $l = $donor->{age_bin};
            $h = $donor->{age_bin} + 5;
        }

        if ( !defined $min_age || $l < $min_age ) {
            $min_age = $l;
        }
        if ( !defined $max_age || $h > $max_age ) {
            $max_age = $h;
        }
    }

    my %d;
    for my $k (qw(sex tissue ethnicity)) {
        my @values = keys %{ $holder->{$k} };
        if ( scalar(@values) > 1 ) {
            $d{$k} = 'NA';
        }
        else {
            $d{$k} = $values[0];
        }
    }
    if ( $min_age == $max_age ) {
        $d{age} = $min_age;
    }
    else {
        $d{age} = "$min_age - $max_age";
    }

  # die Dumper($donors,$holder,$min_age,$max_age,\%d) if (scalar(@$donors) > 1);

    return \%d;
}

sub fresh_sample_data {
    my ( $sample, $donors, $ontology_lookup ) = @_;
    my $donation = extract_donor_information($donors);

    my $ontology_vals = $ontology_lookup->{ $sample->{cell_type} };
    die(    'No ontology lookup for sample '
          . $sample->{sample_id}
          . ' cell type '
          . $sample->{cell_type} )
      unless $ontology_vals;

    my @sample_uri = ( $ontology_vals->{URI} );
    my @sample_term_ids = ( $ontology_vals->{ontology_term_id}, 'PATO:0000461' );

    # sex
    my $sex = $donation->{sex};
    if ( $sex eq 'M' ) {
        $sex = 'Male';
    }
    elsif ( $sex eq 'F' ) {
        $sex = 'Female';
    }
    elsif ( $sex eq 'X' ) {
        $sex = 'Unknown';
    }
    else {
        die(    "Could not get donor sex for "
              . $sample->{sample_id} . ' '
              . Dumper($donation) );
    }

    my $molecule = $sample->{type};
    if ( $molecule eq 'Bisulphite' ) {
        $molecule = 'genomic DNA';
    }
    elsif ( $molecule eq 'Chromatin' ) {
        $molecule = 'genomic DNA';
    }
    elsif ( $molecule eq 'DNAse1' ) {
        $molecule = 'genomic DNA';
    }
    elsif ( $molecule eq 'RNA' ) {
        if ( grep { $sample->{cell_type} eq $_ }
            ( 'HSC', 'MPP', 'CLP', 'MEP', 'CMP', 'GMP' ) )
        {
            $molecule = 'polyA RNA';
        }
        else {
            $molecule = 'total RNA';
        }
    }
    else {
        die( "Unexpected type $molecule for sample " . $sample->{sample_id} );
    }

    
    
    my $tissue = $donation->{tissue};
    if ( $tissue eq 'Adult' ) {
        $tissue = 'Venous blood';
        push @sample_uri,      'http://purl.obolibrary.org/obo/UBERON_0013756';
        push @sample_term_ids, 'UBERON:0013756';
    }
    elsif ( $tissue eq 'Cord' ) {
        $tissue = 'Cord blood';
        push @sample_uri,      'http://purl.obolibrary.org/obo/UBERON_0012168';
        push @sample_term_ids, 'UBERON:0012168';
    }
    else {
        die(    "Could not get tissue for "
              . $sample->{sample_id} . ' '
              . Dumper($donation) );
    }

    my $age = $donation->{age};
    if ( !defined $age ) {
        $age = 'NA';
    }
    elsif ( $tissue eq 'Cord blood' ) {
        $age = 0;
    }

    $donation->{ethnicity} =~ s/Nortern/Northern/;

    my @attributes;

    my $passage = 'NA';
    if ( $ontology_vals->{culture_conditions} ) {
        push @attributes, [ 'BIOMATERIAL_TYPE', 'Primary Cell Culture' ];
        push @attributes,
          [ 'CULTURE_CONDITIONS', $ontology_vals->{culture_conditions} ];
        $passage = $ontology_vals->{culture_conditions};
    }
    else {
        push @attributes, [ 'BIOMATERIAL_TYPE', 'Primary Cell' ];
    }

    push @attributes, [ 'MOLECULE',             $molecule ];
    push @attributes, [ 'DISEASE',              'None' ];
    push @attributes, [ 'BIOMATERIAL_PROVIDER', 'NIHR Cambridge BioResource' ];

    push @attributes, [ 'CELL_TYPE', $ontology_vals->{ontology_term_name} ];
    push @attributes, [ 'SAMPLE_ONTOLOGY_URI', join( ';', @sample_uri ) ];
    push @attributes, [ 'DISEASE_ONTOLOGY_URI', 'NA' ];
    push @attributes, [ 'MARKERS', $ontology_vals->{markers} ];

    push @attributes, [ 'PASSAGE_IF_EXPANDED', $passage ];
    push @attributes, [ 'TISSUE_TYPE',         $tissue ];

    if ( $sample->{number_of_donors_pooled} ) {
        push @attributes, [ 'POOL_ID',          $sample->{sample_id} ];
        push @attributes, [ 'POOLED_DONOR_IDS', $sample->{pooled_donor_ids} ];
        push @attributes, [ 'subject_id', $sample->{pooled_donor_ids} ];

    }
    else {
        push @attributes, [ 'DONOR_ID', $sample->{donor_id} ];
        push @attributes, [ 'subject_id', $sample->{donor_id} ];  # EGA requires this field
        push @attributes,
          [ 'Donor ID', $sample->{donor_id} ];                    # EGA requires this field

    }
    push @attributes, [ 'DONOR_AGE',           $age ];
    push @attributes, [ 'DONOR_HEALTH_STATUS', 'NA' ];
    push @attributes, [ 'DONOR_SEX',           $sex ];
    push @attributes, [ 'gender', lc($sex) ];     # EGA requires this field
    push @attributes, [ 'phenotype', join( ';', @sample_term_ids ) ]; # EGA requires this fields
    push @attributes, [ 'DONOR_ETHNICITY', $donation->{ethnicity} || 'NA' ];

    my $description =
        $ontology_vals->{ontology_term_name}
      . ' from '
      . $sample->{donor_id} . ' ('
      . $sample->{type}
      . ') from the BLUEPRINT project';

    my $o = {
        alias           => $sample->{sample_id},
        center_name     => 'BLUEPRINT',
        taxon_id        => '9606',
        scientific_name => 'Homo sapiens',
        common_name     => 'Human',
        anonymized_name => $sample->{donor_id},
        title           => $sample->{sample_id},
        description     => $description,
        attributes      => \@attributes,
    };

    return $o;
}

sub existing_sample_data {
    my ( $sample, $donors, $ena_sample_info, $ontology_lookup ) = @_;

    my $sample_as_fresh =
      fresh_sample_data( $sample, $donors, $ontology_lookup );
    $sample_as_fresh->{accession} = $ena_sample_info->{accession};

    my ($ena_molecule_attr) =
      grep { $_->[0] eq 'MOLECULE' } @{ $ena_sample_info->{attributes} };
    my ($fresh_molecule_attr) =
      grep { $_->[0] eq 'MOLECULE' } @{ $sample_as_fresh->{attributes} };

# molecule type depends on the experimental protocol, so use what's in the archive instead of blithely changing it to the default.
    if ( $ena_molecule_attr && $fresh_molecule_attr ) {
        $fresh_molecule_attr->[1] = $ena_molecule_attr->[1];
    }

    #  if ($sample_as_fresh->{accession} eq 'ERS353040') {
    #    die Dumper($sample_as_fresh,$ena_sample_info);
    #  }

    if ( Compare( $sample_as_fresh, $ena_sample_info ) ) {
        return undef;
    }
    else {
        return $sample_as_fresh;
    }
}

sub output_submission_xml {
    my ( $mode, $submission_alias, $submission_output, $samples_output ) = @_;
    my %modes = ( "VALIDATE" => 1, "ADD" => 1, "MODIFY" => 1 );
    die "Invalid mode $mode" unless $modes{$mode};
    my $output = new IO::File "> $submission_output";
    my $writer = XML::Writer->new(
        OUTPUT      => $output,
        DATA_MODE   => 1,
        DATA_INDENT => 4
    );

    $writer->xmlDecl('UTF-8');
    $writer->startTag(
        'SUBMISSION_SET',
        'xmlns:xsi' => 'http://www.w3.org/2001/XMLSchema-instance',
        'xsi:noNamespaceSchemaLocation' =>
'ftp://ftp . sra . ebi . ac . uk / meta / xsd / sra_1_5 / SRA . submission . xsd
',
    );
    {
        $writer->startTag(
            'SUBMISSION',
            alias       => $submission_alias,
            center_name => 'BLUEPRINT',
            broker_name => 'EGA',
        );
        {
            $writer->startTag('ACTIONS');
            {
                $writer->startTag('ACTION');
                {
                    $writer->emptyTag(
                        $mode,
                        source => $samples_output,
                        schema => 'sample'
                    );
                }
                $writer->endTag();    # add action
            }
            {
                $writer->startTag('ACTION');
                {
                    $writer->emptyTag('PROTECT');
                }
                $writer->endTag();    # update action
            }
            $writer->endTag();        #actions
        }
        $writer->endTag();            #submission
    }
    $writer->endTag();                #submission set
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

        my %sample_tag = (
            'alias'       => $sample->{alias},
            'center_name' => $sample->{center_name},
            'broker_name' => 'EGA',
        );

        $sample_tag{accession} = $sample->{accession}
          if ( $sample->{accession} );

        $writer->startTag( 'SAMPLE', %sample_tag );
        {
            $writer->dataElement( "TITLE", $sample->{title} );
            $writer->startTag('SAMPLE_NAME');
            {
                $writer->dataElement( 'TAXON_ID', $sample->{taxon_id} );
                $writer->dataElement( 'SCIENTIFIC_NAME',
                    $sample->{scientific_name} );
                $writer->dataElement( 'COMMON_NAME', $sample->{common_name} );
                $writer->dataElement( 'ANONYMIZED_NAME',
                    $sample->{anonymized_name} );
            }
            $writer->endTag();    #sample_name

            $writer->dataElement( "DESCRIPTION", $sample->{description} );

            $writer->startTag('SAMPLE_ATTRIBUTES');
            foreach my $attribute ( @{ $sample->{attributes} } ) {
                $writer->startTag('SAMPLE_ATTRIBUTE');
                {
                    $writer->dataElement( 'TAG',   $attribute->[0] );
                    $writer->dataElement( 'VALUE', $attribute->[1] );
                    $writer->dataElement( 'UNITS', $attribute->[2] )
                      if $attribute->[2];
                }
                $writer->endTag();    #sample attribute
            }

            $writer->endTag();        #sample attributes
        }

        $writer->endTag();            #sample
    }

    $writer->endTag();                #sample_set
}

sub load_known_samples {
    my ( $dbc, $ega_submission_account_id ) = @_;
    my $query_stmt = <<'QUERY_END';
  select
  s.sample_xml.getClobVal(),
  s.sample_alias,
  s.anonymized_name,
  s.sample_title
  from
  submission sub,
  sample s,
  xmltable('/SAMPLE_SET/SAMPLE/SAMPLE_ATTRIBUTES' PASSING s.sample_xml  
  COLUMNS 
          BIOMATERIAL_PROVIDER varchar2(512) PATH '//SAMPLE_ATTRIBUTE[TAG[text()="BIOMATERIAL_PROVIDER"]]/VALUE'        
  )(+) sx  
  where sub.ega_submission_account_id = ?
  and s.submission_id = sub.submission_id
  and sx.BIOMATERIAL_PROVIDER = 'NIHR Cambridge BioResource'
QUERY_END

# accessing xmltype as a clob
# default read limit is far too short to be useful
# truncating the read at the read limit isn't acceptable, will produce invalid xml
    $dbc->db_handle()->{LongReadLen} = 66000;
    $dbc->db_handle()->{LongTruncOk} = 0;

    my $query_sth = $dbc->prepare($query_stmt);
    $query_sth->execute($ega_submission_account_id)
      or die $query_sth->errstr;

    my %known_samples;

    while ( my $rs = $query_sth->fetchrow_arrayref ) {
        my ( $sample_xml, $sample_alias, $anonymized_name, $sample_title ) =
          @$rs;

        my %sample = ( attributes => [] );
        $sample{anonymized_name} = $anonymized_name;
        $sample{title}           = $sample_title;

        my $t = XML::Twig->new(
            twig_handlers => {
                SAMPLE => sub {
                    my ( $xt, $element ) = @_;
                    $sample{alias}       = $element->att('alias');
                    $sample{center_name} = $element->att('center_name');
                    $sample{accession}   = $element->att('accession');
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

                    my @attribute = ( $tag->text, $value->text );
                    $attribute[2] = $units->text if $units;

                    push @{ $sample{attributes} }, \@attribute
                      unless ( $tag->text eq 'ENA-SUBMISSION-TOOL'
                        || $tag->text eq 'ENA-CHECKLIST' );
                },
            }
        );
        $t->parse($sample_xml);

        $known_samples{$sample_alias} = \%sample;
    }
    $query_sth->finish;
    return \%known_samples;
}

sub parse_file {
    my ( $file, $col_sep ) = @_;
		confess("No file!")if (!$file);
    open my $fh, '<', $file;

    my @headers;
    my @items;

    while ( my $row = <$fh> ) {
        chomp $row;

        my @vals = split $col_sep, $row;

        if (@headers) {
            my %item;
            @item{@headers} = @vals;
            push @items, \%item;
        }
        else {
            @headers = @vals;
        }
    }

    close $fh;

    return \@items;
}

sub hash_from_list {
    my ( $items, $key ) = @_;
    my %hash;

    for my $item (@$items) {
        my $key = $item->{$key};
        confess "hash key collision on $key: $/" . Dumper($item)
          if $hash{$key};
        $hash{$key} = $item;
    }
    return \%hash;
}
