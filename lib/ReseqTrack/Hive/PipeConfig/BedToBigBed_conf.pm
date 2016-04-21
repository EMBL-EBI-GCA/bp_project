package ReseqTrack::Hive::PipeConfig::BedToBigBed_conf;

use strict;
use warnings;

use base ('ReseqTrack::Hive::PipeConfig::ReseqTrackGeneric_conf');

sub default_options {
    my ($self) = @_;

    return {
        %{ $self->SUPER::default_options() },

        pipeline_name => 'bed2bigbed',
        seeding_module => 'ReseqTrack::Hive::PipeSeed::BasePipeSeed',
        seeding_options => {
            output_columns     => ['name', 'collection_id'],
            require_columns    => $self->o('require_collection_columns'),
            exclude_columns    => $self->o('exclude_collection_columns'),
            require_attributes => $self->o('require_collection_attributes'),
            exclude_attributes => $self->o('exclude_collection_attributes'),
        },
        require_collection_columns => { type => $self->o('call_group_type') },
         
        exclude_collection_columns => {},
        require_collection_attributes => {},
        exclude_collection_attributes => {},
         
        bigbed_type     => undef,
        call_group_type => undef,
 
        samtools         => '/nfs/1000g-work/G1K/work/bin/samtools/samtools',
        bedtools         => '/nfs/1000g-work/G1K/work/bin/bedtools2-2.20.1/bin/bedtools',
        bedToBigBedPath  => '/nfs/1000g-work/G1K/work/davidr/pipeline-deps/bin/bedToBigBed',
        chr_file         => '/nfs/1000g-work/ihec/work/bp_pipe/pipeline_run/reference_dir/GCA_000001405.15_GRCh38_no_alt_analysis_set.fa.fai',
        collection_name  => '#name#',
        build_collection => 1,
       
       
    };
}

sub pipeline_create_commands {
    my ($self) = @_;

    return [
        @{$self->SUPER::pipeline_create_commands},
    ];
}

sub pipeline_wide_parameters {
    my ($self) = @_;
    return {
        %{$self->SUPER::pipeline_wide_parameters},
    };
}

sub resource_classes {
    my ($self) = @_;
    return {
            %{$self->SUPER::resource_classes},
            '200Mb' => { 'LSF' => '-C0 -M200 -q '.$self->o('lsf_queue').' -R"select[mem>200] rusage[mem=200]"' },
            '500Mb' => { 'LSF' => '-C0 -M500 -q '.$self->o('lsf_queue').' -R"select[mem>500] rusage[mem=500]"' },
            '1Gb'   => { 'LSF' => '-C0 -M1000 -q '.$self->o('lsf_queue').' -R"select[mem>1000] rusage[mem=1000]"' },
            '2Gb'   => { 'LSF' => '-C0 -M2000 -q '.$self->o('lsf_queue').' -R"select[mem>2000] rusage[mem=2000]"' },
            '4Gb'   => { 'LSF' => '-C0 -M4000 -q '.$self->o('lsf_queue').' -R"select[mem>4000] rusage[mem=4000]"' },
            '5Gb'   => { 'LSF' => '-C0 -M5000 -q '.$self->o('lsf_queue').' -R"select[mem>5000] rusage[mem=5000]"' },
            '8Gb'   => { 'LSF' => '-C0 -M8000 -q '.$self->o('lsf_queue').' -R"select[mem>8000] rusage[mem=8000]"' },
            '12Gb'  => { 'LSF' => '-C0 -M12000 -q '.$self->o('lsf_queue').' -R"select[mem>12000] rusage[mem=12000]"' },
    };
}

sub pipeline_analyses {
    my ($self) = @_;

    my @analyses;
    push(@analyses, {
            -logic_name    => 'get_seeds',
            -module        => 'ReseqTrack::Hive::Process::SeedFactory',
            -meadow_type => 'LOCAL',
            -parameters  => {
                seeding_module  => $self->o('seeding_module'),
                seeding_options => $self->o('seeding_options'),
            },
            -flow_into => {
                2 => [ 'block_seed_complete' ],
            },
      });
   push(@analyses, {
          -logic_name  => 'block_seed_complete',
          -module      => 'ReseqTrack::Hive::Process::BaseProcess',
          -meadow_type => 'LOCAL',
          -parameters  => {
            reseqtrack_options  => {
              flows_non_factory => [1,2],
            },
          },
            -flow_into => {
                '2->A' => { 'find_source_beds' => {'callgroup' => '#name#', 'bed_collection_id' => '#collection_id#'}},
                'A->1' => [ 'mark_seed_complete' ],
            },
   });
   push(@analyses, {
            -logic_name    => 'find_source_beds',
            -module        => 'ReseqTrack::Hive::Process::ImportCollection',
            -meadow_type   => 'LOCAL',
            -parameters    => {
                collection_id      => '#bed_collection_id#',
                output_param       => 'bed',
                reseqtrack_options => {
                  flows_file_count_param  => 'bed',
                  flows_file_count        => { 1 => '1+', },
                },
            },
            -flow_into => {
                1 => [ 'bed_to_bigbed' ],
            },
      });

   push(@analyses, {
            -logic_name => 'bed_to_bigbed',
            -module     => 'ReseqTrack::Hive::Process::ConvertBedToBigBed',
            -parameters => {
                bed     => ['#bed#'],
                chr_file        => $self->o('chr_file'),
                bedToBigBedPath => $self->o('bedToBigBedPath'),
            },
            -rc_name        => '2Gb',
            -hive_capacity  =>  200,
            -flow_into      => {
                1 => {  'store_bigbed' => { 'bigbed' => '#bigbed#' } },
                },
   });
   push(@analyses, {
            -logic_name    => 'store_bigbed',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('bigbed_type'),
              file => '#bigbed#',
              collection_name => $self->o('collection_name'),
              collect => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
   });
   push(@analyses, {
            -logic_name    => 'mark_seed_complete',
            -module        => 'ReseqTrack::Hive::Process::UpdateSeed',
            -parameters    => {
              is_complete  => 1,
            },
            -meadow_type => 'LOCAL',
      });
      
      return \@analyses;
}

1;
