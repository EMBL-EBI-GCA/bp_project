package Blueprint::PipeConfig::Run_wiggler_conf;

use strict;
use warnings;

use base ('ReseqTrack::Hive::PipeConfig::ReseqTrackGeneric_conf');

sub default_options {
    my ($self) = @_;

    return {
        %{ $self->SUPER::default_options() },

        'pipeline_name' => 'wiggler',
        seeding_module  => 'ReseqTrack::Hive::PipeSeed::RunWigglerSeed',
        seeding_options => {
          output_columns     => ['name', 'collection_id'],
          require_columns    => $self->o('require_collection_columns'),
          exclude_columns    => $self->o('exclude_collection_columns'),
          require_attributes => $self->o('require_collection_attributes'),
          exclude_attributes => $self->o('exclude_collection_attributes'),
          metadata_file      => $self->o('metadata_file'),
          path_names_array   => $self->o('path_names_array'),
        },
        metadata_file        => $self->o('metadata_file'),
        path_names_array     => [ 'sample_desc_1', 'sample_desc_2', 'sample_desc_3', 'library_strategy', 'center_name' ],

        #'callgroup_type'    => 'DNASE_DEDUP_BAM',
        #'wiggler_file_type' => 'DNASE_WIGGLE',

        'wiggler_file_type'             => $self->o('wiggler_file_type'),
        'callgroup_type'                => $self->o('callgroup_type'),
        'require_collection_columns'    => {'type' => $self->o('callgroup_type')},
        'exclude_collection_columns'    => {},
        'require_collection_attributes' => {},
        'exclude_collection_attributes' => {},
        'collection_name'               => '#experiment_source_id#',

        'samtools_exe'            => '/nfs/1000g-work/G1K/work/bin/samtools/samtools',
        'wiggler_exe'             => '/nfs/1000g-work/G1K/work/davidr/pipeline-deps/align2rawsignal/bin/align2rawsignal',
        'bedGraph_to_bigWig_path' => '/nfs/1000g-work/G1K/work/davidr/pipeline-deps/bin/bedGraphToBigWig',
        'output_format'           => 'bw',
        'chrom_sizes_file'        => '/nfs/1000g-work/G1K/work/avikd/test_dir/test_grch38/no_alt_no_decoy_bwa/GCA_000001405.15_GRCh38_no_alt_analysis_set.fa.fai',
        'chrom_fasta_file'        => '/nfs/1000g-work/G1K/work/davidr/ref_genomes/homo_sapiens/grch38_no_alt_analysis/fasta/',
        'mappability_tracks'      => '/nfs/1000g-work/G1K/work/davidr/ref_genomes/homo_sapiens/grch38_no_alt_analysis/umap/globalmap_k36tok92',
        'mcr_root'                => '/nfs/1000g-work/G1K/work/davidr/pipeline-deps/MCR/v714',
        'wiggler_options'         =>  {},   
        'build_collection'        => 1,

        'root_output_dir'    => $self->o('root_output_dir'),
        'final_output_dir'   => $self->o('final_output_dir'),
        final_output_layout     => '#sample_desc_1#/#sample_desc_2#/#sample_desc_3#/#library_strategy#/#center_name#',

        name_file_module    => 'ReseqTrack::Hive::NameFile::BaseNameFile',
        name_file_method    => 'basic',
        name_file_params    => {
          new_dir       => '#final_output_dir#/#final_output_layout#',
          new_basename  => '#sample_alias#.#experiment_source_id#.#EXPERIMENT_TYPE#.bwa.GRCh38',
          add_datestamp => 1,
          suffix        => '.bw',
       },
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
            -logic_name  => 'get_seeds',
            -module      => 'ReseqTrack::Hive::Process::SeedFactory',
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
                '2->A' => { 'find_source_bams' => {'callgroup' => '#name#', 'bam_collection_id' => '#collection_id#'}},
                'A->1' => [ 'mark_seed_complete' ],
            },
   });
   push(@analyses, {
            -logic_name    => 'find_source_bams',
            -module        => 'ReseqTrack::Hive::Process::ImportCollection',
            -meadow_type   => 'LOCAL',
            -parameters    => {
                collection_id => '#bam_collection_id#',
                output_param  => 'bam',
                reseqtrack_options => {
                  flows_file_count_param => 'bam',
                  flows_file_count       => { 1 => '1+', },
                },
            },
            -flow_into => {
                1 => [ 'run_wiggler' ],
            },
      });
      push(@analyses, {
            -logic_name    => 'run_wiggler',
            -module        => 'ReseqTrack::Hive::Process::RunWiggler',
            -parameters    => {
                program_file            => $self->o('wiggler_exe'),
                options                 => $self->o('wiggler_options'),
                samtools                => $self->o('samtools_exe'),
                fragment_size           => '#estFraglen#',
                output_format           => $self->o('output_format'),
                chrom_sizes_file        => $self->o('chrom_sizes_file'),
                chrom_fasta_file        => $self->o('chrom_fasta_file'),
                mappability_tracks      => $self->o('mappability_tracks'),
                mcr_root                => $self->o('mcr_root'),
                bedGraph_to_bigWig_path => $self->o('bedGraph_to_bigWig_path'),
            },
            -rc_name            => '4Gb',
            -analysis_capacity  =>  50,
            -hive_capacity      =>  200,
            -flow_into => {
                1 => [ 'store_wiggler' ],
            },
      });
      push(@analyses, {
            -logic_name    => 'store_wiggler',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('wiggler_file_type'),
              file => '#bigwig#',
              name_file_module => $self->o('name_file_module'),
              name_file_method => $self->o('name_file_method'),
              name_file_params => $self->o('name_file_params'),
              final_output_dir => $self->o('final_output_dir'),
              final_output_layout => $self->o('final_output_layout'),
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

