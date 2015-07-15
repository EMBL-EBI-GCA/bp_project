
my %hash1=( chr1=>'1',
	    chr2=>'2',
	    chr3=>'100'
);

my %hash2=( chr1=>'1',
	    chr2=>'2',
	    chr3=>'10'
);

print compare_2_hashes(\%hash1,\%hash2,"DNASE_HOTSPOT_PEAK_BE"),"\n";

sub compare_2_hashes {
    my ($hash1,$hash2,$ftype)=@_;
    for ( keys %$hash1 ) {
	if (!exists($hash2->{$_})) {
	    return 0;
	    next;
	}
	if ($ftype eq 'DNASE_HOTSPOT_PEAK_BED') {
	    #this file type does not contain the same number of peaks than the rest
	    #of DNAse file types. This means, that only chr existence is checked
	    return 1;
	} else {
	    if ( $hash1->{$_} != $hash2->{$_} ) {
		return 0;
	    }
	}
    }
    return 1;
}
