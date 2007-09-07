use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Sphinx::Search',
    AUTHOR              => 'Jon Schutz <jon@jschutz.net>',
    VERSION_FROM        => 'lib/Sphinx/Search.pm',
    ABSTRACT_FROM       => 'lib/Sphinx/Search.pm',
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
	'File::SearchPath' => 0,
	'Path::Class' => 0,
	'Carp' => 0,
	'Socket' => 0,
	'DBI' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Sphinx-Search-*' },
);
