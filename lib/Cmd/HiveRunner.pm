use strict;
use warnings;
use feature ':5.10.1';
package Cmd::HiveRunner;
# ABSTRACT: Runs an instance of the Hive CLI as the given user
use Moo;
use Carp;
use Data::Dumper;
use autodie qw(:all);
use Hash::Merge::Simple qw(merge);
use File::Which qw(which);
use Cmd::HiveRunner::Task;

has sudo_path => ( is => 'ro', default => sub { __which_or_die('sudo') } );
has hive_path => ( is => 'ro', default => sub { __which_or_die('hive') } );
has user => ( is => 'ro' );
has conf => ( is => 'ro', default => sub { +{} } );

sub __which_or_die { which $_[0] or croak "Can't find the '$_[0]' command" }

sub run {
  my ($self, %opt) = @_;
  my %hiveconf = %{ merge $self->conf, $opt{conf} };
  my $user = $opt{user} || $self->user;
  my @cmd = (
    ( $user ? ($self->sudo_path, '-E', '-u', $user) : () ),
      $self->hive_path, ( map { ("-hiveconf", "$_=$hiveconf{$_}") } keys %hiveconf ),
        '-e', $opt{query}
  );
  return Cmd::HiveRunner::Task->new( cmd => \@cmd );
}

1 && q{ this statement is true }; # truth
__END__

=head1 DESCRIPTION

This is a class used to run queries via the Hive CLI, as a specific user and setting
things like which mapreduce job queue to use.

Calling C<new> sets up an object with whatever you want your defaults to be, and makes
sure the program can find your hive and sudo commands. (sudo is necessary to run jobs
as a different user).

Then, you can kick off hive processes by calling C<run> with a query (in HQL), and
as that process runs in the background, you can check on the progress, and get other
potentially useful info about it.

=head1 SYNOPSIS

  my $hive = Cmd::HiveRunner->new(
    user => 'sscaffid',
    conf => { foo => 'bar' }
  );

  # confs are merged, with conf in run getting precedence.
  my $job = $hive->run(
    conf  => { 'mapred.job.queue.name' => 'default' },
    query => q{select * from t_location LIMIT 5},
  );

  # if you want to run things synchronously, just do this.
  $job->wait;

=method new

Construct a new HiveRunner object

=method run

Run the hive CLI with the given settings. Accepted parameters:

=for :list
= user
The user under which to run hive
= conf
Conf attributes to add-to or override those set in the object
= query
The HQL query you want Hive to run. I<required>

=attr sudo_path

The path to the sudo command. Will be auto-detected if not specified.

=attr hive_path

The path to the hive command. Will be auto-detected if not specified.

=attr user

The user under which to run hive. Will use sudo to change users if necessary.

=attr conf

A hash of key => value pairs like would be passed to hive with the -hiveconf option.

