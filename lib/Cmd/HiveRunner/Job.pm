use strict;
use warnings;
use feature ':5.10.1';
package Cmd::HiveRunner::Job;
# ABSTRACT: Info about a running Job within a Hive Task
use Moo;
use Data::Dumper;
use autodie qw(:all);
use String::ShellQuote qw(shell_quote_best_effort);
use IPC::System::Simple qw(systemx);

has id           => ( is => 'rwp', required => 1 );
has start_time   => ( is => 'rwp', default => \&__now );
has state        => ( is => 'rwp', default => sub { 'ready' } );
has tracking_url => ( is => 'rwp' );
has kill_cmd     => ( is => 'rwp' );
has end_time     => ( is => 'rwp' );

sub run_time {
  my ($self) = @_;
  return ($self->end_time || __now()) - $self->start_time;
}

sub __now { scalar time }

1 && q{ this statement is true }; # truth
__END__

=head1 DESCRIPTION

=head1 SYNOPSIS

=head1 METHODS

=head2 start_time

The unix timestamp of when the hive process was started

=head2 state

The current state of the Hive process

=head2 id

The Hadoop job ID

=head2 tracking_url

The URL the user can visit to track the job's progress

=head2 kill_cmd

The command that can be used to kill the current Hive job

=head2 end_time

The unix timestamp of when the hive job finished

=head2 run_time

The time in seconds for which the hive job has been (or had been) running

