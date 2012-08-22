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
use Cmd::HiveRunner::Job;

has sudo_path => ( is => 'ro', default => sub { __which_or_die('sudo') } );
has hive_path => ( is => 'ro', default => sub { __which_or_die('hive') } );
has user => ( is => 'ro' );
has conf => ( is => 'ro' );

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
  return Cmd::HiveRunner::Job->new( cmd => \@cmd );
}

1 && q{ this statement is true }; # truth
__END__

=head1 DESCRIPTION

=head1 SYNOPSIS

