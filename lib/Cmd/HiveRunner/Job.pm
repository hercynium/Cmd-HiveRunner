use strict;
use warnings;
use feature ':5.10.1';
package Cmd::HiveRunner::Job;
# ABSTRACT: track and control a running hive process
use Moo;
use Data::Dumper;
use autodie qw(:all);
use String::ShellQuote qw(shell_quote_best_effort);
use IPC::System::Simple qw(systemx);
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Util qw(run_cmd portable_pipe);

has cmd => ( is => 'ro', required => 1 );
has start_time => ( is => 'ro', default => \&__now );

has _state => ( is => 'rw', default =>  sub { 'init' } );
sub state { shift->_state }

has _history_file => ( is => 'rw' );
sub history_file { shift->_history_file }

has _id => ( is => 'rw' );
sub id { shift->_id }

has _tracking_url => ( is => 'rw' );
sub tracking_url { shift->_tracking_url }

has _kill_cmd => ( is => 'rw' );
sub kill_cmd { shift->_kill_cmd }

has _progress => ( is => 'rw', default => sub { +{ map => 0, reduce => 0, stage => 0 } } );
sub progress { shift->_progress }

has _end_time => ( is => 'rw' );
sub end_time { shift->_end_time }

has _job_count => ( is => 'rw' );
sub job_count { shift->_job_count }

has _cur_job => ( is => 'rw' );
sub cur_job { shift->_cur_job }

has _pid => ( is => 'rw' );
sub pid { shift->_pid }

has _cmd_cv => ( is => 'rw' );
sub wait { shift->_cmd_cv->recv }

has _out_hdl => ( is => 'rw' );
has _err_hdl => ( is => 'rw' );

sub run_time {
  my ($self) = @_;
  return __now() - $self->start_time;
}

sub __now { scalar time }

# after the constructor, execute the job in another process, reading lines in
# from it as it goes and using the contents of those lines to set the various
# attributes/accessors.
sub BUILD {
  my ($self) = @_;

  my @cmd = @{ $self->cmd };

  print shell_quote_best_effort(@cmd) . "\n";

  # prepare an asynch handle for the command's STDOUT
  my ($out_r, $out_w) = portable_pipe;
  my $out_hdl = AnyEvent::Handle->new(
    fh => $out_r,
    on_read => sub {
      shift->push_read( line => sub {
        my ($hdl, $line) = @_;
        $self->_handle_stdout_line($line);
      });
    },
    on_eof => sub {},
    on_error => sub {
      my ($hdl, $fatal, $msg) = @_;
      AE::log error => $msg;
      $hdl->destroy;
  });
  $self->_out_hdl($out_hdl);

  # prepare an asynch handle for the command's STDERR
  my ($err_r, $err_w) = portable_pipe;
  my $err_hdl = AnyEvent::Handle->new(
    fh => $err_r,
    on_read => sub {
      shift->push_read( line => sub {
        my ($hdl, $line) = @_;
        $self->_handle_stderr_line($line);
      });
    },
    on_eof => sub {},
    on_error => sub {
      my ($hdl, $fatal, $msg) = @_;
      AE::log error => $msg;
      $hdl->destroy;
  });
  $self->_err_hdl($err_hdl);

  # This will run when the command finishes
  my $cmd_done = sub {
    print "Command finished: ", Dumper(\@_) . "\n";
    $self->_state('done');
    $self->_err_hdl->destroy;
    $self->_out_hdl->destroy;
  };

  my $cmd_pid;
  my $cv = run_cmd \@cmd,
    '<'  => \*STDIN,
    '>'  => $out_w,
    '2>' => $err_w,
    '$$' => \$cmd_pid;

  $cv->cb($cmd_done);
  $self->_pid($cmd_pid);
  $self->_cmd_cv($cv);
  $self->_state('start');
}

# The Hive CLI prints out results on STDOUT. This method
# will handle that output.
sub _handle_stdout_line {
  my ($self, $line) = @_;
  print "+ $line\n";
}

# The Hive CLI prints out status information on STDERR. This
# method parses that output and updates the job attributes.
sub _handle_stderr_line {
  my ($self, $line) = @_;

  given ($line) {
    when (/Hive history file/) {
      $self->_history_file( $line =~ /=\s*(.*)$/ )
    }
    when (/Total MapReduce jobs/) {
      $self->_job_count( $line =~ /.*?=\s*(.*)$/ );
    }
    when (/Launching Job/) {
      $self->_cur_job( $line =~ /Launching Job (\d+)/ );
    }
    when (/Starting Job/) {
      $self->_id( $line =~ /.*?=\s*(\w+),/ );
      $self->_tracking_url( $line =~ /URL\s*=\s*(.*)$/ );
    }
    when (/Kill Command/) {
      $self->_kill_cmd( $line =~ /=\s*(.*)$/ );
      $self->_state( 'run' );
    }
    when (/Stage-(\d+)\s+map\s*=\s*(\d+)%,\s*reduce\s*=\s*(\d+)%/) {
      @{ $self->_progress }{qw(stage map reduce)} = ($1, $2, $3);
    }
    when (/Ended Job/) {
      $self->_state( 'done' );
      $self->_end_time( __now() );
    }
    when (/OK/) { }
    default { say "* $line"; }
  };
}

1 && q{ this statement is true }; # truth
__END__

=head1 DESCRIPTION

=head1 SYNOPSIS

