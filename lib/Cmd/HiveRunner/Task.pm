use strict;
use warnings;
use feature ':5.10.1';
package Cmd::HiveRunner::Task;
# ABSTRACT: track and control a running hive task
use Moo;
use Data::Dumper;
use Storable qw(dclone);
use autodie qw(:all);
use String::ShellQuote qw(shell_quote_best_effort);
use IPC::System::Simple qw(systemx);
use Hash::Merge::Simple qw(merge);
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Util qw(run_cmd portable_pipe);

# public attributes
has cmd          => ( is => 'ro', required => 1 );
has start_time   => ( is => 'ro', default => \&__now );
has end_time     => ( is => 'rwp' );
has history_file => ( is => 'rwp' );
has pid          => ( is => 'rwp' );
has job_count    => ( is => 'rwp' );
has running_jobs => ( is => 'rwp', default => sub { 0 } );
has state        => ( is => 'rwp', default => sub { 'ready' } );
has progress_info => ( is => 'rwp', default => sub { +{} } );

# private
has _cmd_cv  => ( is => 'rwp', writer => '_set_cmd_cv' );
has _out_hdl => ( is => 'rwp', writer => '_set_out_hdl' );
has _err_hdl => ( is => 'rwp', writer => '_set_err_hdl' );

# give users a *copy* of the current jobs hash.
# If jobs later become objects we can unpack them as hashes here.
sub jobs_info { dclone shift->_jobs }
has _jobs => ( is => 'rw', default => sub { +{} } );

# wait for the hive command to finish and exit.
sub wait { shift->_cmd_cv->recv }

# how long the task has been running or took to run
sub run_time {
  my ($self) = @_;
  return ($self->end_time || __now()) - $self->start_time;
}

# just a helper function
sub __now { scalar time }


# after the constructor, execute the job in another process, reading lines in
# from it as it goes and using the contents of those lines to set the various
# attributes/accessors.
sub BUILD {
  my ($self) = @_;

  my @cmd = @{ $self->cmd };

  #print shell_quote_best_effort(@cmd) . "\n";

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
  $self->_set_out_hdl($out_hdl);

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
  $self->_set_err_hdl($err_hdl);

  # run the command in a child process, cv is a condvar.
  my $cmd_pid;
  my $cmd_cv = run_cmd \@cmd,
    '<'  => \*STDIN,
    '>'  => $out_w,
    '2>' => $err_w,
    '$$' => \$cmd_pid;

  # set the callback for when the command exits.
  $cmd_cv->cb( sub { $self->_handle_cmd_done } );

  $self->_set_pid($cmd_pid);
  $self->_set_cmd_cv($cmd_cv);
  $self->_set_state('started');
}

# when the command ends, this method is called.
sub _handle_cmd_done {
  my ($self, $cv) = @_;
  #print "Command finished\n";
  $self->_set_state('finished');
  $self->_err_hdl->destroy;
  $self->_out_hdl->destroy;
}

# The Hive CLI prints out results on STDOUT. This method
# will handle that output.
sub _handle_stdout_line {
  my ($self, $line) = @_;
  say "+ $line";
}


# The Hive CLI prints out status information on STDERR. This
# method parses that output and updates the job attributes.
sub _handle_stderr_line {
  my ($self, $line) = @_;

  state $last_seen_job_id;
  my $nomatch = 0;

  given ($line) {
    when (/Hive history file/) {
      $self->_set_history_file( $line =~ /=\s*(.*)$/ )
    }
    when (/Total MapReduce jobs/) {
      $self->_set_job_count( $line =~ /.*?=\s*(.*)$/ );
    }
    when (/Launching Job/) {
      $self->_set_running_jobs( $self->running_jobs + 1 );
    }
    when (/Starting Job/) {
      # create and update the attributes of the job.
      # later, I might make the hash in jobs an object.
      my ($job_id)  = ($line =~ /.*?=\s*(\w+),/);
      my ($job_url) = ($line =~ /URL\s*=\s*(.*)$/);

      $self->_jobs->{ $job_id }{id}          = $job_id;
      $self->_jobs->{ $job_id }{tracker_url} = $job_url;
      $self->_jobs->{ $job_id }{state}       = 'started';
      $self->_jobs->{ $job_id }{start_time}  = __now();

      $last_seen_job_id = $job_id;
    }
    when (/Kill Command/) {
      my ($kill_cmd) = ($line =~ /=\s*(.*)$/);
      $self->_jobs->{ $last_seen_job_id }{kill_cmd} = $kill_cmd;
    }
    when (/Stage-(\d+)\s+map\s*=\s*(\d+)%,\s*reduce\s*=\s*(\d+)%/) {
      $self->_set_progress_info(
        merge $self->progress_info, { $1 => { stage => $1, map => $2, reduce => $3 } }
      );
    }
    when (/Ended Job = (.*)/) {
      my ($job_id) = $1;
      $self->_jobs->{ $job_id }{state}    = 'finished';
      $self->_jobs->{ $job_id }{end_time} = __now();
      $self->_set_running_jobs( $self->running_jobs - 1 );
    }
    when (/OK/) {
        $self->_set_state('finished');
    }
    # nothing else matched?
    default { print "* $line"; $nomatch = 1; }
  };
  say $line unless $nomatch;
}


1 && q{ this statement is true }; # truth
__END__

=head1 DESCRIPTION

=head1 SYNOPSIS

=head1 METHODS

=head2 new

Constructs a new Cmd::HiveCLI::Task object. Currently accepts the following parameters:

=over 4

=item I<cmd>

This should be an I<array> of strings, which make up the command and command-line
arguments for running the hive CLI process.

=back

=head2 cmd

The command used to start the hive CLI process

=head2 start_time

The unix timestamp of when the hive process was started

=head2 state

The current state of the Hive process.
Can be one of I<ready>, I<started>, or I<finished>.

=head2 history_file

The file where Hive stores the command history of this invocation

=head2 jobs_info

Returns a hash with info about the jobs run by hive

=head2 progress_info

Returns a hash with information about Hive's progress running your query.

=head2 end_time

The unix timestamp of when the hive command finished

=head2 job_count

The number of hadoop jobs hive plans to run (or will run)

=head2 running_jobs

How many jobs hive is currently running

=head2 pid

The pid of the hive process

=head2 wait

Wait for the process to finish

=head2 run_time

The time in seconds for which the hive process has been (or had been) running

