#!/usr/bin/env perl
use strict;
use warnings;
use lib 'lib';
use AnyEvent;
use Data::Dumper;
use Cmd::HiveRunner;

# set up a hive-runner
my $hive = Cmd::HiveRunner->new( user => 'sscaffid', conf => { foo => 'bar' } );


my $job = $hive->run(
  conf  => { 'mapred.job.queue.name' => 'default' },
  query => q{select * from t_location LIMIT 5},
);
$job->wait;
print Dumper $job;

__END__

# on init:
$job->cmd;          # /usr/bin/sudo -E -u sscaffid /usr/local/hive/bin/hive -hiveconf foo=bar -e 'select * from t_foo where foo != "yarr";'
$job->start_time;   # 1234556788
$job->run_time;     # 0
$job->state;        # init

# as it starts:
$job->history_file; # /tmp/sscaffid/hive_job_log_sscaffid_201208211744_614380010.txt
$job->id;           # job_201208202150_0002
$job->tracking_url; # http://localhost:50030/jobdetails.jsp?jobid=job_201208202150_0002
$job->kill_cmd;     # /usr/local/hadoop/bin/hadoop job  -Dmapred.job.tracker=localhost:8021 -kill job_201208202150_0002
$job->state;        # start

# as it runs
$job->progress;     # { map => 80, reduce => 33 }
$job->run_time;     # 15
$job->state;        # run

# when it is done
$job->run_time;     # 30
$job->end_time;     # 1234556818
$job->state;        # done

# if anything goes wrong, perhaps it should throw an exception?
$job->errors;       # not sure, but it seems like it needs this, right?

